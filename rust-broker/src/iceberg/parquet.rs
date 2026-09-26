//! Hand-written minimal Parquet writer: one row group, PLAIN encoding,
//! UNCOMPRESSED, all columns OPTIONAL, Iceberg field-ids in the schema.
//! Metadata is serialized with the Thrift compact protocol.

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ColType {
    Int,
    Long,
    TimestampTz,
    Binary,
    String,
    Double,
    Boolean,
}

impl ColType {
    pub fn iceberg_name(&self) -> &'static str {
        match self {
            ColType::Int => "int",
            ColType::Long => "long",
            ColType::TimestampTz => "timestamptz",
            ColType::Binary => "binary",
            ColType::String => "string",
            ColType::Double => "double",
            ColType::Boolean => "boolean",
        }
    }
    fn physical(&self) -> i32 {
        match self {
            ColType::Boolean => 0,
            ColType::Int => 1,
            ColType::Long | ColType::TimestampTz => 2,
            ColType::Double => 5,
            ColType::Binary | ColType::String => 6,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Column {
    pub id: i32,
    pub name: String,
    pub ty: ColType,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Val {
    Null,
    Bool(bool),
    Int(i32),
    Long(i64),
    Double(f64),
    Bytes(Vec<u8>),
}

#[derive(Debug, Clone, Default)]
pub struct FileStats {
    pub record_count: i64,
    /// (field id, bytes)
    pub column_sizes: Vec<(i32, i64)>,
    pub value_counts: Vec<(i32, i64)>,
    pub null_counts: Vec<(i32, i64)>,
}

// ---------------- Thrift compact writer ----------------

struct Thrift {
    buf: Vec<u8>,
    last: Vec<i16>,
}

const T_BOOL_TRUE: u8 = 1;
const T_I32: u8 = 5;
const T_I64: u8 = 6;
const T_BINARY: u8 = 8;
const T_LIST: u8 = 9;
const T_STRUCT: u8 = 12;

impl Thrift {
    fn new() -> Self {
        Thrift { buf: Vec::new(), last: vec![0] }
    }
    fn varint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.buf.push((v as u8 & 0x7f) | 0x80);
            v >>= 7;
        }
        self.buf.push(v as u8);
    }
    fn zz32(v: i32) -> u64 {
        ((v << 1) ^ (v >> 31)) as u32 as u64
    }
    fn zz64(v: i64) -> u64 {
        ((v << 1) ^ (v >> 63)) as u64
    }
    fn field(&mut self, id: i16, ty: u8) {
        let last = *self.last.last().unwrap();
        let delta = id - last;
        if (1..=15).contains(&delta) {
            self.buf.push(((delta as u8) << 4) | ty);
        } else {
            self.buf.push(ty);
            let z = ((id as i32) << 1) ^ ((id as i32) >> 31);
            self.varint(z as u32 as u64);
        }
        *self.last.last_mut().unwrap() = id;
    }
    fn i32(&mut self, id: i16, v: i32) {
        self.field(id, T_I32);
        self.varint(Self::zz32(v));
    }
    fn i64(&mut self, id: i16, v: i64) {
        self.field(id, T_I64);
        self.varint(Self::zz64(v));
    }
    fn string(&mut self, id: i16, s: &str) {
        self.field(id, T_BINARY);
        self.varint(s.len() as u64);
        self.buf.extend_from_slice(s.as_bytes());
    }
    fn bool_field(&mut self, id: i16, v: bool) {
        self.field(id, if v { T_BOOL_TRUE } else { 2 });
    }
    fn list_begin(&mut self, id: i16, elem: u8, n: usize) {
        self.field(id, T_LIST);
        self.list_header(elem, n);
    }
    fn list_header(&mut self, elem: u8, n: usize) {
        if n < 15 {
            self.buf.push(((n as u8) << 4) | elem);
        } else {
            self.buf.push(0xF0 | elem);
            self.varint(n as u64);
        }
    }
    fn struct_field(&mut self, id: i16) {
        self.field(id, T_STRUCT);
        self.last.push(0);
    }
    fn struct_begin_elem(&mut self) {
        self.last.push(0);
    }
    fn end(&mut self) {
        self.buf.push(0);
        self.last.pop();
    }
    fn raw_i32(&mut self, v: i32) {
        self.varint(Self::zz32(v));
    }
    fn raw_string(&mut self, s: &str) {
        self.varint(s.len() as u64);
        self.buf.extend_from_slice(s.as_bytes());
    }
}

/// RLE/bit-packed hybrid, bit width 1, as runs of identical def-levels.
fn encode_def_levels(present: &[bool]) -> Vec<u8> {
    let mut body = Vec::new();
    let mut i = 0;
    while i < present.len() {
        let v = present[i];
        let mut j = i;
        while j < present.len() && present[j] == v {
            j += 1;
        }
        let mut run = ((j - i) as u64) << 1;
        while run >= 0x80 {
            body.push((run as u8 & 0x7f) | 0x80);
            run >>= 7;
        }
        body.push(run as u8);
        body.push(v as u8);
        i = j;
    }
    let mut out = (body.len() as u32).to_le_bytes().to_vec();
    out.extend(body);
    out
}

fn encode_values(col: &Column, rows: &[Vec<Val>], idx: usize) -> (Vec<bool>, Vec<u8>) {
    let mut present = Vec::with_capacity(rows.len());
    let mut data = Vec::new();
    let mut bits: Vec<bool> = Vec::new();
    for r in rows {
        let v = &r[idx];
        let has = !matches!(v, Val::Null);
        present.push(has);
        if !has {
            continue;
        }
        match (col.ty, v) {
            (ColType::Boolean, Val::Bool(b)) => bits.push(*b),
            (ColType::Int, Val::Int(i)) => data.extend_from_slice(&i.to_le_bytes()),
            (ColType::Long | ColType::TimestampTz, Val::Long(i)) => data.extend_from_slice(&i.to_le_bytes()),
            (ColType::Double, Val::Double(d)) => data.extend_from_slice(&d.to_le_bytes()),
            (ColType::Binary | ColType::String, Val::Bytes(b)) => {
                data.extend_from_slice(&(b.len() as u32).to_le_bytes());
                data.extend_from_slice(b);
            }
            _ => panic!("value type mismatch for column {}", col.name),
        }
    }
    if col.ty == ColType::Boolean {
        for chunk in bits.chunks(8) {
            let mut byte = 0u8;
            for (k, b) in chunk.iter().enumerate() {
                if *b {
                    byte |= 1 << k;
                }
            }
            data.push(byte);
        }
    }
    (present, data)
}

pub fn write_parquet(cols: &[Column], rows: &[Vec<Val>]) -> (Vec<u8>, FileStats) {
    let mut out = b"PAR1".to_vec();
    let mut stats = FileStats { record_count: rows.len() as i64, ..Default::default() };
    struct ChunkMeta {
        offset: i64,
        total: i64,
        num_values: i64,
    }
    let mut chunks = Vec::new();
    for (ci, col) in cols.iter().enumerate() {
        let (present, vals) = encode_values(col, rows, ci);
        let mut page = encode_def_levels(&present);
        page.extend(vals);
        let mut h = Thrift::new();
        h.i32(1, 0); // DATA_PAGE
        h.i32(2, page.len() as i32);
        h.i32(3, page.len() as i32);
        h.struct_field(5);
        h.i32(1, rows.len() as i32);
        h.i32(2, 0); // PLAIN
        h.i32(3, 3); // RLE (def levels)
        h.i32(4, 3); // RLE (rep levels)
        h.end();
        h.end();
        let offset = out.len() as i64;
        out.extend_from_slice(&h.buf);
        out.extend_from_slice(&page);
        let total = out.len() as i64 - offset;
        let nulls = present.iter().filter(|p| !**p).count() as i64;
        stats.column_sizes.push((col.id, total));
        stats.value_counts.push((col.id, rows.len() as i64));
        stats.null_counts.push((col.id, nulls));
        chunks.push(ChunkMeta { offset, total, num_values: rows.len() as i64 });
    }

    // FileMetaData
    let mut t = Thrift::new();
    t.i32(1, 1); // version
    t.list_begin(2, T_STRUCT, cols.len() + 1);
    t.struct_begin_elem();
    t.string(4, "schema");
    t.i32(5, cols.len() as i32);
    t.end();
    for c in cols {
        t.struct_begin_elem();
        t.i32(1, c.ty.physical());
        t.i32(3, 1); // OPTIONAL
        t.string(4, &c.name);
        match c.ty {
            ColType::String => {
                t.i32(6, 0); // UTF8
            }
            ColType::TimestampTz => {
                t.i32(6, 10); // TIMESTAMP_MICROS
            }
            _ => {}
        }
        t.i32(9, c.id);
        match c.ty {
            ColType::String => {
                t.struct_field(10);
                t.struct_field(1); // STRING
                t.end();
                t.end();
            }
            ColType::TimestampTz => {
                t.struct_field(10);
                t.struct_field(8); // TIMESTAMP
                t.bool_field(1, true);
                t.struct_field(2); // unit
                t.struct_field(2); // MICROS
                t.end();
                t.end();
                t.end();
                t.end();
            }
            _ => {}
        }
        t.end();
    }
    t.i64(3, rows.len() as i64);
    t.list_begin(4, T_STRUCT, 1);
    t.struct_begin_elem();
    t.list_begin(1, T_STRUCT, cols.len());
    let mut total_bytes = 0;
    for (c, m) in cols.iter().zip(&chunks) {
        t.struct_begin_elem();
        t.i64(2, m.offset);
        t.struct_field(3);
        t.i32(1, c.ty.physical());
        t.list_begin(2, T_I32, 2);
        t.raw_i32(0);
        t.raw_i32(3);
        t.list_begin(3, T_BINARY, 1);
        t.raw_string(&c.name);
        t.i32(4, 0); // UNCOMPRESSED
        t.i64(5, m.num_values);
        t.i64(6, m.total);
        t.i64(7, m.total);
        t.i64(9, m.offset);
        t.end();
        t.end();
        total_bytes += m.total;
    }
    t.i64(2, total_bytes);
    t.i64(3, rows.len() as i64);
    t.end();
    t.string(6, "aerostream-iceberg");
    t.end();

    out.extend_from_slice(&t.buf);
    out.extend_from_slice(&(t.buf.len() as u32).to_le_bytes());
    out.extend_from_slice(b"PAR1");
    (out, stats)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_well_formed_file() {
        let cols = vec![
            Column { id: 1, name: "a".into(), ty: ColType::Long },
            Column { id: 2, name: "b".into(), ty: ColType::String },
            Column { id: 3, name: "c".into(), ty: ColType::Boolean },
        ];
        let rows = vec![
            vec![Val::Long(1), Val::Bytes(b"x".to_vec()), Val::Bool(true)],
            vec![Val::Null, Val::Null, Val::Bool(false)],
        ];
        let (bytes, stats) = write_parquet(&cols, &rows);
        assert_eq!(&bytes[..4], b"PAR1");
        assert_eq!(&bytes[bytes.len() - 4..], b"PAR1");
        let flen = u32::from_le_bytes(bytes[bytes.len() - 8..bytes.len() - 4].try_into().unwrap()) as usize;
        assert!(flen > 0 && flen < bytes.len());
        assert_eq!(stats.record_count, 2);
        assert_eq!(stats.null_counts[0], (1, 1));
    }
}
