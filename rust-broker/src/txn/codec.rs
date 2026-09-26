//! Small flexible/non-flexible Kafka wire codec used by the transaction and
//! share-group APIs. A `Rd`/`Wr` is created with a `flex` flag; strings, arrays,
//! bytes and tagged fields adapt automatically (compact encodings when flexible).

use bytes::{BufMut, BytesMut};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodecError(pub String);

impl std::fmt::Display for CodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl std::error::Error for CodecError {}

pub type CResult<T> = Result<T, CodecError>;

fn eof<T>() -> CResult<T> {
    Err(CodecError("unexpected end of buffer".into()))
}

pub struct Rd<'a> {
    b: &'a [u8],
    pub flex: bool,
}

impl<'a> Rd<'a> {
    pub fn new(b: &'a [u8], flex: bool) -> Self {
        Self { b, flex }
    }
    pub fn remaining(&self) -> usize {
        self.b.len()
    }
    pub fn take(&mut self, n: usize) -> CResult<&'a [u8]> {
        if self.b.len() < n {
            return eof();
        }
        let (h, t) = self.b.split_at(n);
        self.b = t;
        Ok(h)
    }
    pub fn i8(&mut self) -> CResult<i8> {
        Ok(self.take(1)?[0] as i8)
    }
    pub fn bool(&mut self) -> CResult<bool> {
        Ok(self.i8()? != 0)
    }
    pub fn i16(&mut self) -> CResult<i16> {
        Ok(i16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }
    pub fn i32(&mut self) -> CResult<i32> {
        Ok(i32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    pub fn i64(&mut self) -> CResult<i64> {
        Ok(i64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }
    pub fn uvarint(&mut self) -> CResult<u32> {
        let mut v: u32 = 0;
        for i in 0..5 {
            let b = self.take(1)?[0];
            v |= ((b & 0x7f) as u32) << (7 * i);
            if b & 0x80 == 0 {
                return Ok(v);
            }
        }
        Err(CodecError("malformed varint".into()))
    }
    /// Nullable string.
    pub fn nstring(&mut self) -> CResult<Option<String>> {
        let len: i64 = if self.flex {
            self.uvarint()? as i64 - 1
        } else {
            self.i16()? as i64
        };
        if len < 0 {
            return Ok(None);
        }
        let raw = self.take(len as usize)?;
        String::from_utf8(raw.to_vec())
            .map(Some)
            .map_err(|e| CodecError(format!("invalid utf8: {}", e)))
    }
    /// Non-nullable string (null is coerced to empty).
    pub fn string(&mut self) -> CResult<String> {
        Ok(self.nstring()?.unwrap_or_default())
    }
    /// Array length; `None` means null array.
    pub fn array_len(&mut self) -> CResult<Option<usize>> {
        let n: i64 = if self.flex {
            self.uvarint()? as i64 - 1
        } else {
            self.i32()? as i64
        };
        if n < 0 {
            Ok(None)
        } else if n as usize > self.b.len() + 1024 {
            Err(CodecError("array length implausible".into()))
        } else {
            Ok(Some(n as usize))
        }
    }
    pub fn uuid(&mut self) -> CResult<[u8; 16]> {
        Ok(self.take(16)?.try_into().unwrap())
    }
    pub fn skip_tags(&mut self) -> CResult<()> {
        self.read_tags().map(|_| ())
    }
    /// Reads a tagged-field section returning raw (tag, payload) pairs.
    pub fn read_tags(&mut self) -> CResult<Vec<(u32, Vec<u8>)>> {
        let mut out = Vec::new();
        if !self.flex {
            return Ok(out);
        }
        let n = self.uvarint()?;
        for _ in 0..n {
            let tag = self.uvarint()?;
            let sz = self.uvarint()? as usize;
            out.push((tag, self.take(sz)?.to_vec()));
        }
        Ok(out)
    }
    /// Length-prefixed bytes / records (compact when flexible). `None` = null.
    pub fn bytes(&mut self) -> CResult<Option<&'a [u8]>> {
        let n: i64 = if self.flex {
            self.uvarint()? as i64 - 1
        } else {
            self.i32()? as i64
        };
        if n < 0 {
            return Ok(None);
        }
        Ok(Some(self.take(n as usize)?))
    }
}

pub struct Wr {
    pub buf: BytesMut,
    pub flex: bool,
}

impl Wr {
    pub fn new(flex: bool) -> Self {
        Self {
            buf: BytesMut::new(),
            flex,
        }
    }
    pub fn i8(&mut self, v: i8) {
        self.buf.put_i8(v);
    }
    pub fn bool(&mut self, v: bool) {
        self.buf.put_i8(v as i8);
    }
    pub fn i16(&mut self, v: i16) {
        self.buf.put_i16(v);
    }
    pub fn i32(&mut self, v: i32) {
        self.buf.put_i32(v);
    }
    pub fn i64(&mut self, v: i64) {
        self.buf.put_i64(v);
    }
    pub fn uvarint(&mut self, mut v: u32) {
        loop {
            let b = (v & 0x7f) as u8;
            v >>= 7;
            if v == 0 {
                self.buf.put_u8(b);
                break;
            }
            self.buf.put_u8(b | 0x80);
        }
    }
    pub fn nstring(&mut self, s: Option<&str>) {
        match s {
            Some(s) => {
                if self.flex {
                    self.uvarint(s.len() as u32 + 1);
                } else {
                    self.buf.put_i16(s.len() as i16);
                }
                self.buf.put_slice(s.as_bytes());
            }
            None => {
                if self.flex {
                    self.uvarint(0);
                } else {
                    self.buf.put_i16(-1);
                }
            }
        }
    }
    pub fn string(&mut self, s: &str) {
        self.nstring(Some(s));
    }
    pub fn array_len(&mut self, n: usize) {
        if self.flex {
            self.uvarint(n as u32 + 1);
        } else {
            self.buf.put_i32(n as i32);
        }
    }
    pub fn null_array(&mut self) {
        if self.flex {
            self.uvarint(0);
        } else {
            self.buf.put_i32(-1);
        }
    }
    pub fn uuid(&mut self, u: &[u8; 16]) {
        self.buf.put_slice(u);
    }
    /// Length-prefixed bytes / records (compact when flexible). `None` = null.
    pub fn bytes(&mut self, b: Option<&[u8]>) {
        match b {
            Some(b) => {
                if self.flex {
                    self.uvarint(b.len() as u32 + 1);
                } else {
                    self.buf.put_i32(b.len() as i32);
                }
                self.buf.put_slice(b);
            }
            None => {
                if self.flex {
                    self.uvarint(0);
                } else {
                    self.buf.put_i32(-1);
                }
            }
        }
    }
    /// Empty tagged-fields section (no-op for non-flexible versions).
    pub fn tags(&mut self) {
        if self.flex {
            self.uvarint(0);
        }
    }
    /// Pre-encoded tagged-field section: `fields` are (tag, payload) sorted by tag.
    pub fn tags_with(&mut self, fields: &[(u32, Vec<u8>)]) {
        if !self.flex {
            return;
        }
        self.uvarint(fields.len() as u32);
        for (t, p) in fields {
            self.uvarint(*t);
            self.uvarint(p.len() as u32);
            self.buf.put_slice(p);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flex_and_legacy_roundtrip() {
        for flex in [false, true] {
            let mut w = Wr::new(flex);
            w.nstring(Some("hello"));
            w.nstring(None);
            w.array_len(3);
            w.i64(-5);
            w.bytes(Some(b"abc"));
            w.tags();
            let bytes = w.buf.to_vec();
            let mut r = Rd::new(&bytes, flex);
            assert_eq!(r.nstring().unwrap().as_deref(), Some("hello"));
            assert_eq!(r.nstring().unwrap(), None);
            assert_eq!(r.array_len().unwrap(), Some(3));
            assert_eq!(r.i64().unwrap(), -5);
            assert_eq!(r.bytes().unwrap(), Some(&b"abc"[..]));
            r.skip_tags().unwrap();
            assert_eq!(r.remaining(), 0);
        }
    }
}
