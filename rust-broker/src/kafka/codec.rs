//! Version-aware Kafka wire codec supporting both classic and "flexible" encodings.
//!
//! Flexible versions (KIP-482) change the encoding as follows:
//!   * strings/bytes use an unsigned-varint length + 1 (0 = null) ("compact"),
//!   * arrays use an unsigned-varint length + 1 (0 = null),
//!   * every struct (and the request/response header) ends with a tagged-field section,
//!   * request header is v2 (client_id stays a classic nullable string, followed by tagged fields),
//!     response header is v1 (correlation id + tagged fields; ApiVersions always uses header v0).
//!
//! `Rd` and `Wr` carry a `flex` flag so a handler can be written once for all versions.

use bytes::{BufMut, BytesMut};

pub type CodecResult<T> = Result<T, String>;

/// Cursor over a request body.
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

    fn take(&mut self, n: usize) -> CodecResult<&'a [u8]> {
        if self.b.len() < n {
            return Err("unexpected end of kafka request".into());
        }
        let (h, t) = self.b.split_at(n);
        self.b = t;
        Ok(h)
    }

    pub fn i8(&mut self) -> CodecResult<i8> {
        Ok(self.take(1)?[0] as i8)
    }
    pub fn bool(&mut self) -> CodecResult<bool> {
        Ok(self.i8()? != 0)
    }
    pub fn i16(&mut self) -> CodecResult<i16> {
        Ok(i16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }
    pub fn i32(&mut self) -> CodecResult<i32> {
        Ok(i32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    pub fn i64(&mut self) -> CodecResult<i64> {
        Ok(i64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }
    pub fn uuid(&mut self) -> CodecResult<[u8; 16]> {
        Ok(self.take(16)?.try_into().unwrap())
    }

    pub fn uvarint(&mut self) -> CodecResult<u32> {
        let mut v: u32 = 0;
        let mut shift = 0;
        loop {
            let byte = self.take(1)?[0];
            v |= ((byte & 0x7f) as u32) << shift;
            if byte & 0x80 == 0 {
                return Ok(v);
            }
            shift += 7;
            if shift > 28 {
                return Err("malformed varint".into());
            }
        }
    }

    /// Nullable string (classic: i16 len, -1 = null; flexible: uvarint len+1, 0 = null).
    pub fn nstr(&mut self) -> CodecResult<Option<String>> {
        let len: i64 = if self.flex {
            let l = self.uvarint()? as i64;
            if l == 0 {
                return Ok(None);
            }
            l - 1
        } else {
            let l = self.i16()? as i64;
            if l < 0 {
                return Ok(None);
            }
            l
        };
        let raw = self.take(len as usize)?;
        String::from_utf8(raw.to_vec())
            .map(Some)
            .map_err(|e| format!("invalid utf8: {e}"))
    }

    /// Non-nullable string (a null is treated as empty).
    pub fn str(&mut self) -> CodecResult<String> {
        Ok(self.nstr()?.unwrap_or_default())
    }

    /// Nullable bytes.
    pub fn nbytes(&mut self) -> CodecResult<Option<Vec<u8>>> {
        let len: i64 = if self.flex {
            let l = self.uvarint()? as i64;
            if l == 0 {
                return Ok(None);
            }
            l - 1
        } else {
            let l = self.i32()? as i64;
            if l < 0 {
                return Ok(None);
            }
            l
        };
        Ok(Some(self.take(len as usize)?.to_vec()))
    }

    pub fn bytes(&mut self) -> CodecResult<Vec<u8>> {
        Ok(self.nbytes()?.unwrap_or_default())
    }

    /// Nullable array length (None = null array).
    pub fn narr(&mut self) -> CodecResult<Option<usize>> {
        let n: i64 = if self.flex {
            let l = self.uvarint()? as i64;
            if l == 0 {
                return Ok(None);
            }
            l - 1
        } else {
            let l = self.i32()? as i64;
            if l < 0 {
                return Ok(None);
            }
            l
        };
        let n = n as usize;
        // Every element takes at least one byte, which bounds hostile lengths.
        if n > self.b.len() {
            return Err("array length exceeds request size".into());
        }
        Ok(Some(n))
    }

    pub fn arr(&mut self) -> CodecResult<usize> {
        Ok(self.narr()?.unwrap_or(0))
    }

    /// Skip a tagged-field section (only present in flexible versions).
    pub fn tagged(&mut self) -> CodecResult<()> {
        if !self.flex {
            return Ok(());
        }
        let n = self.uvarint()?;
        for _ in 0..n {
            let _tag = self.uvarint()?;
            let size = self.uvarint()? as usize;
            self.take(size)?;
        }
        Ok(())
    }
}

/// Response writer.
pub struct Wr {
    pub buf: BytesMut,
    pub flex: bool,
}

impl Wr {
    pub fn new(flex: bool) -> Self {
        Self { buf: BytesMut::new(), flex }
    }
    pub fn i8(&mut self, v: i8) -> &mut Self {
        self.buf.put_i8(v);
        self
    }
    pub fn bool(&mut self, v: bool) -> &mut Self {
        self.buf.put_i8(v as i8);
        self
    }
    pub fn i16(&mut self, v: i16) -> &mut Self {
        self.buf.put_i16(v);
        self
    }
    pub fn i32(&mut self, v: i32) -> &mut Self {
        self.buf.put_i32(v);
        self
    }
    pub fn i64(&mut self, v: i64) -> &mut Self {
        self.buf.put_i64(v);
        self
    }
    pub fn uuid(&mut self, v: &[u8; 16]) -> &mut Self {
        self.buf.put_slice(v);
        self
    }
    pub fn uvarint(&mut self, mut v: u32) -> &mut Self {
        while v >= 0x80 {
            self.buf.put_u8((v as u8 & 0x7f) | 0x80);
            v >>= 7;
        }
        self.buf.put_u8(v as u8);
        self
    }
    pub fn nstr(&mut self, s: Option<&str>) -> &mut Self {
        match (s, self.flex) {
            (Some(s), true) => {
                self.uvarint(s.len() as u32 + 1);
                self.buf.put_slice(s.as_bytes());
            }
            (None, true) => {
                self.uvarint(0);
            }
            (Some(s), false) => {
                self.buf.put_i16(s.len() as i16);
                self.buf.put_slice(s.as_bytes());
            }
            (None, false) => {
                self.buf.put_i16(-1);
            }
        }
        self
    }
    pub fn str(&mut self, s: &str) -> &mut Self {
        self.nstr(Some(s))
    }
    pub fn nbytes(&mut self, b: Option<&[u8]>) -> &mut Self {
        match (b, self.flex) {
            (Some(b), true) => {
                self.uvarint(b.len() as u32 + 1);
                self.buf.put_slice(b);
            }
            (None, true) => {
                self.uvarint(0);
            }
            (Some(b), false) => {
                self.buf.put_i32(b.len() as i32);
                self.buf.put_slice(b);
            }
            (None, false) => {
                self.buf.put_i32(-1);
            }
        }
        self
    }
    pub fn bytes(&mut self, b: &[u8]) -> &mut Self {
        self.nbytes(Some(b))
    }
    pub fn arr(&mut self, n: usize) -> &mut Self {
        if self.flex {
            self.uvarint(n as u32 + 1);
        } else {
            self.buf.put_i32(n as i32);
        }
        self
    }
    pub fn null_arr(&mut self) -> &mut Self {
        if self.flex {
            self.uvarint(0);
        } else {
            self.buf.put_i32(-1);
        }
        self
    }
    /// Empty tagged-field section (flexible versions only).
    pub fn tagged(&mut self) -> &mut Self {
        if self.flex {
            self.buf.put_u8(0);
        }
        self
    }
    pub fn finish(self) -> Vec<u8> {
        self.buf.to_vec()
    }
}

/// First flexible version for each API this crate encodes via `Rd`/`Wr` (None = never flexible).
pub fn first_flexible_version(api_key: i16) -> Option<i16> {
    Some(match api_key {
        2 => 6,   // ListOffsets
        8 => 8,   // OffsetCommit
        9 => 6,   // OffsetFetch
        10 => 3,  // FindCoordinator
        11 => 6,  // JoinGroup
        12 => 4,  // Heartbeat
        13 => 4,  // LeaveGroup
        14 => 4,  // SyncGroup
        15 => 5,  // DescribeGroups
        16 => 3,  // ListGroups
        19 => 5,  // CreateTopics
        20 => 4,  // DeleteTopics
        32 => 4,  // DescribeConfigs
        33 => 2,  // AlterConfigs
        37 => 2,  // CreatePartitions
        42 => 2,  // DeleteGroups
        43 => 2,  // ElectLeaders
        44 => 1,  // IncrementalAlterConfigs
        60 => 0,  // DescribeCluster
        _ => return None,
    })
}

pub fn is_flexible(api_key: i16, version: i16) -> bool {
    first_flexible_version(api_key).map_or(false, |f| version >= f)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_classic_and_flexible() {
        for flex in [false, true] {
            let mut w = Wr::new(flex);
            w.i16(7).str("hello").nstr(None).arr(2).i32(1).i32(2).bytes(b"xyz").nbytes(None).tagged();
            let data = w.finish();
            let mut r = Rd::new(&data, flex);
            assert_eq!(r.i16().unwrap(), 7);
            assert_eq!(r.str().unwrap(), "hello");
            assert_eq!(r.nstr().unwrap(), None);
            assert_eq!(r.arr().unwrap(), 2);
            assert_eq!(r.i32().unwrap(), 1);
            assert_eq!(r.i32().unwrap(), 2);
            assert_eq!(r.bytes().unwrap(), b"xyz");
            assert_eq!(r.nbytes().unwrap(), None);
            r.tagged().unwrap();
            assert_eq!(r.remaining(), 0);
        }
    }

    #[test]
    fn flexible_compact_string_layout() {
        let mut w = Wr::new(true);
        w.str("ab");
        assert_eq!(w.finish(), vec![3, b'a', b'b']);
        let mut w = Wr::new(true);
        w.nstr(None);
        assert_eq!(w.finish(), vec![0]);
    }

    #[test]
    fn tagged_fields_are_skipped() {
        // 1 tag: id=1, size=2, payload 0xAA 0xBB, then i8 = 9
        let data = [1u8, 1, 2, 0xAA, 0xBB, 9];
        let mut r = Rd::new(&data, true);
        r.tagged().unwrap();
        assert_eq!(r.i8().unwrap(), 9);
    }

    #[test]
    fn truncated_input_errors() {
        let mut r = Rd::new(&[0, 0, 0], false);
        assert!(r.i32().is_err());
        let mut r = Rd::new(&[0x7f, 0xff, 0xff, 0xff], false);
        assert!(r.arr().is_err());
    }

    #[test]
    fn uvarint_multi_byte() {
        let mut w = Wr::new(true);
        w.uvarint(300);
        let d = w.finish();
        assert_eq!(d, vec![0xAC, 0x02]);
        assert_eq!(Rd::new(&d, true).uvarint().unwrap(), 300);
    }
}
