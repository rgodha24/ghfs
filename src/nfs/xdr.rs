//! Minimal XDR (RFC 4506) reader and writer.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct XdrError;

pub type XdrResult<T> = Result<T, XdrError>;

pub struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn take(&mut self, n: usize) -> XdrResult<&'a [u8]> {
        let end = self.pos.checked_add(n).ok_or(XdrError)?;
        let out = self.buf.get(self.pos..end).ok_or(XdrError)?;
        self.pos = end;
        Ok(out)
    }

    pub fn u32(&mut self) -> XdrResult<u32> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }

    pub fn u64(&mut self) -> XdrResult<u64> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }

    pub fn bool(&mut self) -> XdrResult<bool> {
        Ok(self.u32()? != 0)
    }

    /// Variable-length opaque with a maximum length.
    pub fn opaque(&mut self, max: usize) -> XdrResult<&'a [u8]> {
        let len = self.u32()? as usize;
        if len > max {
            return Err(XdrError);
        }
        let data = self.take(len)?;
        self.take(pad(len))?;
        Ok(data)
    }

    pub fn fixed(&mut self, len: usize) -> XdrResult<&'a [u8]> {
        let data = self.take(len)?;
        self.take(pad(len))?;
        Ok(data)
    }

    /// Bytes not consumed yet.
    pub fn rest(&self) -> &'a [u8] {
        &self.buf[self.pos..]
    }

    pub fn skip_opaque(&mut self, max: usize) -> XdrResult<()> {
        self.opaque(max).map(|_| ())
    }
}

fn pad(len: usize) -> usize {
    (4 - len % 4) % 4
}

#[derive(Default)]
pub struct Writer {
    pub buf: Vec<u8>,
}

impl Writer {
    pub fn new() -> Self {
        Self {
            buf: Vec::with_capacity(256),
        }
    }

    pub fn u32(&mut self, v: u32) -> &mut Self {
        self.buf.extend_from_slice(&v.to_be_bytes());
        self
    }

    pub fn u64(&mut self, v: u64) -> &mut Self {
        self.buf.extend_from_slice(&v.to_be_bytes());
        self
    }

    pub fn bool(&mut self, v: bool) -> &mut Self {
        self.u32(v as u32)
    }

    pub fn opaque(&mut self, data: &[u8]) -> &mut Self {
        self.u32(data.len() as u32);
        self.fixed(data)
    }

    pub fn fixed(&mut self, data: &[u8]) -> &mut Self {
        self.buf.extend_from_slice(data);
        self.buf.extend(std::iter::repeat_n(0, pad(data.len())));
        self
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }
}

/// Encoded size of an opaque/string of `len` bytes.
pub fn opaque_size(len: usize) -> usize {
    4 + len + pad(len)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let mut w = Writer::new();
        w.u32(7)
            .u64(1 << 40)
            .bool(true)
            .opaque(b"abcde")
            .fixed(b"xy");
        assert_eq!(w.len(), 4 + 8 + 4 + 12 + 4);
        let mut r = Reader::new(&w.buf);
        assert_eq!(r.u32().unwrap(), 7);
        assert_eq!(r.u64().unwrap(), 1 << 40);
        assert!(r.bool().unwrap());
        assert_eq!(r.opaque(16).unwrap(), b"abcde");
        assert_eq!(r.fixed(2).unwrap(), b"xy");
        assert!(r.u32().is_err());
    }

    #[test]
    fn rejects_oversized_opaque() {
        let mut w = Writer::new();
        w.opaque(&[0; 10]);
        assert!(Reader::new(&w.buf).opaque(9).is_err());
        assert_eq!(opaque_size(5), 12);
    }
}
