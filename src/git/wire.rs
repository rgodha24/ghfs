//! The small protocol-v2 subset ghfs uses. Packet and control data are bounded.

use std::time::{Duration, Instant};

use bytes::Bytes;
use git2::Oid;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::GitError;
use super::operation::{Limits, Reservation, Work};
use super::remote::RemoteRefs;
use super::transport::Body;

const MAX_PACKET: usize = 65_520;
const MAX_CONTROL: usize = 16 << 20;
const MAX_LINES: usize = 100_000;
const PROGRESS_WINDOW: Duration = Duration::from_secs(20);
const MIN_WINDOW_BYTES: usize = 20 * 1024;

#[derive(Debug, Clone)]
pub(super) struct Capabilities {
    ls_refs: bool,
    fetch: bool,
    filter: bool,
    shallow: bool,
}

impl Capabilities {
    pub fn require_fetch(&self, shallow: bool) -> Result<(), GitError> {
        if !self.fetch || !self.filter || (shallow && !self.shallow) {
            return Err(GitError::Protocol(
                "remote lacks required protocol-v2 fetch/filter/shallow capabilities".into(),
            ));
        }
        Ok(())
    }

    pub fn require_refs(&self) -> Result<(), GitError> {
        if !self.ls_refs {
            return Err(GitError::Protocol(
                "remote lacks protocol-v2 ls-refs".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Packet {
    Data(Vec<u8>),
    Flush,
    Delimiter,
    End,
}

struct Reader<'a> {
    body: Body,
    work: &'a Work,
    control: usize,
    lines: usize,
    window: Instant,
    useful: usize,
}

impl<'a> Reader<'a> {
    fn new(body: Body, work: &'a Work) -> Self {
        Self {
            body,
            work,
            control: 0,
            lines: 0,
            window: Instant::now(),
            useful: 0,
        }
    }

    fn control(&mut self, bytes: usize) -> Result<(), GitError> {
        self.control = self
            .control
            .checked_add(bytes)
            .ok_or_else(|| GitError::Protocol("control response overflow".into()))?;
        self.lines += 1;
        if self.control > MAX_CONTROL || self.lines > MAX_LINES {
            return Err(GitError::Protocol("control response exceeds limit".into()));
        }
        Ok(())
    }

    fn progress(&mut self, bytes: usize) -> Result<(), GitError> {
        self.useful = self.useful.saturating_add(bytes);
        if self.window.elapsed() >= PROGRESS_WINDOW {
            if self.useful < MIN_WINDOW_BYTES {
                return Err(GitError::Transport(
                    "remote made insufficient useful progress".into(),
                ));
            }
            self.window = Instant::now();
            self.useful = 0;
        }
        Ok(())
    }

    async fn read(&mut self, out: &mut [u8]) -> Result<usize, GitError> {
        // A window is renewed only by useful data, never progress sidebands.
        if self.window.elapsed() >= PROGRESS_WINDOW {
            self.progress(0)?;
        }
        let deadline = self.window + PROGRESS_WINDOW;
        self.work
            .wait(async {
                tokio::time::timeout_at(deadline.into(), self.body.read(out))
                    .await
                    .map_err(|_| {
                        GitError::Transport(
                            "remote made no useful progress within 20 seconds".into(),
                        )
                    })?
                    .map_err(GitError::Io)
            })
            .await
    }

    async fn exact(&mut self, out: &mut [u8]) -> Result<(), GitError> {
        let mut pos = 0;
        while pos < out.len() {
            let n = self.read(&mut out[pos..]).await?;
            if n == 0 {
                return Err(GitError::Protocol("truncated pkt-line".into()));
            }
            pos += n;
        }
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<Packet>, GitError> {
        let mut header = [0; 4];
        if self.read(&mut header[..1]).await? == 0 {
            return Ok(None);
        }
        self.exact(&mut header[1..]).await?;
        let mut length = 0usize;
        for byte in header {
            let digit = match byte {
                b'0'..=b'9' => byte - b'0',
                b'a'..=b'f' => byte - b'a' + 10,
                b'A'..=b'F' => byte - b'A' + 10,
                _ => return Err(GitError::Protocol("non-hex pkt-line length".into())),
            };
            length = length * 16 + digit as usize;
        }
        let packet = match length {
            0 => Packet::Flush,
            1 => Packet::Delimiter,
            2 => Packet::End,
            4..=MAX_PACKET => {
                let mut data = vec![0; length - 4];
                self.exact(&mut data).await?;
                Packet::Data(data)
            }
            _ => return Err(GitError::Protocol("invalid pkt-line length".into())),
        };
        Ok(Some(packet))
    }

    async fn finish(&mut self, allow_end: bool) -> Result<(), GitError> {
        match self.next().await? {
            None => Ok(()),
            Some(Packet::End) if allow_end && self.next().await?.is_none() => Ok(()),
            _ => Err(GitError::Protocol(
                "trailing bytes after protocol response".into(),
            )),
        }
    }
}

fn text(bytes: &[u8]) -> Result<&str, GitError> {
    std::str::from_utf8(bytes)
        .map(str::trim_end)
        .map_err(|_| GitError::Protocol("non-UTF8 protocol control line".into()))
}

fn oid(raw: &str) -> Result<Oid, GitError> {
    if raw.len() != 40 {
        return Err(GitError::Protocol(
            "OID must have exactly 40 hex digits".into(),
        ));
    }
    Oid::from_str(raw).map_err(|_| GitError::Protocol("invalid OID".into()))
}

fn remote_error(bytes: &[u8], wanted: Option<Oid>) -> GitError {
    let message = String::from_utf8_lossy(bytes);
    let lower = message.to_ascii_lowercase();
    if let Some(oid) = wanted
        && (lower.contains("not our ref")
            || lower.contains("unadvertised object")
            || lower.contains("couldn't find remote ref"))
    {
        return GitError::MissingObject(oid);
    }
    // Error messages may contain untrusted URLs or credentials; don't echo them.
    GitError::Protocol("remote rejected the request".into())
}

pub(super) fn command(name: &str, args: impl IntoIterator<Item = String>) -> Bytes {
    fn line(out: &mut Vec<u8>, value: &str) {
        let value = format!("{value}\n");
        out.extend_from_slice(format!("{:04x}", value.len() + 4).as_bytes());
        out.extend_from_slice(value.as_bytes());
    }
    let mut out = Vec::new();
    line(&mut out, &format!("command={name}"));
    out.extend_from_slice(b"0001");
    for arg in args {
        line(&mut out, &arg);
    }
    out.extend_from_slice(b"0000");
    out.into()
}

pub(super) async fn advertisement(body: Body, work: &Work) -> Result<Capabilities, GitError> {
    let mut reader = Reader::new(body, work);
    let first = reader.next().await?;
    let first = match first {
        Some(Packet::Data(data)) if text(&data)? == "# service=git-upload-pack" => {
            if reader.next().await? != Some(Packet::Flush) {
                return Err(GitError::Protocol(
                    "missing service-advertisement flush".into(),
                ));
            }
            reader.next().await?
        }
        other => other,
    };
    if first != Some(Packet::Data(b"version 2\n".to_vec()))
        && first != Some(Packet::Data(b"version 2".to_vec()))
    {
        return Err(GitError::Protocol(
            "remote did not advertise protocol v2".into(),
        ));
    }
    let mut caps = Capabilities {
        ls_refs: false,
        fetch: false,
        filter: false,
        shallow: false,
    };
    loop {
        match reader.next().await? {
            Some(Packet::Data(data)) => {
                reader.control(data.len())?;
                reader.progress(data.len())?;
                let value = text(&data)?;
                if value == "ls-refs" || value.starts_with("ls-refs=") {
                    caps.ls_refs = true;
                } else if value == "fetch" || value.starts_with("fetch=") {
                    caps.fetch = true;
                    if let Some((_, features)) = value.split_once('=') {
                        caps.filter = features.split_whitespace().any(|v| v == "filter");
                        caps.shallow = features.split_whitespace().any(|v| v == "shallow");
                    }
                } else if let Some(format) = value.strip_prefix("object-format=")
                    && format != "sha1"
                {
                    return Err(GitError::Protocol(
                        "unsupported Git object format (SHA-1 required)".into(),
                    ));
                }
            }
            Some(Packet::Flush) => {
                reader.finish(true).await?;
                return Ok(caps);
            }
            _ => {
                return Err(GitError::Protocol(
                    "truncated/malformed capability advertisement".into(),
                ));
            }
        }
    }
}

pub(super) async fn refs(body: Body, work: &Work, all: bool) -> Result<RemoteRefs, GitError> {
    let mut reader = Reader::new(body, work);
    let mut result = RemoteRefs::default();
    loop {
        match reader.next().await? {
            Some(Packet::Data(data)) => {
                reader.control(data.len())?;
                reader.progress(data.len())?;
                let line = text(&data)?;
                if data.starts_with(b"ERR ") {
                    return Err(remote_error(&data, None));
                }
                let mut fields = line.split_whitespace();
                let raw = fields
                    .next()
                    .ok_or_else(|| GitError::Protocol("empty ref response".into()))?;
                let name = fields
                    .next()
                    .ok_or_else(|| GitError::Protocol("ref name missing".into()))?;
                if raw == "unborn" {
                    continue;
                }
                let object = oid(raw)?;
                let mut peeled = None;
                let mut target = None;
                for field in fields {
                    if let Some(value) = field.strip_prefix("peeled:") {
                        peeled = Some(oid(value)?);
                    } else if let Some(value) = field.strip_prefix("symref-target:") {
                        target = Some(value.to_owned());
                    }
                }
                if name == "HEAD" {
                    result.head = Some(object);
                    result.head_ref = target;
                } else if all && (name.starts_with("refs/heads/") || name.starts_with("refs/tags/"))
                {
                    result
                        .refs
                        .push((name.to_owned(), peeled.unwrap_or(object)));
                }
            }
            Some(Packet::Flush) => {
                reader.finish(true).await?;
                return Ok(result);
            }
            _ => {
                return Err(GitError::Protocol(
                    "truncated/malformed ls-refs response".into(),
                ));
            }
        }
    }
}

pub(super) async fn fetch(
    body: Body,
    work: &Work,
    output: &mut tokio::fs::File,
    reservation: &mut Reservation,
    limits: &Limits,
    wanted: Oid,
) -> Result<(), GitError> {
    let mut reader = Reader::new(body, work);
    let mut shallow = false;
    loop {
        let Some(Packet::Data(data)) = reader.next().await? else {
            return Err(GitError::Protocol("missing packfile section".into()));
        };
        reader.control(data.len())?;
        let header = text(&data)?;
        if header.starts_with("ERR ") {
            return Err(remote_error(&data, Some(wanted)));
        }
        match header {
            "packfile" => break,
            "shallow-info" if !shallow => {
                shallow = true;
                loop {
                    match reader.next().await? {
                        Some(Packet::Data(data)) => {
                            reader.control(data.len())?;
                            let line = text(&data)?;
                            let raw = line
                                .strip_prefix("shallow ")
                                .or_else(|| line.strip_prefix("unshallow "))
                                .ok_or_else(|| {
                                    GitError::Protocol("invalid shallow-info line".into())
                                })?;
                            oid(raw)?;
                        }
                        Some(Packet::Delimiter) => break,
                        _ => {
                            return Err(GitError::Protocol(
                                "invalid shallow-info terminator".into(),
                            ));
                        }
                    }
                }
            }
            _ => {
                return Err(GitError::Protocol(
                    "unexpected fetch response section".into(),
                ));
            }
        }
    }
    let mut written = 0u64;
    loop {
        match reader.next().await? {
            Some(Packet::Data(data)) => {
                let (channel, payload) = data
                    .split_first()
                    .ok_or_else(|| GitError::Protocol("empty sideband packet".into()))?;
                match channel {
                    1 => {
                        let next = written.checked_add(payload.len() as u64).ok_or_else(|| {
                            GitError::ResourceLimit("pack length overflow".into())
                        })?;
                        if next > limits.max_pack_bytes {
                            return Err(GitError::ResourceLimit(
                                "downloaded pack exceeds limit".into(),
                            ));
                        }
                        reservation.grow(payload.len() as u64)?;
                        work.wait(async { output.write_all(payload).await.map_err(GitError::Io) })
                            .await?;
                        written = next;
                        reader.progress(payload.len())?;
                    }
                    2 => {
                        reader.control(payload.len())?;
                        log::debug!("Git remote progress ({} bytes)", payload.len());
                    }
                    3 => return Err(remote_error(payload, Some(wanted))),
                    _ => return Err(GitError::Protocol("invalid sideband channel".into())),
                }
            }
            Some(terminal @ (Packet::Flush | Packet::End)) => {
                // A flush may be followed by one response-end; response-end
                // itself cannot be followed by a second terminator.
                reader.finish(terminal == Packet::Flush).await?;
                if written < 32 {
                    return Err(GitError::Protocol("missing/truncated pack data".into()));
                }
                work.wait(async { output.flush().await.map_err(GitError::Io) })
                    .await?;
                return Ok(());
            }
            _ => {
                return Err(GitError::Protocol(
                    "truncated/malformed packfile response".into(),
                ));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn body(data: Vec<u8>) -> Body {
        Box::pin(Cursor::new(data))
    }
    fn pkt(data: &[u8]) -> Vec<u8> {
        [format!("{:04x}", data.len() + 4).as_bytes(), data].concat()
    }
    fn work() -> Work {
        Work::new(Duration::from_secs(2))
    }

    #[test]
    fn request_has_delimiter_and_flush() {
        assert_eq!(
            command("ls-refs", ["symrefs".into()]).as_ref(),
            b"0014command=ls-refs\n0001000csymrefs\n0000"
        );
    }

    #[tokio::test]
    async fn direct_and_service_prefixed_advertisements() {
        for prefix in [
            Vec::new(),
            [pkt(b"# service=git-upload-pack\n"), b"0000".to_vec()].concat(),
        ] {
            let data = [
                prefix,
                pkt(b"version 2\n"),
                pkt(b"ls-refs=unborn\n"),
                pkt(b"fetch=shallow filter\n"),
                pkt(b"object-format=sha1\n"),
                b"0000".to_vec(),
            ]
            .concat();
            let caps = advertisement(body(data), &work()).await.unwrap();
            caps.require_refs().unwrap();
            caps.require_fetch(true).unwrap();
        }
    }

    #[tokio::test]
    async fn unsupported_formats_and_missing_capabilities_fail() {
        let data = [
            pkt(b"version 2\n"),
            pkt(b"object-format=sha256\n"),
            b"0000".to_vec(),
        ]
        .concat();
        assert!(advertisement(body(data), &work()).await.is_err());
        let data = [
            pkt(b"version 2\n"),
            pkt(b"fetch=shallow\n"),
            b"0000".to_vec(),
        ]
        .concat();
        assert!(
            advertisement(body(data), &work())
                .await
                .unwrap()
                .require_fetch(true)
                .is_err()
        );
    }

    #[tokio::test]
    async fn malformed_packets_and_trailing_data_fail() {
        for data in [
            b"zzzz".to_vec(),
            b"0003".to_vec(),
            b"ffff".to_vec(),
            b"0008a".to_vec(),
            [pkt(b"version 2\n"), b"0000junk".to_vec()].concat(),
        ] {
            assert!(advertisement(body(data), &work()).await.is_err());
        }
    }

    #[tokio::test]
    async fn peels_tags_and_filters_unrequested_names() {
        let a = "1111111111111111111111111111111111111111";
        let b = "2222222222222222222222222222222222222222";
        let data = [
            pkt(format!("{a} HEAD symref-target:refs/heads/main\n").as_bytes()),
            pkt(format!("{a} refs/tags/v1 peeled:{b}\n").as_bytes()),
            pkt(format!("{a} refs/pull/1/head\n").as_bytes()),
            b"0000".to_vec(),
        ]
        .concat();
        let result = refs(body(data), &work(), true).await.unwrap();
        assert_eq!(result.head_ref.as_deref(), Some("refs/heads/main"));
        assert_eq!(
            result.refs,
            vec![("refs/tags/v1".into(), Oid::from_str(b).unwrap())]
        );
    }

    #[tokio::test]
    async fn fragmented_packets_are_read_without_boundary_assumptions() {
        let data = [
            pkt(b"version 2\n"),
            pkt(b"fetch=filter shallow\n"),
            b"0000".to_vec(),
        ]
        .concat();
        let (mut tx, rx) = tokio::io::duplex(1);
        tokio::spawn(async move {
            for byte in data {
                tx.write_all(&[byte]).await.unwrap();
            }
        });
        advertisement(Box::pin(rx), &work())
            .await
            .unwrap()
            .require_fetch(true)
            .unwrap();
    }

    #[tokio::test]
    async fn stalled_stream_obeys_operation_deadline() {
        let (_tx, rx) = tokio::io::duplex(1);
        assert!(matches!(
            advertisement(Box::pin(rx), &Work::new(Duration::from_millis(10))).await,
            Err(GitError::Timeout(_))
        ));
    }

    async fn transfer(data: Vec<u8>) -> (Result<(), GitError>, Vec<u8>) {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("pack");
        let mut out = tokio::fs::File::create(&path).await.unwrap();
        let budget = super::super::operation::Budget::new("test", 1024 * 1024);
        let mut reservation = budget.reserve(0).unwrap();
        let result = fetch(
            body(data),
            &work(),
            &mut out,
            &mut reservation,
            &Limits::default(),
            Oid::zero(),
        )
        .await;
        out.flush().await.unwrap();
        drop(out);
        drop(reservation);
        assert_eq!(budget.used(), 0);
        (result, std::fs::read(path).unwrap())
    }

    #[tokio::test]
    async fn pack_bytes_span_packets_and_progress_is_not_output() {
        let raw = [b"PACK".as_slice(), &[0; 28]].concat();
        let mut data = [
            pkt(b"shallow-info\n"),
            pkt(b"shallow 1111111111111111111111111111111111111111\n"),
            b"0001".to_vec(),
            pkt(b"packfile\n"),
            pkt(b"\x02progress\n"),
        ]
        .concat();
        for chunk in raw.chunks(3) {
            data.extend(pkt(&[&[1], chunk].concat()));
        }
        data.extend_from_slice(b"00000002");
        let (result, output) = transfer(data).await;
        result.unwrap();
        assert_eq!(output, raw);
    }

    #[tokio::test]
    async fn fatal_invalid_want_is_object_missing_not_repository_missing() {
        for data in [
            pkt(b"ERR upload-pack: not our ref deadbeef\n"),
            [
                pkt(b"packfile\n"),
                pkt(b"\x03upload-pack: not our ref deadbeef\n"),
            ]
            .concat(),
        ] {
            assert!(matches!(
                transfer(data).await.0,
                Err(GitError::MissingObject(_))
            ));
        }
    }

    #[tokio::test]
    async fn unexpected_sections_channels_and_terminal_framing_fail() {
        let start = [
            pkt(b"packfile\n"),
            pkt(&[&[1u8][..], &[0u8; 32][..]].concat()),
        ]
        .concat();
        for suffix in [
            Vec::new(),
            b"0000junk".to_vec(),
            b"00020002".to_vec(),
            b"0001".to_vec(),
        ] {
            assert!(transfer([start.clone(), suffix].concat()).await.0.is_err());
        }
        assert!(transfer(pkt(b"packfile-uris\n")).await.0.is_err());
        assert!(
            transfer([pkt(b"packfile\n"), pkt(b"\x04invalid")].concat())
                .await
                .0
                .is_err()
        );
    }

    #[tokio::test]
    async fn every_truncation_of_an_advertisement_fails() {
        let data = [
            pkt(b"version 2\n"),
            pkt(b"fetch=filter shallow\n"),
            b"0000".to_vec(),
        ]
        .concat();
        for end in 0..data.len() {
            assert!(
                advertisement(body(data[..end].to_vec()), &work())
                    .await
                    .is_err(),
                "prefix {end}"
            );
        }
    }

    #[tokio::test]
    async fn abbreviated_network_oids_are_rejected() {
        let data = [pkt(b"1234 HEAD\n"), b"0000".to_vec()].concat();
        assert!(refs(body(data), &work(), false).await.is_err());
    }

    #[tokio::test]
    async fn control_traffic_cannot_renew_a_useful_progress_window() {
        let work = work();
        let mut reader = Reader::new(body(pkt(b"\x02progress")), &work);
        reader.control(100_000).unwrap();
        reader.window = Instant::now() - Duration::from_secs(21);
        assert!(matches!(reader.next().await, Err(GitError::Transport(_))));
    }
}
