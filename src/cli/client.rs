use std::io::{BufReader, BufWriter, Write};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;

use crate::protocol::{
    ListResult, Request, Response, RpcError, RpcRequest, StatusResult, SyncResult, read_response,
    write_message,
};

/// Get the socket path
/// Uses $XDG_RUNTIME_DIR/ghfs.sock on Linux
/// Falls back to /tmp/ghfs-$UID.sock
pub fn socket_path() -> PathBuf {
    if let Some(runtime_dir) = std::env::var_os("XDG_RUNTIME_DIR") {
        return PathBuf::from(runtime_dir).join("ghfs.sock");
    }

    let uid = unsafe { libc::getuid() };
    PathBuf::from(format!("/tmp/ghfs-{}.sock", uid))
}

/// Client for communicating with the daemon
pub struct Client {
    reader: BufReader<UnixStream>,
    writer: BufWriter<UnixStream>,
    next_id: u64,
}

#[derive(Debug)]
pub enum ClientError {
    /// Could not connect to daemon
    NotRunning,
    /// IO error
    Io(std::io::Error),
    /// RPC error from daemon
    Rpc(RpcError),
    /// Invalid response
    InvalidResponse(String),
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientError::NotRunning => write!(f, "daemon is not running"),
            ClientError::Io(e) => write!(f, "IO error: {}", e),
            ClientError::Rpc(e) => write!(f, "RPC error: {} (code {})", e.message, e.code),
            ClientError::InvalidResponse(s) => write!(f, "invalid response: {}", s),
        }
    }
}

impl std::error::Error for ClientError {}

fn is_not_running_io_error(err: &std::io::Error) -> bool {
    matches!(
        err.kind(),
        std::io::ErrorKind::ConnectionRefused
            | std::io::ErrorKind::NotFound
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::BrokenPipe
            | std::io::ErrorKind::UnexpectedEof
    ) || matches!(
        err.raw_os_error(),
        Some(libc::ECONNREFUSED)
            | Some(libc::ENOENT)
            | Some(libc::ECONNABORTED)
            | Some(libc::ECONNRESET)
            | Some(libc::EPIPE)
            | Some(libc::ENOTCONN)
    )
}

impl From<std::io::Error> for ClientError {
    fn from(e: std::io::Error) -> Self {
        if is_not_running_io_error(&e) {
            ClientError::NotRunning
        } else {
            ClientError::Io(e)
        }
    }
}

impl Client {
    /// Connect to the daemon
    pub fn connect() -> Result<Self, ClientError> {
        let path = socket_path();
        let stream = UnixStream::connect(&path)?;

        let reader = BufReader::new(stream.try_clone()?);
        let writer = BufWriter::new(stream);

        Ok(Self {
            reader,
            writer,
            next_id: 1,
        })
    }

    /// Send a request and wait for response
    pub fn call(&mut self, request: Request) -> Result<Response, ClientError> {
        let id = self.next_id;
        self.next_id += 1;

        let rpc_request = RpcRequest::new(request, id);
        write_message(&mut self.writer, &rpc_request)?;
        self.writer.flush()?;

        match read_response(&mut self.reader)? {
            Ok(response) => Ok(response.result),
            Err(error) => Err(ClientError::Rpc(error.error)),
        }
    }

    /// Convenience: get status
    pub fn status(&mut self) -> Result<StatusResult, ClientError> {
        match self.call(Request::Status)? {
            Response::Status(s) => Ok(s),
            other => Err(ClientError::InvalidResponse(format!("{:?}", other))),
        }
    }

    /// Convenience: sync a repo
    pub fn sync(&mut self, repo: &str) -> Result<SyncResult, ClientError> {
        match self.call(Request::Sync {
            repo: repo.to_string(),
        })? {
            Response::Sync(s) => Ok(s),
            other => Err(ClientError::InvalidResponse(format!("{:?}", other))),
        }
    }

    /// Convenience: watch a repo
    pub fn watch(&mut self, repo: &str) -> Result<(), ClientError> {
        match self.call(Request::Watch {
            repo: repo.to_string(),
        })? {
            Response::Ok(()) => Ok(()),
            other => Err(ClientError::InvalidResponse(format!("{:?}", other))),
        }
    }

    /// Convenience: unwatch a repo
    pub fn unwatch(&mut self, repo: &str) -> Result<(), ClientError> {
        match self.call(Request::Unwatch {
            repo: repo.to_string(),
        })? {
            Response::Ok(()) => Ok(()),
            other => Err(ClientError::InvalidResponse(format!("{:?}", other))),
        }
    }

    /// Convenience: unshallow a repo (fetch full history)
    pub fn unshallow(&mut self, repo: &str) -> Result<SyncResult, ClientError> {
        match self.call(Request::UnshallowRepo {
            repo: repo.to_string(),
        })? {
            Response::Sync(s) => Ok(s),
            other => Err(ClientError::InvalidResponse(format!("{:?}", other))),
        }
    }

    /// Convenience: reshallow a repo (convert back to depth=1)
    pub fn reshallow(&mut self, repo: &str) -> Result<SyncResult, ClientError> {
        match self.call(Request::ReshallowRepo {
            repo: repo.to_string(),
        })? {
            Response::Sync(s) => Ok(s),
            other => Err(ClientError::InvalidResponse(format!("{:?}", other))),
        }
    }

    /// Convenience: list repos
    pub fn list(&mut self) -> Result<ListResult, ClientError> {
        match self.call(Request::List)? {
            Response::List(l) => Ok(l),
            other => Err(ClientError::InvalidResponse(format!("{:?}", other))),
        }
    }

    /// Convenience: stop daemon
    pub fn stop(&mut self) -> Result<(), ClientError> {
        match self.call(Request::Stop)? {
            Response::Ok(()) => Ok(()),
            other => Err(ClientError::InvalidResponse(format!("{:?}", other))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ClientError;

    #[test]
    fn maps_broken_pipe_to_not_running() {
        let err = std::io::Error::from(std::io::ErrorKind::BrokenPipe);
        assert!(matches!(ClientError::from(err), ClientError::NotRunning));
    }

    #[test]
    fn keeps_unrelated_io_errors_as_io() {
        let err = std::io::Error::from(std::io::ErrorKind::PermissionDenied);
        assert!(matches!(ClientError::from(err), ClientError::Io(_)));
    }
}
