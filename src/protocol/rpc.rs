use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::{BufRead, Write};

use super::messages::{Request, Response, RpcError};

/// JSON-RPC 2.0 request envelope
#[derive(Debug, Serialize, Deserialize)]
pub struct RpcRequest {
    pub jsonrpc: String,
    #[serde(flatten)]
    pub request: Request,
    pub id: Value,
}

/// JSON-RPC 2.0 success response envelope
#[derive(Debug, Serialize, Deserialize)]
pub struct RpcResponse {
    pub jsonrpc: String,
    pub result: Response,
    pub id: Value,
}

/// JSON-RPC 2.0 error response envelope
#[derive(Debug, Serialize, Deserialize)]
pub struct RpcErrorResponse {
    pub jsonrpc: String,
    pub error: RpcError,
    pub id: Value,
}

impl RpcRequest {
    pub fn new(request: Request, id: impl Into<Value>) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            request,
            id: id.into(),
        }
    }
}

impl RpcResponse {
    pub fn new(result: Response, id: Value) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            result,
            id,
        }
    }
}

impl RpcErrorResponse {
    pub fn new(error: RpcError, id: Value) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            error,
            id,
        }
    }
}

/// Write a JSON-RPC message as a single line (JSONL)
pub fn write_message<W: Write, T: Serialize>(writer: &mut W, msg: &T) -> std::io::Result<()> {
    let json = serde_json::to_string(msg)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    writeln!(writer, "{}", json)?;
    writer.flush()
}

/// Read a JSON-RPC request from a line
pub fn read_request<R: BufRead>(reader: &mut R) -> std::io::Result<Option<RpcRequest>> {
    let mut line = String::new();
    let n = reader.read_line(&mut line)?;
    if n == 0 {
        return Ok(None); // EOF
    }
    let req: RpcRequest = serde_json::from_str(&line)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    Ok(Some(req))
}

/// Read a JSON-RPC response (success or error) from a line
pub fn read_response<R: BufRead>(
    reader: &mut R,
) -> std::io::Result<Result<RpcResponse, RpcErrorResponse>> {
    let mut line = String::new();
    let n = reader.read_line(&mut line)?;
    if n == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "connection closed",
        ));
    }

    // Try to parse as success first
    if let Ok(resp) = serde_json::from_str::<RpcResponse>(&line) {
        return Ok(Ok(resp));
    }

    // Try error response
    if let Ok(err) = serde_json::from_str::<RpcErrorResponse>(&line) {
        return Ok(Err(err));
    }

    Err(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        "invalid JSON-RPC response",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_serialize_request_with_params() {
        let req = RpcRequest::new(
            Request::Sync {
                repo: "owner/repo".to_string(),
            },
            1,
        );
        let json = serde_json::to_string(&req).unwrap();

        // Verify JSON-RPC 2.0 format with flattened request
        assert!(json.contains(r#""jsonrpc":"2.0""#));
        assert!(json.contains(r#""method":"sync""#));
        assert!(json.contains(r#""params":{"repo":"owner/repo"}"#));
        assert!(json.contains(r#""id":1"#));
    }

    #[test]
    fn test_roundtrip_request() {
        let original = RpcRequest::new(
            Request::Sync {
                repo: "rust-lang/rust".to_string(),
            },
            42,
        );

        // Write to buffer
        let mut buffer = Vec::new();
        write_message(&mut buffer, &original).unwrap();

        // Read back
        let mut cursor = Cursor::new(buffer);
        let parsed = read_request(&mut cursor).unwrap().unwrap();

        assert_eq!(parsed.jsonrpc, "2.0");
        assert_eq!(parsed.id, 42);
        match parsed.request {
            Request::Sync { repo } => assert_eq!(repo, "rust-lang/rust"),
            _ => panic!("Expected Sync request"),
        }
    }

    #[test]
    fn test_roundtrip_request_with_string_id() {
        let original = RpcRequest::new(Request::Status, "req-123");

        let mut buffer = Vec::new();
        write_message(&mut buffer, &original).unwrap();

        let mut cursor = Cursor::new(buffer);
        let parsed = read_request(&mut cursor).unwrap().unwrap();

        assert_eq!(parsed.id, "req-123");
        assert!(matches!(parsed.request, Request::Status));
    }

    #[test]
    fn test_serialize_error_response() {
        let err_resp = RpcErrorResponse::new(RpcError::not_found("repo not found"), Value::from(1));
        let json = serde_json::to_string(&err_resp).unwrap();

        assert!(json.contains(r#""jsonrpc":"2.0""#));
        assert!(json.contains(r#""error":"#));
        assert!(json.contains(r#""code":-1"#));
        assert!(json.contains(r#""message":"repo not found""#));
        assert!(json.contains(r#""id":1"#));
    }

    #[test]
    fn test_roundtrip_success_response() {
        use super::super::messages::{Response, StatusResult};

        let original = RpcResponse::new(
            Response::Status(StatusResult {
                running: true,
                mount_point: "/mnt/ghfs".to_string(),
                repo_count: 10,
                uptime_secs: 7200,
            }),
            Value::from(5),
        );

        let mut buffer = Vec::new();
        write_message(&mut buffer, &original).unwrap();

        let mut cursor = Cursor::new(buffer);
        let result = read_response(&mut cursor).unwrap();

        match result {
            Ok(resp) => {
                assert_eq!(resp.jsonrpc, "2.0");
                assert_eq!(resp.id, 5);
                match resp.result {
                    Response::Status(status) => {
                        assert!(status.running);
                        assert_eq!(status.mount_point, "/mnt/ghfs");
                        assert_eq!(status.repo_count, 10);
                        assert_eq!(status.uptime_secs, 7200);
                    }
                    _ => panic!("Expected Status response"),
                }
            }
            Err(_) => panic!("Expected success response"),
        }
    }

    #[test]
    fn test_roundtrip_error_response() {
        let original =
            RpcErrorResponse::new(RpcError::internal("something went wrong"), Value::from(99));

        let mut buffer = Vec::new();
        write_message(&mut buffer, &original).unwrap();

        let mut cursor = Cursor::new(buffer);
        let result = read_response(&mut cursor).unwrap();

        match result {
            Err(err_resp) => {
                assert_eq!(err_resp.jsonrpc, "2.0");
                assert_eq!(err_resp.id, 99);
                assert_eq!(err_resp.error.code, -32603);
                assert_eq!(err_resp.error.message, "something went wrong");
            }
            Ok(_) => panic!("Expected error response"),
        }
    }

    #[test]
    fn test_read_request_eof() {
        let mut cursor = Cursor::new(Vec::<u8>::new());
        let result = read_request(&mut cursor).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_read_response_eof() {
        let mut cursor = Cursor::new(Vec::<u8>::new());
        let result = read_response(&mut cursor);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().kind(),
            std::io::ErrorKind::UnexpectedEof
        );
    }

    #[test]
    fn test_multiple_requests_on_same_stream() {
        let mut buffer = Vec::new();

        // Write multiple requests
        write_message(&mut buffer, &RpcRequest::new(Request::Status, 1)).unwrap();
        write_message(
            &mut buffer,
            &RpcRequest::new(
                Request::Sync {
                    repo: "foo/bar".to_string(),
                },
                2,
            ),
        )
        .unwrap();
        write_message(&mut buffer, &RpcRequest::new(Request::List, 3)).unwrap();

        // Read them back
        let mut cursor = Cursor::new(buffer);

        let req1 = read_request(&mut cursor).unwrap().unwrap();
        assert_eq!(req1.id, 1);
        assert!(matches!(req1.request, Request::Status));

        let req2 = read_request(&mut cursor).unwrap().unwrap();
        assert_eq!(req2.id, 2);
        assert!(matches!(req2.request, Request::Sync { .. }));

        let req3 = read_request(&mut cursor).unwrap().unwrap();
        assert_eq!(req3.id, 3);
        assert!(matches!(req3.request, Request::List));

        // EOF
        assert!(read_request(&mut cursor).unwrap().is_none());
    }
}
