use serde::{Deserialize, Serialize};

/// All RPC methods supported by the daemon
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "method", content = "params", rename_all = "snake_case")]
pub enum Request {
    /// Get daemon status
    Status,

    /// Force sync a repo
    Sync { repo: String },

    /// Watch a repo (increase priority)
    Watch { repo: String },

    /// Unwatch a repo (reset priority)
    Unwatch { repo: String },

    /// Unshallow a repo (fetch full history)
    #[serde(rename = "unshallow")]
    UnshallowRepo { repo: String },

    /// Reshallow a repo (convert back to depth=1)
    #[serde(rename = "reshallow")]
    ReshallowRepo { repo: String },

    /// List all known repos
    List,

    /// Get daemon version
    Version,

    /// Stop the daemon
    Stop,
}

/// Status response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusResult {
    pub running: bool,
    pub mount_point: String,
    pub repo_count: usize,
    pub uptime_secs: u64,
    pub version: String,
    pub pid: u32,
    #[serde(default)]
    pub pending_syncs: Vec<String>,
}

/// Sync response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncResult {
    pub generation: u64,
    pub commit: String,
}

/// Single repo info for list
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepoInfo {
    pub owner: String,
    pub repo: String,
    pub priority: i32,
    pub generation: Option<u64>,
    pub commit: Option<String>,
    pub last_sync: Option<String>,   // Human-readable timestamp
    pub last_access: Option<String>, // Human-readable timestamp
    #[serde(default)]
    pub generation_count: u64,
    #[serde(default)]
    pub commit_count: u64,
    #[serde(default)]
    pub total_size_bytes: u64,
    /// Whether the mirror is a shallow clone (None if mirror doesn't exist)
    #[serde(default)]
    pub shallow: Option<bool>,
}

/// List response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListResult {
    pub repos: Vec<RepoInfo>,
}

/// Version response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionResult {
    pub version: String,
    pub pid: u32,
}

/// All possible success responses
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Response {
    Status(StatusResult),
    Sync(SyncResult),
    List(ListResult),
    Version(VersionResult),
    Ok(()), // For watch/unwatch/stop - unit type serializes as null
}

/// RPC error
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcError {
    pub code: i32,
    pub message: String,
}

impl RpcError {
    pub fn new(code: i32, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn invalid_params(msg: impl Into<String>) -> Self {
        Self::new(-32602, msg)
    }

    pub fn internal(msg: impl Into<String>) -> Self {
        Self::new(-32603, msg)
    }

    pub fn not_found(msg: impl Into<String>) -> Self {
        Self::new(-1, msg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_request_with_params() {
        let req = Request::Sync {
            repo: "owner/repo".to_string(),
        };
        let json = serde_json::to_string(&req).unwrap();

        // Should have method and params fields due to serde tag/content
        assert!(json.contains(r#""method":"sync""#));
        assert!(json.contains(r#""params":"#));
        assert!(json.contains(r#""repo":"owner/repo""#));
    }

    #[test]
    fn test_serialize_request_without_params() {
        let req = Request::Status;
        let json = serde_json::to_string(&req).unwrap();

        // Status has no params, so just method
        assert!(json.contains(r#""method":"status""#));
        // Should not have params field for unit variant
        assert!(!json.contains(r#""params""#));
    }

    #[test]
    fn test_serialize_version_request_without_params() {
        let req = Request::Version;
        let json = serde_json::to_string(&req).unwrap();

        assert!(json.contains(r#""method":"version""#));
        assert!(!json.contains(r#""params""#));
    }

    #[test]
    fn test_deserialize_request_with_params() {
        let json = r#"{"method":"sync","params":{"repo":"owner/repo"}}"#;
        let req: Request = serde_json::from_str(json).unwrap();

        match req {
            Request::Sync { repo } => assert_eq!(repo, "owner/repo"),
            _ => panic!("Expected Sync request"),
        }
    }

    #[test]
    fn test_deserialize_request_without_params() {
        let json = r#"{"method":"status"}"#;
        let req: Request = serde_json::from_str(json).unwrap();

        assert!(matches!(req, Request::Status));
    }

    #[test]
    fn test_serialize_status_result() {
        let result = StatusResult {
            running: true,
            mount_point: "/mnt/ghfs".to_string(),
            repo_count: 5,
            uptime_secs: 3600,
            version: "0.1.0".to_string(),
            pid: 1234,
            pending_syncs: vec![],
        };
        let json = serde_json::to_string(&result).unwrap();

        assert!(json.contains(r#""running":true"#));
        assert!(json.contains(r#""mount_point":"/mnt/ghfs""#));
        assert!(json.contains(r#""repo_count":5"#));
        assert!(json.contains(r#""uptime_secs":3600"#));
    }

    #[test]
    fn test_response_ok_serializes_as_null() {
        let resp = Response::Ok(());
        let json = serde_json::to_string(&resp).unwrap();
        assert_eq!(json, "null");
    }

    #[test]
    fn test_rpc_error_codes() {
        let invalid = RpcError::invalid_params("bad param");
        assert_eq!(invalid.code, -32602);

        let internal = RpcError::internal("server error");
        assert_eq!(internal.code, -32603);

        let not_found = RpcError::not_found("repo not found");
        assert_eq!(not_found.code, -1);
    }
}
