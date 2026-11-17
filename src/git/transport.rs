//! Streaming transport seam. Tests own the `upload-pack` subprocess adapter.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::TryStreamExt;
use reqwest::header::{ACCEPT, CONTENT_TYPE};
use tokio::io::AsyncRead;
use tokio::sync::Mutex;
use tokio_util::io::StreamReader;

use super::GitError;

pub type Body = Pin<Box<dyn AsyncRead + Send>>;
pub type IdentityFuture<'a> = Pin<Box<dyn Future<Output = u64> + Send + 'a>>;
pub type TransportFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Response, GitError>> + Send + 'a>>;

pub struct Response {
    pub status: u16,
    pub content_type: Option<String>,
    pub body: Body,
}

impl fmt::Debug for Response {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Response")
            .field("status", &self.status)
            .field("content_type", &self.content_type)
            .finish_non_exhaustive()
    }
}

pub trait Transport: Send + Sync + fmt::Debug {
    /// Opaque credential generation for capability-cache separation.
    fn identity<'a>(&'a self, _url: &'a str) -> IdentityFuture<'a> {
        Box::pin(async { 0 })
    }
    /// Advertise protocol-v2 capabilities for a repository URL.
    fn discover<'a>(&'a self, url: &'a str) -> TransportFuture<'a>;
    /// Execute one stateless protocol-v2 command against a repository URL.
    fn command<'a>(&'a self, url: &'a str, body: Bytes) -> TransportFuture<'a>;
}

struct CachedToken {
    at: Instant,
    value: Option<String>,
    environment: bool,
    identity: u64,
}

struct Credential {
    value: Option<String>,
    identity: u64,
}
type TokenFuture<'a> = Pin<Box<dyn Future<Output = Credential> + Send + 'a>>;
trait TokenSource: Send + Sync {
    fn get(&self, force: bool) -> TokenFuture<'_>;
}

/// Token values never participate in Debug or error output.
struct Tokens {
    cached: Mutex<Option<CachedToken>>,
    generation: AtomicU64,
}

impl Tokens {
    async fn load(&self, force: bool) -> Credential {
        let environment = ["GH_TOKEN", "GITHUB_TOKEN"]
            .iter()
            .find_map(|key| std::env::var(key).ok().filter(|s| !s.is_empty()));
        let mut cached = self.cached.lock().await;
        if environment.is_none()
            && !force
            && let Some(entry) = cached.as_ref()
            && !entry.environment
            && entry.at.elapsed() < Duration::from_secs(3600)
        {
            return Credential {
                value: entry.value.clone(),
                identity: entry.identity,
            };
        }
        let from_environment = environment.is_some();
        let value = if from_environment {
            environment
        } else {
            // Cache absence too: public browsing must not repeatedly spawn gh.
            let mut command = tokio::process::Command::new("gh");
            command
                .args(["auth", "token", "--hostname", "github.com"])
                .stdin(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .stdout(std::process::Stdio::piped())
                .kill_on_drop(true);
            match command.spawn() {
                Ok(mut child) => {
                    use tokio::io::AsyncReadExt;
                    let stdout = child.stdout.take();
                    let read = async {
                        let mut token = Vec::new();
                        let stdout = stdout?;
                        stdout
                            .take(16 * 1024 + 1)
                            .read_to_end(&mut token)
                            .await
                            .ok()?;
                        if token.len() > 16 * 1024 {
                            return None;
                        }
                        if !child.wait().await.ok()?.success() {
                            return None;
                        }
                        let token = String::from_utf8(token).ok()?.trim().to_owned();
                        (!token.is_empty()).then_some(token)
                    };
                    tokio::time::timeout(Duration::from_secs(5), read)
                        .await
                        .ok()
                        .flatten()
                }
                Err(_) => None,
            }
        };
        let changed = cached.as_ref().map(|entry| entry.value.as_ref()) != Some(value.as_ref());
        let identity = if changed {
            self.generation.fetch_add(1, Ordering::AcqRel) + 1
        } else {
            self.generation.load(Ordering::Acquire)
        };
        *cached = Some(CachedToken {
            at: Instant::now(),
            value: value.clone(),
            environment: from_environment,
            identity,
        });
        Credential { value, identity }
    }
}

impl TokenSource for Tokens {
    fn get(&self, force: bool) -> TokenFuture<'_> {
        Box::pin(self.load(force))
    }
}

pub struct HttpsTransport {
    client: reqwest::Client,
    auth_origin: Option<String>,
    tokens: Arc<dyn TokenSource>,
}

impl fmt::Debug for HttpsTransport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HttpsTransport")
            .field("auth_origin", &self.auth_origin)
            .finish_non_exhaustive()
    }
}

impl HttpsTransport {
    pub fn new(base_url: &str) -> Self {
        let auth_origin = reqwest::Url::parse(base_url)
            .ok()
            .filter(|url| {
                url.scheme() == "https"
                    && url.host_str() == Some("github.com")
                    && url.port_or_known_default() == Some(443)
            })
            .map(|url| url.origin().ascii_serialization());
        Self {
            client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .connect_timeout(Duration::from_secs(15))
                .read_timeout(Duration::from_secs(30))
                .user_agent(concat!("ghfs/", env!("CARGO_PKG_VERSION")))
                .build()
                .expect("valid static HTTP client configuration"),
            auth_origin,
            tokens: Arc::new(Tokens {
                cached: Mutex::new(None),
                generation: AtomicU64::new(0),
            }),
        }
    }

    async fn request(&self, url: &str, body: Option<Bytes>) -> Result<Response, GitError> {
        let mut target = reqwest::Url::parse(url)
            .map_err(|_| GitError::Transport("invalid remote URL".into()))?;
        if !matches!(target.scheme(), "http" | "https")
            || !target.username().is_empty()
            || target.password().is_some()
            || target.query().is_some()
            || target.fragment().is_some()
        {
            return Err(GitError::Transport(
                "remote URL must be HTTP(S), without embedded credentials, query or fragment"
                    .into(),
            ));
        }
        let auth_allowed =
            self.auth_origin.as_deref() == Some(target.origin().ascii_serialization().as_str());
        target.set_path(&format!(
            "{}/{}",
            target.path().trim_end_matches('/'),
            if body.is_some() {
                "git-upload-pack"
            } else {
                "info/refs"
            }
        ));
        if body.is_none() {
            target.set_query(Some("service=git-upload-pack"));
        }
        for attempt in 0..2 {
            let token = if auth_allowed {
                self.tokens.get(attempt != 0).await.value
            } else {
                None
            };
            let mut request = if let Some(body) = body.as_ref() {
                self.client
                    .post(target.clone())
                    .header(CONTENT_TYPE, "application/x-git-upload-pack-request")
                    .header(ACCEPT, "application/x-git-upload-pack-result")
                    .body(body.clone())
            } else {
                self.client
                    .get(target.clone())
                    .header(ACCEPT, "application/x-git-upload-pack-advertisement")
            }
            .header("Git-Protocol", "version=2");
            let anonymous = token.is_none();
            if let Some(token) = token {
                request = request.basic_auth("x-access-token", Some(token));
            }
            let response = request
                .send()
                .await
                .map_err(|error| GitError::Transport(error.without_url().to_string()))?;
            if response.status() == reqwest::StatusCode::UNAUTHORIZED
                && auth_allowed
                && attempt == 0
            {
                // Bound refresh to one retry; its body is deliberately dropped.
                continue;
            }
            let status = match response.status().as_u16() {
                // Smart HTTP answers anonymous requests for private and
                // nonexistent repositories alike with 401. Without any
                // credential to offer, that is "not found" for us, and so
                // becomes negative-cache material instead of EIO.
                401 if anonymous => 404,
                status => status,
            };
            let content_type = response
                .headers()
                .get(CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned);
            let body = response.bytes_stream().map_err(std::io::Error::other);
            return Ok(Response {
                status,
                content_type,
                body: Box::pin(StreamReader::new(body)),
            });
        }
        unreachable!("second HTTP attempt always returns")
    }
}

impl Transport for HttpsTransport {
    fn identity<'a>(&'a self, url: &'a str) -> IdentityFuture<'a> {
        Box::pin(async move {
            let allowed = reqwest::Url::parse(url).ok().is_some_and(|url| {
                self.auth_origin.as_deref() == Some(url.origin().ascii_serialization().as_str())
            });
            if allowed {
                self.tokens.get(false).await.identity
            } else {
                0
            }
        })
    }

    fn discover<'a>(&'a self, url: &'a str) -> TransportFuture<'a> {
        Box::pin(self.request(url, None))
    }

    fn command<'a>(&'a self, url: &'a str, body: Bytes) -> TransportFuture<'a> {
        Box::pin(self.request(url, Some(body)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    struct FakeTokens(AtomicU64);
    impl TokenSource for FakeTokens {
        fn get(&self, force: bool) -> TokenFuture<'_> {
            Box::pin(async move {
                self.0.fetch_add(1, Ordering::Relaxed);
                Credential {
                    value: Some(
                        if force {
                            "new-test-token"
                        } else {
                            "old-test-token"
                        }
                        .into(),
                    ),
                    identity: if force { 2 } else { 1 },
                }
            })
        }
    }

    #[tokio::test]
    async fn authentication_refresh_is_bounded_and_never_follows_redirects() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let mut auth = Vec::new();
            for _ in 0..2 {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut header = Vec::new();
                while !header.ends_with(b"\r\n\r\n") {
                    let mut byte = [0];
                    socket.read_exact(&mut byte).await.unwrap();
                    header.push(byte[0]);
                }
                let header = String::from_utf8(header).unwrap();
                auth.push(
                    header
                        .lines()
                        .find(|line| line.to_ascii_lowercase().starts_with("authorization:"))
                        .unwrap()
                        .to_owned(),
                );
                socket
                    .write_all(
                        b"HTTP/1.1 401 rejected\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    )
                    .await
                    .unwrap();
            }
            auth
        });
        let tokens = Arc::new(FakeTokens(AtomicU64::new(0)));
        let mut transport = HttpsTransport::new(&base);
        // Deliberately inject dummy credentials and this test-only origin.
        // Actual environment/helper tokens are never read by this mock.
        transport.auth_origin = Some(base.clone());
        transport.tokens = tokens.clone();
        let response = transport
            .discover(&format!("{base}/owner/repo.git"))
            .await
            .unwrap();
        assert_eq!(response.status, 401);
        let auth = server.await.unwrap();
        assert_ne!(auth[0], auth[1]);
        assert!(auth.iter().all(|line| {
            line.to_ascii_lowercase()
                .starts_with("authorization: basic ")
        }));
        assert_eq!(
            tokens.0.load(Ordering::Relaxed),
            2,
            "a second rejection must not refresh again"
        );
    }

    #[tokio::test]
    async fn credentials_are_never_looked_up_for_another_origin() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut header = Vec::new();
            while !header.ends_with(b"\r\n\r\n") {
                let mut byte = [0];
                socket.read_exact(&mut byte).await.unwrap();
                header.push(byte[0]);
            }
            assert!(
                !String::from_utf8_lossy(&header)
                    .to_ascii_lowercase()
                    .contains("authorization:")
            );
            socket
                .write_all(
                    b"HTTP/1.1 404 missing\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .await
                .unwrap();
        });
        let tokens = Arc::new(FakeTokens(AtomicU64::new(0)));
        let mut transport = HttpsTransport::new("https://github.com");
        transport.tokens = tokens.clone();
        assert_eq!(transport.identity(&format!("{base}/a/b.git")).await, 0);
        transport
            .discover(&format!("{base}/a/b.git"))
            .await
            .unwrap();
        server.await.unwrap();
        assert_eq!(tokens.0.load(Ordering::Relaxed), 0);
    }
}
