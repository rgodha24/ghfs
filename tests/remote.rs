//! Real HTTP boundary tests; local upload-pack fixtures exercise a different seam.
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ghfs::git::{GitError, GitRemote};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn pkt(value: &[u8]) -> Vec<u8> {
    [format!("{:04x}", value.len() + 4).as_bytes(), value].concat()
}
fn advertisement() -> Vec<u8> {
    [
        pkt(b"version 2\n"),
        pkt(b"ls-refs=unborn\n"),
        pkt(b"fetch=filter shallow\n"),
        pkt(b"object-format=sha1\n"),
        b"0000".to_vec(),
    ]
    .concat()
}
fn refs() -> Vec<u8> {
    [
        pkt(b"1111111111111111111111111111111111111111 HEAD symref-target:refs/heads/main\n"),
        b"0000".to_vec(),
    ]
    .concat()
}
struct Reply {
    status: u16,
    content_type: &'static str,
    body: Vec<u8>,
    location: Option<&'static str>,
    fragment: usize,
    stall: bool,
}
impl Reply {
    fn discovery() -> Self {
        Self {
            status: 200,
            content_type: "application/x-git-upload-pack-advertisement",
            body: advertisement(),
            location: None,
            fragment: 1,
            stall: false,
        }
    }
    fn refs() -> Self {
        Self {
            status: 200,
            content_type: "application/x-git-upload-pack-result",
            body: refs(),
            location: None,
            fragment: 1,
            stall: false,
        }
    }
}
async fn server(
    replies: Vec<Reply>,
) -> (
    String,
    Arc<Mutex<Vec<Vec<u8>>>>,
    tokio::task::JoinHandle<()>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let requests = Arc::new(Mutex::new(Vec::new()));
    let recorded = requests.clone();
    let task = tokio::spawn(async move {
        for reply in replies {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                let mut byte = [0];
                socket.read_exact(&mut byte).await.unwrap();
                request.push(byte[0]);
                assert!(request.len() < 32 * 1024);
            }
            let header = String::from_utf8(request.clone()).unwrap();
            let length = header
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
                .unwrap_or(0);
            assert!(length < 1024 * 1024);
            let start = request.len();
            request.resize(start + length, 0);
            socket.read_exact(&mut request[start..]).await.unwrap();
            recorded.lock().unwrap().push(request);
            let location = reply
                .location
                .map(|v| format!("Location: {v}\r\n"))
                .unwrap_or_default();
            let headers = format!(
                "HTTP/1.1 {} test\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n{}\r\n",
                reply.status,
                reply.content_type,
                reply.body.len(),
                location
            );
            if socket.write_all(headers.as_bytes()).await.is_err() {
                return;
            }
            if reply.stall {
                // Client cancellation must close/drop its body without waiting
                // for the server to make progress.
                let mut byte = [0];
                let _ = socket.read(&mut byte).await;
                return;
            }
            for chunk in reply.body.chunks(reply.fragment) {
                if socket.write_all(chunk).await.is_err() {
                    return;
                }
                tokio::task::yield_now().await;
            }
        }
    });
    (url, requests, task)
}

#[tokio::test]
async fn http_headers_fragmentation_and_capability_cache() {
    let (base, recorded, task) =
        server(vec![Reply::discovery(), Reply::refs(), Reply::refs()]).await;
    let git = GitRemote::with_base_url(&base);
    let url = git.url("owner", "repo");
    assert_eq!(
        git.ls_remote_head(&url).await.unwrap().head_ref.as_deref(),
        Some("refs/heads/main")
    );
    git.ls_remote_all(&url).await.unwrap();
    task.await.unwrap();
    let requests = recorded.lock().unwrap();
    assert_eq!(requests.len(), 3);
    let get = String::from_utf8_lossy(&requests[0]);
    assert!(get.starts_with("GET /owner/repo.git/info/refs?service=git-upload-pack HTTP/1.1\r\n"));
    assert!(get.to_ascii_lowercase().contains("git-protocol: version=2"));
    for request in &requests[1..] {
        let request = String::from_utf8_lossy(request);
        assert!(request.starts_with("POST /owner/repo.git/git-upload-pack HTTP/1.1\r\n"));
        assert!(
            request
                .to_ascii_lowercase()
                .contains("content-type: application/x-git-upload-pack-request")
        );
        assert!(
            !request.to_ascii_lowercase().contains("authorization:"),
            "GitHub credentials must never reach another origin"
        );
        assert!(request.contains("0001"));
        assert!(request.ends_with("0000"));
    }
    assert_eq!(git.stats().discovery_requests, 1);
    assert_eq!(git.stats().reference_requests, 2);
}

#[tokio::test]
async fn only_not_found_responses_are_negative_cache_material() {
    for status in [404, 401, 403, 429, 500, 503] {
        let mut reply = Reply::discovery();
        reply.status = status;
        let (base, _, task) = server(vec![reply]).await;
        let git = GitRemote::with_base_url(&base);
        let error = git.ls_remote_head(&git.url("a", "b")).await.unwrap_err();
        assert_eq!(
            matches!(error, GitError::RepoNotFound(_)),
            // No credential is configured for this origin, so an anonymous
            // 401 means private-or-missing, exactly like 404.
            status == 404 || status == 401,
            "status {status}: {error}"
        );
        task.await.unwrap();
    }
}

#[tokio::test]
async fn redirects_are_rejected_without_following() {
    let mut reply = Reply::discovery();
    reply.status = 302;
    reply.location = Some("http://127.0.0.1:1/credential-target");
    let (base, requests, task) = server(vec![reply]).await;
    let git = GitRemote::with_base_url(&base);
    assert!(
        matches!(git.ls_remote_head(&git.url("a","b")).await,Err(GitError::Transport(message)) if message.contains("302"))
    );
    task.await.unwrap();
    assert_eq!(requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn stalled_response_body_and_immediate_retry_release_capacity() {
    let mut reply = Reply::discovery();
    reply.stall = true;
    let (base, _, task) = server(vec![reply]).await;
    let git = GitRemote::with_base_url(&base).with_operation_timeout(Duration::from_millis(50));
    assert!(matches!(
        git.ls_remote_head(&git.url("a", "b")).await,
        Err(GitError::Timeout(_))
    ));
    assert_eq!(git.stats().available_operation_slots, 24);
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap();
    git.shutdown().await;
    assert!(matches!(
        git.ls_remote_head(&git.url("a", "b")).await,
        Err(GitError::Cancelled)
    ));
}

#[tokio::test]
async fn malformed_http_content_type_is_rejected() {
    let mut reply = Reply::discovery();
    reply.content_type = "text/html";
    let (base, _, task) = server(vec![reply]).await;
    let git = GitRemote::with_base_url(&base);
    assert!(matches!(
        git.ls_remote_head(&git.url("a", "b")).await,
        Err(GitError::Protocol(_))
    ));
    task.await.unwrap();
}
