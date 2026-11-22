//! Requests over HTTPS: the workspace builds `reqwest` with TLS, so clients,
//! nodes and coordinators can reach object storage and peers that only
//! answer over HTTPS.

use std::time::Duration;

use tokio::net::TcpListener;
use tokio::time::timeout;

#[tokio::test]
async fn an_https_request_reaches_the_server() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    // Without a TLS backend, reqwest refuses an https URL before it connects,
    // so a connection arriving here shows the request went out over TLS. The
    // handshake then fails, which doesn't matter.
    let accepted = tokio::spawn(async move {
        let accept = listener.accept();
        matches!(timeout(Duration::from_secs(5), accept).await, Ok(Ok(_)))
    });

    let result = reqwest::Client::new()
        .get(format!("https://{address}/"))
        .send()
        .await;

    assert!(result.is_err());
    assert!(accepted.await.unwrap());
}
