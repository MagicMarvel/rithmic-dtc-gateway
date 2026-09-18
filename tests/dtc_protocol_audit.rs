//! Offline regressions discovered by the 2026-09-04 protocol audit.
//! Fixed regressions. These tests use localhost only, with no upstream or orders.
use rithmic_dtc_bridge::dtc::handle_connection;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{Duration, timeout},
};

fn frame(size: usize, kind: u16) -> Vec<u8> {
    let mut bytes = vec![0; size];
    bytes[..2].copy_from_slice(&(size as u16).to_le_bytes());
    bytes[2..4].copy_from_slice(&kind.to_le_bytes());
    bytes
}

async fn read(stream: &mut TcpStream) -> Vec<u8> {
    let mut header = [0; 4];
    stream.read_exact(&mut header).await.unwrap();
    let mut bytes = vec![0; u16::from_le_bytes([header[0], header[1]]) as usize];
    bytes[..4].copy_from_slice(&header);
    stream.read_exact(&mut bytes[4..]).await.unwrap();
    bytes
}

async fn connect() -> TcpStream {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let _ = handle_connection(stream).await;
    });
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let mut logon = frame(284, 1);
    logon[4..8].copy_from_slice(&8_i32.to_le_bytes());
    logon[144..148].copy_from_slice(&5_i32.to_le_bytes());
    stream.write_all(&logon).await.unwrap();
    assert_eq!(&read(&mut stream).await[2..4], &2_u16.to_le_bytes());
    stream
}

#[tokio::test]
async fn fragmented_request_survives_outgoing_heartbeat() {
    let mut stream = connect().await;
    let mut request = frame(88, 506);
    request[4..8].copy_from_slice(&123_i32.to_le_bytes());
    request[8..12].copy_from_slice(b"ESU6");
    request[72..75].copy_from_slice(b"CME");
    stream.write_all(&request[..2]).await.unwrap();
    let heartbeat = timeout(Duration::from_secs(7), read(&mut stream))
        .await
        .unwrap();
    assert_eq!(&heartbeat[2..4], &3_u16.to_le_bytes());
    stream.write_all(&request[2..]).await.unwrap();
    let reply = timeout(Duration::from_secs(1), read(&mut stream))
        .await
        .expect("complete request must still produce a response after its header was fragmented");
    assert_eq!(&reply[4..8], &123_i32.to_le_bytes());
}

#[tokio::test]
async fn disabled_cancel_preserves_client_order_id_and_reason() {
    let mut stream = connect().await;
    let mut request = frame(100, 203);
    request[4..10].copy_from_slice(b"server");
    request[36..42].copy_from_slice(b"client");
    stream.write_all(&request).await.unwrap();
    let reply = timeout(Duration::from_secs(1), read(&mut stream))
        .await
        .unwrap();
    assert_eq!(
        &reply[160..166],
        b"client",
        "rejection must correlate to client order"
    );
    assert_eq!(
        &reply[228..232],
        &9_i32.to_le_bytes(),
        "cancel needs ORDER_CANCEL_REJECTED"
    );
}

#[tokio::test]
async fn history_request_without_optional_compression_tail_is_accepted() {
    let mut stream = connect().await;
    let mut request = frame(120, 800);
    request[4..8].copy_from_slice(&123_i32.to_le_bytes());
    request[8..12].copy_from_slice(b"ESU6");
    request[72..75].copy_from_slice(b"CME");
    stream.write_all(&request).await.unwrap();
    let reply = timeout(Duration::from_secs(1), read(&mut stream))
        .await
        .unwrap();
    assert_eq!(
        &reply[2..4],
        &802_u16.to_le_bytes(),
        "unavailable history must reject request without dropping session"
    );
    assert_eq!(&reply[4..8], &123_i32.to_le_bytes());
}
