use super::{Client, Connection};
use crate::tds::codec::{BatchRequest, PacketHeader, TokenAltMetaData, TokenColMetaData};
use crate::SqlReadBytes;
use futures_util::io::{AsyncRead, AsyncWrite};
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

type Writes = Arc<Mutex<Vec<u8>>>;

struct Wire {
    input: io::Cursor<Vec<u8>>,
    writes: Writes,
    block_writes: bool,
    pending_at_end: bool,
}

impl AsyncRead for Wire {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        if self.pending_at_end && self.input.position() as usize == self.input.get_ref().len() {
            Poll::Pending
        } else {
            Poll::Ready(io::Read::read(&mut self.input, buf))
        }
    }
}

impl AsyncWrite for Wire {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.block_writes {
            Poll::Pending
        } else {
            self.writes.lock().unwrap().extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

fn frame(payload: &[u8], final_packet: bool) -> Vec<u8> {
    let size = u16::try_from(payload.len() + 8).unwrap().to_be_bytes();
    let mut bytes = vec![4, u8::from(final_packet), size[0], size[1], 0, 0, 1, 0];
    bytes.extend_from_slice(payload);
    bytes
}

fn done(status: u16) -> Vec<u8> {
    let mut token = vec![0xfd];
    token.extend_from_slice(&status.to_le_bytes());
    token.extend_from_slice(&[0; 10]);
    token
}

const ACK: &[u8] = &[0xe3, 3, 0, 18, 0, 0];

fn response() -> Vec<u8> {
    frame(&[ACK, &done(0)].concat(), true)
}

fn client(responses: &[u8], block_writes: bool, pending_at_end: bool) -> (Client<Wire>, Writes) {
    let writes = Arc::default();
    // The empty initial response lets flush_stream complete before the first send.
    let input = io::Cursor::new([frame(&[], true), responses.to_vec()].concat());
    let wire = Wire {
        input,
        writes: Arc::clone(&writes),
        block_writes,
        pending_at_end,
    };
    (
        Client {
            connection: Connection::test_over(wire, false),
        },
        writes,
    )
}

fn packets(writes: &[u8]) -> Vec<&[u8]> {
    let mut packets = Vec::new();
    let mut rest = writes;
    while !rest.is_empty() {
        let length = u16::from_be_bytes([rest[2], rest[3]]) as usize;
        packets.push(&rest[..length]);
        rest = &rest[length..];
    }
    packets
}

#[tokio::test]
async fn reset_sends_zero_transaction_and_only_one_reset_flag() {
    let (mut client, writes) = client(&[response(), frame(&done(0), true)].concat(), false, false);
    client
        .connection
        .context_mut()
        .set_transaction_descriptor([7; 8]);
    client
        .connection
        .context_mut()
        .set_last_meta(Arc::new(TokenColMetaData { columns: vec![] }));
    client
        .connection
        .context_mut()
        .set_alt_meta(Arc::new(TokenAltMetaData {
            id: 7,
            by_columns: vec![],
            columns: vec![],
        }));
    client.reset_connection().await.unwrap();
    assert!(client.connection.context().last_meta().is_none());
    assert!(client.connection.context().alt_meta(7).is_none());
    assert_eq!(client.connection.context().transaction_descriptor(), [0; 8]);
    client
        .simple_query("SELECT 1")
        .await
        .unwrap()
        .into_results()
        .await
        .unwrap();
    let writes = writes.lock().unwrap();
    let packets = packets(&writes);
    assert_eq!(packets.len(), 2);
    assert_eq!(&packets[0][..2], &[1, 0x09]);
    assert_eq!(&packets[0][18..26], &[0; 8]);
    assert_eq!(&packets[1][..2], &[1, 0x01]);
}

#[tokio::test]
async fn reset_flag_is_only_on_first_packet_of_split_request() {
    let (mut client, writes) = client(&response(), false, false);
    client.connection.context_mut().set_packet_size(32);
    client.reset_connection().await.unwrap();
    let writes = writes.lock().unwrap();
    let packets = packets(&writes);
    assert!(packets.len() >= 3);
    assert_eq!(packets[0][1], 0x08);
    assert_eq!(packets.last().unwrap()[1], 0x01);
    for packet in &packets[1..packets.len() - 1] {
        assert_eq!(packet[1], 0);
    }
}

#[tokio::test]
async fn reset_consumes_response_after_acknowledgment() {
    let replies = [frame(ACK, false), frame(&done(0), true), response()].concat();
    let (mut client, _) = client(&replies, false, false);
    client.reset_connection().await.unwrap();
    client.reset_connection().await.unwrap();
}

#[tokio::test]
async fn incomplete_or_failed_reset_rejects_reuse() {
    for reply in [
        frame(&done(0), true),                  // no acknowledgment
        frame(ACK, true),                       // no DONE
        frame(&[ACK, &done(2)].concat(), true), // DONE_ERROR
        frame(&[ACK, &done(1)].concat(), true), // DONE_MORE without completion
        frame(&[0xff], true),                   // malformed token
    ] {
        let (mut client, writes) = client(&reply, false, false);
        assert!(client.reset_connection().await.is_err());
        let before = writes.lock().unwrap().len();
        assert!(client.simple_query("SELECT 1").await.is_err());
        assert_eq!(writes.lock().unwrap().len(), before);
    }
}

#[tokio::test]
async fn cancellation_during_write_or_read_rejects_reuse() {
    for block_writes in [true, false] {
        let (mut client, _) = client(&frame(ACK, false), block_writes, true);
        let mut reset = Box::pin(client.reset_connection());
        assert!(futures_util::poll!(&mut reset).is_pending());
        drop(reset);
        assert!(client.simple_query("SELECT 1").await.is_err());
        assert!(client.reset_connection().await.is_err());
    }
}

#[tokio::test]
async fn reset_does_not_recover_poisoned_connection() {
    let (mut client, writes) = client(&[], false, false);
    client
        .connection
        .command_desync_flag()
        .store(true, std::sync::atomic::Ordering::Release);
    assert!(client.reset_connection().await.is_err());
    assert!(writes.lock().unwrap().is_empty());
}

#[tokio::test]
async fn ordinary_split_batch_does_not_set_reset_flag() {
    let (mut client, writes) = client(&[], false, false);
    client.connection.context_mut().set_packet_size(32);
    client
        .connection
        .send(
            PacketHeader::batch(1),
            BatchRequest::new("SELECT 123456789", [0; 8]),
        )
        .await
        .unwrap();
    let writes = writes.lock().unwrap();
    for packet in packets(&writes) {
        assert_eq!(packet[1] & 0x08, 0);
    }
}

#[tokio::test]
async fn server_error_during_reset_is_preserved_and_rejects_reuse() {
    // ERROR: code, state, class, empty message/server/procedure, line number.
    let mut body = 50000u32.to_le_bytes().to_vec();
    body.extend_from_slice(&[1, 16, 0, 0, 0, 0, 0, 0, 0, 0]);
    let mut error = vec![0xaa];
    error.extend_from_slice(&(body.len() as u16).to_le_bytes());
    error.extend_from_slice(&body);
    let reply = frame(&[ACK, &error, &done(2)].concat(), true);
    let (mut client, _) = client(&reply, false, false);
    let error = client.reset_connection().await.unwrap_err();
    assert!(matches!(error, crate::Error::Server(_)), "{error:?}");
    assert!(client.simple_query("SELECT 1").await.is_err());
}
