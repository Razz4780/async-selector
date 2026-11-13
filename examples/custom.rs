//! This example shows how a custom [`Task`] implementation can make the [`Selector`]
//! more flexible.
//!
//! We run a small TCP server, which handles all of its connections in a single [`Selector`].
//! The connection handler implements [`Task`] for two different strategies:
//!
//! 1. `Vec<u8>` - a receive buffer shared by all connections.
//!    With this strategy, the selector reads data from the connections.
//! 2. `&[u8]` - a goodbye message.
//!    With this strategy, the selector sends the message to every connection, and closes it.
//!
//! The server accepts connections and reads from them for 5 seconds.
//! Then, it switches the selector to the second strategy and gracefully shuts all connections down.
//! Meanwhile, a new client connects every second, sends a greeting, and waits for the goodbye message.
//!
//! Run it with `cargo run --example custom`.

use std::{
    convert::Infallible,
    io,
    net::SocketAddr,
    ops::ControlFlow,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use async_selector::{
    selector::{BorrowedMut, Removed, Selector},
    task::Task,
};
use futures::StreamExt;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf},
    net::{TcpListener, TcpStream},
};

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // The selector starts with the "read" strategy: a receive buffer shared by all connections.
    let buffer = Vec::<u8>::with_capacity(1024);
    let mut selector = Selector::<ConnectionHandler, _>::new(buffer);

    // Spawn a new client every second.
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    // Start the shutdown after 5 seconds.
    let mut stop = std::pin::pin!(tokio::time::sleep(Duration::from_secs(5)));

    loop {
        tokio::select! {
            // One of the connections produced a result.
            // When the selector is empty, `next` returns `None`, which disables this branch.
            Some(result) = selector.next() => match result {
                (addr, Ok(ControlFlow::Continue(data))) => {
                    println!("Peer {addr} sent: {}", String::from_utf8_lossy(&data));
                }
                (addr, Ok(ControlFlow::Break(()))) => {
                    println!("Peer {addr} closed the connection");
                }
                (addr, Err(error)) => {
                    println!("Connection with peer {addr} failed: {error}");
                }
            },

            // A new client connected, start handling the connection in the selector.
            conn = listener.accept() => {
                let (stream, addr) = conn.unwrap();
                println!("Accepted a new connection from {addr}");
                selector.push(ConnectionHandler {
                    peer_addr: addr,
                    stream,
                    write_offset: 0,
                });
            },

            _ = interval.tick() => {
                tokio::spawn(run_client(addr));
            },

            _ = &mut stop => {
                println!("Time is up, shutting all connections down");
                break;
            }
        }
    }

    // Switch the selector to the "shutdown" strategy: the goodbye message.
    // All connections are kept.
    let selector = selector.with_strategy(b"bye bye".as_slice());

    // The selector only polls a task after the task is woken.
    // Right now, all connections are waiting for incoming data,
    // so they would not be polled until a client sends something.
    // We wake them manually, so that they start sending the goodbye message right away.
    selector.wake_all();

    // Drive all connections to completion.
    // With this strategy, each connection yields exactly one result.
    selector
        .for_each(|(addr, result)| {
            match result {
                Ok(()) => println!("Said goodbye to peer {addr}"),
                Err(error) => println!("Failed to say goodbye to peer {addr}: {error}"),
            }
            std::future::ready(())
        })
        .await;

    // Give the clients a moment to print the goodbye message before the runtime shuts down.
    tokio::time::sleep(Duration::from_millis(100)).await;
}

/// Connects to the server, sends a greeting, and waits for the goodbye message.
async fn run_client(addr: SocketAddr) {
    let Ok(mut stream) = TcpStream::connect(addr).await else {
        return;
    };
    if stream.write_all(b"hello there").await.is_err() {
        return;
    }
    let mut buf = String::new();
    stream.read_to_string(&mut buf).await.unwrap();
    println!("Client received a goodbye message: {buf}");
}

/// Server side of a single TCP connection.
struct ConnectionHandler {
    peer_addr: SocketAddr,
    stream: TcpStream,
    /// How much of the goodbye message we have already sent.
    write_offset: usize,
}

impl ConnectionHandler {
    /// Reads the next chunk of data from the connection.
    ///
    /// Returns an empty chunk when the peer closes the connection.
    fn poll_read(
        &mut self,
        recv_buffer: &mut Vec<u8>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<Vec<u8>>> {
        // Read into the buffer's spare capacity. The buffer's length always stays 0,
        // so the whole capacity is available for every read.
        let mut buf = ReadBuf::uninit(recv_buffer.spare_capacity_mut());
        let result = std::task::ready!(Pin::new(&mut self.stream).poll_read(cx, &mut buf))
            // Copy the data out of the shared buffer, so the next read can reuse it.
            .map(|()| buf.filled().to_vec());
        Poll::Ready(result)
    }

    /// Sends the rest of the goodbye message, and then shuts the connection down.
    fn poll_shutdown(
        &mut self,
        goodbye_message: &[u8],
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            let data = goodbye_message.get(self.write_offset..).unwrap();
            if data.is_empty() {
                return Pin::new(&mut self.stream).poll_shutdown(cx);
            }
            let written = std::task::ready!(Pin::new(&mut self.stream).poll_write(cx, data))?;
            if written == 0 {
                break Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
            }
            self.write_offset += written;
        }
    }
}

/// The "read" strategy: the strategy value is a receive buffer shared by all connections.
impl Task<Vec<u8>> for ConnectionHandler {
    /// A chunk of data received from the peer.
    type Cont = Vec<u8>;
    /// The connection was closed by the peer (`Ok`), or failed (`Err`).
    type Break = io::Result<()>;
    /// What the selector yields: the peer address, and either a chunk of data
    /// (`Continue`), or the end of the connection (`Break`).
    type Output = (SocketAddr, io::Result<ControlFlow<(), Vec<u8>>>);

    fn poll_progress(
        self: Pin<&mut Self>,
        buffer: &mut Vec<u8>,
        cx: &mut Context<'_>,
    ) -> Poll<ControlFlow<Self::Break, Self::Cont>> {
        // Thanks to the strategy argument, all connections read into a single shared buffer.
        // No synchronization or unsafe code needed.
        let this = self.get_mut();
        let output = match std::task::ready!(this.poll_read(buffer, cx)) {
            // An empty read means that the peer closed the connection.
            Ok(data) if data.is_empty() => ControlFlow::Break(Ok(())),
            Ok(data) => ControlFlow::Continue(data),
            Err(error) => ControlFlow::Break(Err(error)),
        };
        Poll::Ready(output)
    }

    fn transform_cont(
        task: BorrowedMut<'_, Self>,
        _: &mut Vec<u8>,
        value: Self::Cont,
    ) -> Option<Self::Output> {
        // Attach the peer address to the data.
        Some((task.peer_addr, Ok(ControlFlow::Continue(value))))
    }

    fn transform_break(
        task: Removed<Self>,
        _: &mut Vec<u8>,
        value: Self::Break,
    ) -> Option<Self::Output> {
        // The connection is done, and it was removed from the selector.
        // We only keep its address, and let the rest drop.
        Some((task.peer_addr, value.map(ControlFlow::Break)))
    }
}

/// The "shutdown" strategy: the strategy value is the goodbye message.
impl Task<&[u8]> for ConnectionHandler {
    /// Shutting down produces no intermediate values.
    type Cont = Infallible;
    /// The result of the shutdown.
    type Break = io::Result<()>;
    /// What the selector yields: the peer address and the result of the shutdown.
    type Output = (SocketAddr, Self::Break);

    fn poll_progress(
        self: Pin<&mut Self>,
        goodbye_message: &mut &[u8],
        cx: &mut Context<'_>,
    ) -> Poll<ControlFlow<Self::Break, Self::Cont>> {
        // Thanks to the strategy argument, all connections have access to the goodbye message,
        // while the `ConnectionHandler` type remains `'static`.
        let this = self.get_mut();
        let output = std::task::ready!(this.poll_shutdown(goodbye_message, cx));
        Poll::Ready(ControlFlow::Break(output))
    }

    fn transform_cont(
        _: BorrowedMut<'_, Self>,
        _: &mut &[u8],
        value: Self::Cont,
    ) -> Option<Self::Output> {
        // `Infallible` has no values, so this method is never called.
        match value {}
    }

    fn transform_break(
        task: Removed<Self>,
        _: &mut &[u8],
        value: Self::Break,
    ) -> Option<Self::Output> {
        Some((task.peer_addr, value))
    }
}
