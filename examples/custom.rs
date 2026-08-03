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

/// This example shows how a custom [`Task`] implementation can be leveraged
/// to make the [`Selector`] more flexible.
#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let buffer = Vec::<u8>::with_capacity(1024);
    let mut selector = Selector::<TcpStreamHandler, _>::new(buffer);

    let mut interval = tokio::time::interval(Duration::from_secs(1));
    let mut stop = std::pin::pin!(tokio::time::sleep(Duration::from_secs(5)));

    loop {
        tokio::select! {
            Some(result) = selector.next() => match result {
                (addr, Ok(ControlFlow::Continue(data))) => {
                    println!("Peer {addr} connection got data: {}", String::from_utf8_lossy(&data));
                }
                (addr, Ok(ControlFlow::Break(()))) => {
                    println!("Peer {addr} connection closed");
                }
                (addr, Err(error)) => {
                    println!("Peer {addr} connection failed: {error}");
                }
            },

            conn = listener.accept() => {
                let (stream, addr) = conn.unwrap();
                println!("Accepted a new connection from {addr}, polling the task's read");
                selector.push(TcpStreamHandler {
                    peer_addr: addr,
                    stream,
                    write_offset: 0,
                });
            },

            _ = interval.tick() => {
                tokio::spawn(send_data(addr));
            },

            _ = &mut stop => {
                println!("Timeout elapsed, now polling all tasks' shutdown.");
                break;
            }
        }
    }

    // In this example, before polling with a different strategy,
    // we need to manually wake all tasks.
    // This is because previous poll left them waiting for incoming data.
    // Selector will not poll the task until it receives a wakeup.
    let selector = selector.with_strategy(b"bye bye".as_slice());
    selector.wake_all();

    selector
        .for_each(|(addr, result)| {
            match result {
                Ok(()) => println!("Peer {addr} connection closed"),
                Err(error) => println!("Peer {addr} connection failed: {error}"),
            }
            std::future::ready(())
        })
        .await;
}

async fn send_data(addr: SocketAddr) {
    let Ok(mut stream) = TcpStream::connect(addr).await else {
        return;
    };
    if stream.write_all(b"hello there").await.is_err() {
        return;
    }
    let mut buf = String::new();
    stream.read_to_string(&mut buf).await.unwrap();
    println!("Received goodbye message: {buf}");
}

struct TcpStreamHandler {
    peer_addr: SocketAddr,
    stream: TcpStream,
    write_offset: usize,
}

impl TcpStreamHandler {
    fn poll_read(
        &mut self,
        recv_buffer: &mut Vec<u8>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<Vec<u8>>> {
        let mut buf = ReadBuf::uninit(recv_buffer.spare_capacity_mut());
        let result = std::task::ready!(Pin::new(&mut self.stream).poll_read(cx, &mut buf))
            .map(|()| buf.filled().to_vec());
        recv_buffer.clear();
        Poll::Ready(result)
    }

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

impl Task<Vec<u8>> for TcpStreamHandler {
    type Cont = Vec<u8>;
    type Break = io::Result<()>;
    type Output = (SocketAddr, io::Result<ControlFlow<(), Vec<u8>>>);

    /// Thanks to the strategy argument, all handlers can read data into single a shared buffer.
    ///
    /// No synchronization or unsafe code needed.
    fn poll_progress(
        self: Pin<&mut Self>,
        buffer: &mut Vec<u8>,
        cx: &mut Context<'_>,
    ) -> Poll<ControlFlow<Self::Break, Self::Cont>> {
        let this = self.get_mut();
        let output = match std::task::ready!(this.poll_read(buffer, cx)) {
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
        Some((task.peer_addr, Ok(ControlFlow::Continue(value))))
    }

    fn transform_break(
        task: Removed<Self>,
        _: &mut Vec<u8>,
        value: Self::Break,
    ) -> Option<Self::Output> {
        Some((task.peer_addr, value.map(ControlFlow::Break)))
    }
}

impl Task<&[u8]> for TcpStreamHandler {
    type Cont = Infallible;
    type Break = io::Result<()>;
    type Output = (SocketAddr, Self::Break);

    /// Thanks to the strategy argument, all handlers have access to the goodbye message,
    /// while [`TcpStreamHandler`] type remains `'static`.
    fn poll_progress(
        self: Pin<&mut Self>,
        goodbye_message: &mut &[u8],
        cx: &mut Context<'_>,
    ) -> Poll<ControlFlow<Self::Break, Self::Cont>> {
        let this = self.get_mut();
        let output = std::task::ready!(this.poll_shutdown(goodbye_message, cx));
        Poll::Ready(ControlFlow::Break(output))
    }

    fn transform_cont(
        _: BorrowedMut<'_, Self>,
        _: &mut &[u8],
        _: Self::Cont,
    ) -> Option<Self::Output> {
        unreachable!("cannot construct std::convert::Infallible")
    }

    fn transform_break(
        task: Removed<Self>,
        _: &mut &[u8],
        value: Self::Break,
    ) -> Option<Self::Output> {
        Some((task.peer_addr, value))
    }
}
