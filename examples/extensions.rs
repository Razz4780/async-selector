use std::{
    io,
    net::SocketAddr,
    ops::ControlFlow,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use async_selector::{
    pollable::{PollStrategy, PollWith},
    selector::Selector,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf},
    net::{TcpListener, TcpStream},
};

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mut buffer = Vec::<u8>::with_capacity(1024);
    let mut selector = Selector::<Strategy>::default();

    let mut interval = tokio::time::interval(Duration::from_secs(1));
    let mut stop = std::pin::pin!(tokio::time::sleep(Duration::from_secs(5)));

    loop {
        tokio::select! {
            Some(result) = selector.next_with_ext(&(), &mut buffer) => match result {
                Ok((addr, data)) if data.is_empty() => {
                    println!("Peer {addr} closed connection");
                }
                Ok((addr, data)) => {
                    println!("Peer {addr} sent data: {}", String::from_utf8_lossy(&data));
                }
                Err(error) => {
                    println!("Peer {addr} connection failed: {error}");
                }
            },

            conn = listener.accept() => {
                let (stream, addr) = conn.unwrap();
                println!("Accepted a new connection from {addr}");
                selector.push(TcpStreamHandler {
                    peer_addr: addr,
                    stream,
                    write_offset: 0,
                });
            },

            _ = interval.tick() => {
                tokio::spawn(send_data(addr));
            },

            _ = &mut stop => break,
        }
    }

    selector.wake_all();

    while selector
        .next_with_ext(b"bye bye".as_slice(), &mut ())
        .await
        .transpose()
        .unwrap()
        .is_some()
    {}
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
            self.write_offset += written;
        }
    }
}

#[derive(Default)]
struct Strategy;

impl PollStrategy for Strategy {
    type Pollable = TcpStreamHandler;
}

impl<'a> PollWith<'a, (), Vec<u8>> for Strategy {
    type Item = io::Result<(SocketAddr, Vec<u8>)>;
    type Progress = io::Result<(SocketAddr, Vec<u8>)>;

    fn poll_progress(
        state: Pin<&mut Self::Pollable>,
        _: &(),
        recv_buf: &mut Vec<u8>,
        cx: &mut Context<'_>,
    ) -> Poll<Self::Progress> {
        let this = state.get_mut();
        let data = std::task::ready!(this.poll_read(recv_buf, cx))?;
        Poll::Ready(Ok((this.peer_addr, data)))
    }

    fn try_unwrap(
        &mut self,
        output: Self::Progress,
    ) -> ControlFlow<Option<Self::Item>, Self::Item> {
        match output {
            Ok((addr, data)) if data.is_empty() => ControlFlow::Break(Some(Ok((addr, data)))),
            Ok((addr, data)) => ControlFlow::Continue(Ok((addr, data))),
            Err(error) => ControlFlow::Break(Some(Err(error))),
        }
    }
}

impl<'a> PollWith<'a, [u8], ()> for Strategy {
    type Item = io::Result<SocketAddr>;
    type Progress = io::Result<SocketAddr>;

    fn poll_progress(
        state: Pin<&mut Self::Pollable>,
        goodbye_message: &[u8],
        _: &mut (),
        cx: &mut Context<'_>,
    ) -> Poll<Self::Progress> {
        let this = state.get_mut();
        std::task::ready!(this.poll_shutdown(goodbye_message, cx))?;
        Poll::Ready(Ok(this.peer_addr))
    }

    fn try_unwrap(
        &mut self,
        output: Self::Progress,
    ) -> ControlFlow<Option<Self::Item>, Self::Item> {
        ControlFlow::Break(Some(output))
    }
}
