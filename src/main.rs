mod named_pipe;
mod splice;

use tokio::net::{TcpListener, TcpStream};

use crate::named_pipe::NamedPipe;

impl splice::AsyncReadable for TcpStream {
    fn try_read(&self, buf: &mut [u8]) -> tokio::io::Result<usize> {
        Self::try_read(&self, buf)
    }

    async fn readable(&self) -> tokio::io::Result<()> {
        Self::readable(&self).await
    }
}

#[tokio::main]
async fn main() {
    let pipe = r"\\.\pipe\greenhorn";
    let addr = "192.168.122.189:2001";
    let mut pipe = NamedPipe::new(pipe).unwrap();
    let listener = TcpListener::bind(addr).await.unwrap();
    println!("{}<==>{addr}", pipe.path());
    let (mut socket, addr) = listener.accept().await.unwrap();
    println!("{addr} connected");
    splice::splice(&mut pipe, &mut socket).await.unwrap();
    eprintln!("bye");
}
