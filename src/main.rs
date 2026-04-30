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

    eprintln!("pendolino v0.1");
    let mut pipe = NamedPipe::new(pipe).unwrap();
    let listener = TcpListener::bind(addr).await.unwrap();
    println!("{}<==>{addr}", pipe.path());
    let (mut socket, mut addr) = listener.accept().await.unwrap();
    println!("client {addr} connected");
    loop {
        match splice::splice(&mut pipe, &mut socket).await {
            Ok(_) => {
                panic!("unexpected EOF");
            }
            Err(e) => {
                // Allow client to reconnect
                // TODO: String matching is ugly. Find a better way to detect connection errors
                if e.kind() == std::io::ErrorKind::ConnectionAborted
                    || e.to_string() == "connection closed"
                {
                    eprintln!("client disconnected ({})", e.to_string());
                    (socket, addr) = listener.accept().await.unwrap();
                    eprintln!("client {addr} connected");
                }
                // Reconnect named pipe on pipe errors (e.g. when the VM reboots)
                else if let Err(_) = pipe.info() {
                    eprintln!("reconnecting pipe ... ");
                    if let Err(err) = pipe.reconnect(30) {
                        eprintln!("pipe reconnection failed: {err}");
                        std::process::exit(1_);
                    }

                    eprintln!("pipe reconnected");
                } else {
                    eprintln!("splice error: {e} ({})", e.kind());
                    std::process::exit(1);
                }
            }
        };
    }
}
