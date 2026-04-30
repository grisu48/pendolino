mod named_pipe;
mod splice;

use anyhow::{Result, bail};
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

struct Config {
    pipe: String,      // Path to named pipe to connect to
    bind_addr: String, // Local bind address
}

impl Config {
    // Create new configuration instance with the default settings
    pub fn new() -> Config {
        Config {
            pipe: "".to_string(),
            bind_addr: "127.0.0.1:2001".to_string(),
        }
    }

    pub fn from_args() -> Result<Config> {
        let mut config = Config::new();
        config.parse_args()?;
        Ok(config)
    }

    // Parse program arguments
    pub fn parse_args(&mut self) -> Result<()> {
        let mut args = std::env::args();
        args.next(); // Consume program name

        if let Some(mut pipe) = args.next() {
            // Add pipe prefix for localhost, if no prefix is given
            if !pipe.starts_with(r"\\") {
                pipe = format!(r"\\.\pipe\{pipe}")
            }
            self.pipe = pipe;
        } else {
            bail!("missing pipe argument");
        }

        if let Some(addr) = args.next() {
            self.bind_addr = addr
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let cf = match Config::from_args() {
        Ok(config) => config,
        Err(e) => {
            eprintln!("configuration error: {e}");
            std::process::exit(1);
        }
    };

    eprintln!("pendolino v0.1");
    let mut pipe = NamedPipe::new(cf.pipe.as_str()).unwrap();
    let listener = TcpListener::bind(cf.bind_addr.as_str()).await.unwrap();
    println!("{} <==> {}", pipe.path(), cf.bind_addr.as_str());
    let (mut socket, mut addr) = listener.accept().await.unwrap();
    println!("client {addr} connected");
    loop {
        match splice::splice(&mut pipe, &mut socket).await {
            Ok(_) => {
                panic!("unexpected EOF");
            }
            Err(e) => {
                // Allow client to reconnect
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
