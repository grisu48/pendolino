use std::process;
use std::thread;
use {
    interprocess::bound_util::{RefRead, RefWrite},
    interprocess::os::windows::named_pipe::*,
    std::io::prelude::*,
};

use std::net::TcpListener;
use std::sync::Arc;

#[cfg(windows)]
fn main() {
    let pathname = r"\\.\pipe\Leap16";
    let addr = "127.0.0.1:1234";

    let listener = TcpListener::bind(addr).unwrap();
    eprintln!("listening on: {addr}");

    let pipe = match DuplexPipeStream::<pipe_mode::Bytes>::connect_by_path(pathname) {
        Ok(pipe) => Arc::new(pipe),
        Err(err) => {
            eprintln!("error opening pipe: {err}");
            process::exit(1);
        }
    };
    eprintln!("pipe connected: {pathname}");
    let (client, addr) = listener.accept().unwrap();
    let client = Arc::new(client);

    eprintln!("çlient connected: {addr}");

    let r_thread = spawn_reader(&pipe, &client);
    let w_thread = spawn_writer(&pipe, &client);

    r_thread.join().unwrap();
    w_thread.join().unwrap();
    eprintln!("bye")
}

// Spawn the socket->pipe forwarding thread
#[cfg(windows)]
fn spawn_writer(
    pipe: &Arc<PipeStream<pipe_mode::Bytes, pipe_mode::Bytes>>,
    client: &Arc<std::net::TcpStream>,
) -> thread::JoinHandle<()> {
    let p = Arc::clone(&pipe);
    let input = client.clone();
    let w_thread = thread::spawn(move || {
        let mut buf: [u8; 4096] = [0; 4096];
        let mut input = input.as_read();
        let mut pipe = p.as_write();
        loop {
            let n = match input.read(&mut buf) {
                Ok(buf) => buf,
                Err(err) => {
                    eprintln!("recv error: {err}");
                    return;
                }
            };
            if let Err(err) = pipe.write(&buf[0..n]) {
                eprintln!("write error: {err}");
                return;
            }

            eprintln!("socket->pipe: {n} bytes");
        }
    });
    w_thread
}

// Spawn the pipe->socket forwarding thread
#[cfg(windows)]
fn spawn_reader(
    pipe: &Arc<PipeStream<pipe_mode::Bytes, pipe_mode::Bytes>>,
    client: &Arc<std::net::TcpStream>,
) -> thread::JoinHandle<()> {
    let p = Arc::clone(pipe);
    let output = client.clone();
    let r_thread = thread::spawn(move || {
        let mut buf: [u8; 4096] = [0; 4096];
        let mut output = output.as_write();
        let pipe = p.as_read();

        let mut reader = pipe;
        loop {
            let n = match reader.read(&mut buf) {
                Ok(buf) => buf,
                Err(err) => {
                    eprintln!("read error: {err}");
                    return;
                }
            };

            if let Err(err) = output.write(&buf[0..n]) {
                eprintln!("send error: {err}");
            }
            eprintln!("pipe->socket: {n} bytes");
        }
    });
    r_thread
}
