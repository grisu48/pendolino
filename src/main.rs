use std::env;
use std::process;
use std::thread;
use {
    interprocess::bound_util::{RefRead, RefWrite},
    interprocess::os::windows::named_pipe::*,
    std::io::prelude::*,
};

use std::net::TcpListener;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::mpsc::Sender;
use std::sync::mpsc::channel;

use anyhow::Result;
use anyhow::bail;

struct Config {
    named_pipe: String,
    bind_addr: String,
    verbose: bool,
    quiet: bool,
}

impl Config {
    // Create new configuration struct with the default settings
    pub fn new() -> Config {
        Config {
            named_pipe: "".to_string(),
            bind_addr: "127.0.0.1:2001".to_string(),
            verbose: false,
            quiet: false,
        }
    }

    // Parse program arguments. Returns errors as they occur
    pub fn parse_args(&mut self) -> Result<()> {
        let mut args = env::args();
        args.next(); // Consume program name

        let mut option = 0; // option counter

        for arg in args {
            if arg.is_empty() {
                continue;
            }

            // Check for argument or configuration option
            if arg.starts_with('-') {
                if arg == "-h" || arg == "--help" {
                    usage();
                    std::process::exit(0);
                } else if arg == "-v" || arg == "--verbose" {
                    self.verbose = true;
                    self.quiet = false
                } else if arg == "-q" || arg == "--quite" {
                    self.verbose = false;
                    self.quiet = true
                } else {
                    bail!("invalid argument: {arg}");
                }
            } else {
                if option == 0 {
                    let mut pipe = arg;
                    // Auto-expand pipe prefix, if not present
                    let prefix = r"\\.\pipe\";
                    if !pipe.starts_with(prefix) {
                        pipe = format!("{prefix}{pipe}");
                        if !self.quiet {
                            eprintln!("auto-expanding pipe path to {pipe}")
                        }
                    }
                    self.named_pipe = pipe;
                    option = 1
                } else if option == 1 {
                    self.bind_addr = arg;
                    option = 2;
                } else {
                    bail! {"unused program argument: {arg}"}
                }
            }
        }
        Ok(())
    }

    pub fn check(&self) -> Result<()> {
        if self.named_pipe == "" {
            bail!("undefined pipe");
        }
        if self.bind_addr == "" {
            bail!("undefined bind address");
        }
        Ok(())
    }
}

#[cfg(windows)]
fn main() {
    let mut config = Config::new();
    if let Err(err) = config.parse_args() {
        eprintln!("{err}");
        std::process::exit(1);
    }
    if let Err(err) = config.check() {
        eprintln!("{err}");
        std::process::exit(1);
    }

    let pipe =
        match DuplexPipeStream::<pipe_mode::Bytes>::connect_by_path(config.named_pipe.clone()) {
            Ok(pipe) => Arc::new(pipe),
            Err(err) => {
                eprintln!("error opening pipe: {err}");
                process::exit(1);
            }
        };
    if !config.quiet {
        eprintln!("pipe connected: {}", &config.named_pipe);
    }

    let listener = TcpListener::bind(&config.bind_addr).unwrap();
    if !config.quiet {
        eprintln!("listening on: {}", &config.bind_addr);
    }
    let (client, addr) = listener.accept().unwrap();
    let client = Arc::new(client);

    if !config.quiet {
        eprintln!("çlient connected: {addr}");
    }

    let read_bytes = Arc::new(AtomicU64::new(0));
    let written_bytes = Arc::new(AtomicU64::new(0));

    // Termination signal
    let (terminator, terminate) = channel();

    spawn_reader(
        &pipe,
        &client,
        terminator.clone(),
        config.verbose,
        read_bytes.clone(),
    );
    spawn_writer(
        &pipe,
        &client,
        terminator.clone(),
        config.verbose,
        written_bytes.clone(),
    );

    terminate.recv().unwrap(); // wait for termination signal

    if let Err(err) = client.shutdown(std::net::Shutdown::Both) {
        eprintln!("error closing connection: {err}");
    }
    if !config.quiet {
        let read = read_bytes.load(Ordering::Acquire);
        let written = written_bytes.load(Ordering::Acquire);
        eprintln!("{read} bytes read, {written} bytes written");
    }
    std::process::exit(1);
}

// Spawn the socket->pipe forwarding thread
#[cfg(windows)]
fn spawn_writer(
    pipe: &Arc<PipeStream<pipe_mode::Bytes, pipe_mode::Bytes>>,
    client: &Arc<std::net::TcpStream>,
    signal: Sender<()>,
    verbose: bool,
    counter: Arc<AtomicU64>,
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
                    signal.send(()).unwrap();
                    return;
                }
            };

            if n == 0 {
                // Socket closed?
                if verbose {
                    eprintln!("socket closed");
                }
                signal.send(()).unwrap();
                return;
            }

            if let Err(err) = pipe.write(&buf[0..n]) {
                eprintln!("write error: {err}");
                signal.send(()).unwrap();
                return;
            }

            if verbose {
                eprintln!("socket->pipe: {n} bytes");
            }
            counter.fetch_add(n as u64, Ordering::Acquire);
        }
    });
    w_thread
}

// Spawn the pipe->socket forwarding thread
#[cfg(windows)]
fn spawn_reader(
    pipe: &Arc<PipeStream<pipe_mode::Bytes, pipe_mode::Bytes>>,
    client: &Arc<std::net::TcpStream>,
    signal: Sender<()>,
    verbose: bool,
    counter: Arc<AtomicU64>,
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
                    signal.send(()).unwrap();
                    return;
                }
            };

            if n == 0 {
                // Pipe closed?
                if verbose {
                    eprintln!("pipe closed");
                }
                signal.send(()).unwrap();
                return;
            }

            if let Err(err) = output.write(&buf[0..n]) {
                eprintln!("send error: {err}");
                signal.send(()).unwrap();
                return;
            }
            if verbose {
                eprintln!("pipe->socket: {n} bytes");
            }
            counter.fetch_add(n as u64, Ordering::Acquire);
        }
    });
    r_thread
}

fn usage() {
    let progname = env::args().next().unwrap_or("???".to_string());
    println!("Usage: {progname} [OPTIONS] PIPE [BINDADDRESS]");
    println!("  PIPE                    Path to the named pipe");
    println!("  BINDADDRESS             Local address to bind listening tcp socket to");
    println!("");
    println!("OPTIONS:");
    println!("  -h, --help              Print this help");
    println!("  -v, --verbose           Verbose mode");
    println!("  -q, --quiet             Quiet mode");
}
