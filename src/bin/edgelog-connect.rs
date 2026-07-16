use std::env;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::thread;

#[derive(Clone, Debug)]
struct Args {
    server: String,
    path: Vec<String>,
    target: Option<String>,
    local_listen: Option<String>,
    list_tunnels: bool,
}

impl Args {
    fn parse() -> Self {
        let mut args = env::args().skip(1);
        let mut parsed = Self {
            server: "127.0.0.1:7777".to_string(),
            path: Vec::new(),
            target: None,
            local_listen: None,
            list_tunnels: false,
        };

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--server" => {
                    parsed.server = args
                        .next()
                        .unwrap_or_else(|| usage_exit("--server requires HOST:PORT"));
                }
                "--path" => {
                    let path = args
                        .next()
                        .unwrap_or_else(|| usage_exit("--path requires hop[/hop...]"));
                    parsed.path = path
                        .split('/')
                        .filter(|hop| !hop.is_empty())
                        .map(str::to_string)
                        .collect();
                }
                "--target" | "--tunnel" => {
                    parsed.target = Some(
                        args.next()
                            .unwrap_or_else(|| usage_exit("--target requires a tunnel name")),
                    );
                }
                "--local-listen" | "--listen" => {
                    parsed.local_listen = Some(
                        args.next()
                            .unwrap_or_else(|| usage_exit("--local-listen requires HOST:PORT")),
                    );
                }
                "--tunnels" => parsed.list_tunnels = true,
                "--help" | "-h" => usage_exit(""),
                other => usage_exit(&format!("unknown argument: {other}")),
            }
        }

        parsed
    }
}

fn usage_exit(message: &str) -> ! {
    if !message.is_empty() {
        eprintln!("edgelog-connect: {message}");
    }

    eprintln!(
        "usage: edgelog-connect [--server HOST:PORT] [--path hop[/hop...]] (--target NAME [--local-listen HOST:PORT] | --tunnels)"
    );
    std::process::exit(if message.is_empty() { 0 } else { 2 });
}

fn main() -> io::Result<()> {
    let args = Args::parse();

    if args.list_tunnels {
        return list_tunnels(&args);
    }

    if args.target.is_none() {
        usage_exit("--target or --tunnels is required");
    }

    if let Some(addr) = &args.local_listen {
        serve_local_forward(&args, addr)
    } else {
        connect_stdio(&args)
    }
}

fn list_tunnels(args: &Args) -> io::Result<()> {
    let command = routed_command(&args.path, "TUNNELS".to_string());
    let mut stream = TcpStream::connect(&args.server)?;
    writeln!(stream, "{command}")?;
    stream.flush()?;

    let mut reader = BufReader::new(stream);
    let mut first = true;
    let mut line = String::new();

    loop {
        line.clear();

        if reader.read_line(&mut line)? == 0 {
            break;
        }

        let line = line.trim_end_matches(['\r', '\n']);

        if first {
            first = false;

            if let Some(error) = line.strip_prefix("ERR ") {
                eprintln!("edgelog-connect: {error}");
                std::process::exit(1);
            }

            if line == "OK" || line.starts_with("OK ") {
                continue;
            }
        }

        if line == "END" {
            break;
        }

        println!("{line}");
    }

    Ok(())
}

fn serve_local_forward(args: &Args, addr: &str) -> io::Result<()> {
    let listener = TcpListener::bind(addr)?;
    eprintln!("edgelog-connect: listening on {addr}");

    for local in listener.incoming() {
        match local {
            Ok(local) => {
                let args = args.clone();
                thread::spawn(move || {
                    if let Err(error) = handle_local_client(local, &args) {
                        eprintln!("edgelog-connect: local client error: {error}");
                    }
                });
            }
            Err(error) => eprintln!("edgelog-connect: accept error: {error}"),
        }
    }

    Ok(())
}

fn handle_local_client(mut local: TcpStream, args: &Args) -> io::Result<()> {
    let remote = connect_tunnel(args)?;
    let _ = local.set_nodelay(true);
    bridge_tcp_streams(&mut local, remote)
}

fn connect_stdio(args: &Args) -> io::Result<()> {
    let mut remote = connect_tunnel(args)?;
    let mut remote_writer = remote.try_clone()?;

    let _stdin_to_remote = thread::spawn(move || {
        let stdin = io::stdin();
        let mut stdin = stdin.lock();
        let result = io::copy(&mut stdin, &mut remote_writer);
        let _ = remote_writer.shutdown(Shutdown::Write);
        result
    });

    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    io::copy(&mut remote, &mut stdout)?;
    Ok(())
}

fn connect_tunnel(args: &Args) -> io::Result<TcpStream> {
    let target = args
        .target
        .as_deref()
        .unwrap_or_else(|| usage_exit("--target is required"));
    let command = routed_command(&args.path, format!("CONNECT {target}"));
    let mut stream = TcpStream::connect(&args.server)?;
    stream.set_nodelay(true)?;
    writeln!(stream, "{command}")?;
    stream.flush()?;

    let response = read_control_line(&mut stream)?;

    if let Some(error) = response.strip_prefix("ERR ") {
        return Err(io::Error::other(error.to_string()));
    }

    if response != "OK" && !response.starts_with("OK ") {
        return Err(io::Error::other(format!(
            "unexpected control response: {response}"
        )));
    }

    Ok(stream)
}

fn read_control_line(stream: &mut TcpStream) -> io::Result<String> {
    let mut bytes = Vec::new();
    let mut byte = [0_u8; 1];

    loop {
        let read = stream.read(&mut byte)?;

        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "control connection closed before response",
            ));
        }

        if byte[0] == b'\n' {
            break;
        }

        if byte[0] != b'\r' {
            bytes.push(byte[0]);
        }
    }

    Ok(String::from_utf8_lossy(&bytes).to_string())
}

fn routed_command(path: &[String], mut command: String) -> String {
    for hop in path.iter().rev() {
        command = format!("ROUTE {hop} {command}");
    }

    command
}

fn bridge_tcp_streams(left: &mut TcpStream, right: TcpStream) -> io::Result<()> {
    let mut left_reader = left.try_clone()?;
    let mut left_writer = left.try_clone()?;
    let mut right_reader = right.try_clone()?;
    let mut right_writer = right;

    let left_to_right = thread::spawn(move || {
        let result = io::copy(&mut left_reader, &mut right_writer);
        let _ = right_writer.shutdown(Shutdown::Write);
        result
    });
    let right_to_left = thread::spawn(move || {
        let result = io::copy(&mut right_reader, &mut left_writer);
        let _ = left_writer.shutdown(Shutdown::Write);
        result
    });

    join_bridge_copy(left_to_right)?;
    join_bridge_copy(right_to_left)?;
    Ok(())
}

fn join_bridge_copy(handle: thread::JoinHandle<io::Result<u64>>) -> io::Result<u64> {
    handle
        .join()
        .unwrap_or_else(|_| Err(io::Error::other("tunnel copy thread panicked")))
}
