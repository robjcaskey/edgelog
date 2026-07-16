use std::env;
use std::io::{self, BufRead, BufReader, Write};
use std::net::TcpStream;

#[derive(Debug)]
struct Args {
    server: String,
    path: Vec<String>,
    ring: Option<String>,
    lines: usize,
    follow: bool,
    list_rings: bool,
    list_peers: bool,
}

impl Args {
    fn parse() -> Self {
        let mut args = env::args().skip(1);
        let mut parsed = Self {
            server: "127.0.0.1:7777".to_string(),
            path: Vec::new(),
            ring: None,
            lines: 100,
            follow: true,
            list_rings: false,
            list_peers: false,
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
                "--ring" => {
                    parsed.ring = Some(
                        args.next()
                            .unwrap_or_else(|| usage_exit("--ring requires a name")),
                    );
                }
                "--lines" => {
                    parsed.lines = args
                        .next()
                        .and_then(|value| value.parse().ok())
                        .unwrap_or_else(|| usage_exit("--lines requires a number"));
                }
                "--follow" => parsed.follow = true,
                "--no-follow" => parsed.follow = false,
                "--rings" => parsed.list_rings = true,
                "--peers" => parsed.list_peers = true,
                "--help" | "-h" => usage_exit(""),
                other => usage_exit(&format!("unknown argument: {other}")),
            }
        }

        parsed
    }
}

fn usage_exit(message: &str) -> ! {
    if !message.is_empty() {
        eprintln!("edgelog-tail: {message}");
    }

    eprintln!(
        "usage: edgelog-tail [--server HOST:PORT] [--path hop[/hop...]] (--ring NAME | --rings | --peers) [--lines N] [--no-follow]"
    );
    std::process::exit(if message.is_empty() { 0 } else { 2 });
}

fn main() -> io::Result<()> {
    let args = Args::parse();
    let command = build_command(&args);
    let mut stream = TcpStream::connect(&args.server)?;

    stream.write_all(command.as_bytes())?;
    stream.write_all(b"\n")?;
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
                eprintln!("edgelog-tail: {error}");
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

fn build_command(args: &Args) -> String {
    let mut command = if args.list_peers {
        "PEERS".to_string()
    } else if args.list_rings {
        "RINGS".to_string()
    } else if let Some(ring) = &args.ring {
        if args.follow {
            format!("FOLLOW {ring} {}", args.lines)
        } else {
            format!("TAIL {ring} {}", args.lines)
        }
    } else {
        usage_exit("--ring, --rings, or --peers is required");
    };

    for hop in args.path.iter().rev() {
        command = format!("ROUTE {hop} {command}");
    }

    command
}
