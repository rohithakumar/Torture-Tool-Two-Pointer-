//! Internal Mock Child Database Process for Harness Self-Testing.
//!
//! =========================================================================
//! DISCLAIMER:
//! This binary is STRICTLY an internal test helper designed to exercise
//! the process supervisor, concurrent writers, random killer, and pipe drainer.
//! It is NOT the teammates' database engine and does NOT verify database correctness.
//! =========================================================================
//!
//! Internal Test-Only Protocol:
//!   Subcommands:
//!     mock_child serve <db_path> [options]
//!     mock_child open <db_path>
//!     mock_child check <db_path>
//!     mock_child dump <db_path>
//!
//!   Serve Stdin Protocol:
//!     - `<id> put <key> <val>`  => replies `ACK <id>\n` (flushed)
//!     - `<id> del <key>`        => replies `ACK <id>\n` (flushed)
//!     - `<id> batch <count>`    => accepts <count> ops, then replies `ACK <id>\n` (flushed)
//!     - `TEST_PARTIAL <id>`     => writes `"ACK <id>"` without newline, flushes, then sleeps
//!     - `TEST_DELAY <id> <ms>`  => sleeps for <ms> before sending `ACK <id>\n`
//!     - `TEST_ERR <id> <reason>`=> replies `ERR <id> <reason>\n`
//!     - `TEST_HANG`             => hangs indefinitely without responding
//!     - `TEST_EXIT <code>`      => exits immediately with exit code

use std::collections::BTreeMap;
use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

fn get_state_file_path(db_path: &Path) -> PathBuf {
    if db_path.is_dir() {
        db_path.join("mock_kv.txt")
    } else {
        let mut p = db_path.to_path_buf();
        p.set_extension("mock_kv.txt");
        p
    }
}

fn load_state(path: &Path) -> BTreeMap<u64, String> {
    let mut map = BTreeMap::new();
    let file_path = get_state_file_path(path);
    if let Ok(file) = File::open(&file_path) {
        let reader = BufReader::new(file);
        for line_res in reader.lines() {
            if let Ok(line) = line_res {
                let parts: Vec<&str> = line.trim().split_whitespace().collect();
                if parts.len() >= 2 {
                    if let Ok(k) = parts[0].parse::<u64>() {
                        map.insert(k, parts[1].to_string());
                    }
                }
            }
        }
    }
    map
}

fn save_state(path: &Path, map: &BTreeMap<u64, String>) {
    let file_path = get_state_file_path(path);
    if let Some(parent) = file_path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    if let Ok(mut file) = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&file_path)
    {
        for (k, v) in map {
            let _ = writeln!(file, "{} {}", k, v);
        }
        let _ = file.flush();
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        run_serve_loop(None, 0, false, None, None);
        return;
    }

    let first = &args[1];

    match first.as_str() {
        "open" => {
            let db_path = if args.len() >= 3 {
                PathBuf::from(&args[2])
            } else {
                PathBuf::from("./mock_db")
            };
            let state = load_state(&db_path);
            println!("OPEN OK keys={} recovery_ms=1", state.len());
            std::process::exit(0);
        }
        "check" => {
            let db_path = if args.len() >= 3 {
                PathBuf::from(&args[2])
            } else {
                PathBuf::from("./mock_db")
            };
            // Check if state file can be parsed
            let file_path = get_state_file_path(&db_path);
            if file_path.exists() && fs::read_to_string(&file_path).is_err() {
                eprintln!("CORRUPTED_FILE");
                std::process::exit(1);
            }
            println!("OK");
            std::process::exit(0);
        }
        "dump" => {
            let db_path = if args.len() >= 3 {
                PathBuf::from(&args[2])
            } else {
                PathBuf::from("./mock_db")
            };

            let mut fault = None;
            let mut i = 3;
            while i < args.len() {
                if args[i] == "--fault" && i + 1 < args.len() {
                    fault = Some(args[i + 1].clone());
                    i += 1;
                }
                i += 1;
            }

            let mut state = load_state(&db_path);

            // Apply simulated faults to dump if configured
            if let Some(f) = fault {
                if f.starts_with("lost_ack:") {
                    let key = f[9..].parse::<u64>().unwrap_or(0);
                    state.remove(&key);
                } else if f.starts_with("resurrected_del:") {
                    let parts: Vec<&str> = f[16..].split(':').collect();
                    if parts.len() >= 2 {
                        let key = parts[0].parse::<u64>().unwrap_or(0);
                        state.insert(key, parts[1].to_string());
                    }
                } else if f.starts_with("corrupted_val:") {
                    let key = f[14..].parse::<u64>().unwrap_or(0);
                    if let Some(hex_val) = state.get_mut(&key) {
                        // Invert hex characters to break CRC32
                        if hex_val.len() >= 2 {
                            let mut chars: Vec<char> = hex_val.chars().collect();
                            chars[0] = if chars[0] == '0' { 'f' } else { '0' };
                            *hex_val = chars.into_iter().collect();
                        }
                    }
                } else if f.starts_with("partial_batch:") {
                    let parts: Vec<&str> = f[14..].split(':').collect();
                    if parts.len() >= 2 {
                        let key = parts[1].parse::<u64>().unwrap_or(0);
                        state.remove(&key);
                    }
                } else if f.starts_with("foreign_val:") {
                    let key = f[12..].parse::<u64>().unwrap_or(0);
                    state.insert(key, "deadbeefcafebabe".to_string());
                }
            }

            for (k, v) in state {
                println!("{} {}", k, v);
            }
            std::process::exit(0);
        }
        "serve" => {
            let db_path = if args.len() >= 3 {
                Some(PathBuf::from(&args[2]))
            } else {
                None
            };
            let mut default_delay_ms = 0u64;
            let mut stderr_noise = false;
            let mut partial_on_op = None;
            let mut fault = None;

            let mut i = 3;
            while i < args.len() {
                match args[i].as_str() {
                    "--delay-ms" => {
                        if i + 1 < args.len() {
                            default_delay_ms = args[i + 1].parse().unwrap_or(0);
                            i += 1;
                        }
                    }
                    "--stderr-noise" => {
                        stderr_noise = true;
                    }
                    "--partial-on-op" => {
                        if i + 1 < args.len() {
                            partial_on_op = args[i + 1].parse::<u64>().ok();
                            i += 1;
                        }
                    }
                    "--fault" => {
                        if i + 1 < args.len() {
                            fault = Some(args[i + 1].clone());
                            i += 1;
                        }
                    }
                    _ => {}
                }
                i += 1;
            }

            run_serve_loop(
                db_path,
                default_delay_ms,
                stderr_noise,
                partial_on_op,
                fault,
            );
        }
        _ => {
            // Legacy / stdin invocation without subcommands
            let mut default_delay_ms = 0u64;
            let mut stderr_noise = false;
            let mut partial_on_op = None;

            let mut i = 1;
            while i < args.len() {
                match args[i].as_str() {
                    "--delay-ms" => {
                        if i + 1 < args.len() {
                            default_delay_ms = args[i + 1].parse().unwrap_or(0);
                            i += 1;
                        }
                    }
                    "--stderr-noise" => {
                        stderr_noise = true;
                    }
                    "--partial-on-op" => {
                        if i + 1 < args.len() {
                            partial_on_op = args[i + 1].parse::<u64>().ok();
                            i += 1;
                        }
                    }
                    _ => {}
                }
                i += 1;
            }

            run_serve_loop(None, default_delay_ms, stderr_noise, partial_on_op, None);
        }
    }
}

fn run_serve_loop(
    db_path: Option<PathBuf>,
    default_delay_ms: u64,
    stderr_noise: bool,
    partial_on_op: Option<u64>,
    _fault: Option<String>,
) {
    if stderr_noise {
        eprintln!(
            "[mock_child] Started internal mock process PID: {}",
            std::process::id()
        );
    }

    let mut state = if let Some(ref path) = db_path {
        load_state(path)
    } else {
        BTreeMap::new()
    };

    let stdin = io::stdin();
    let mut stdout = io::stdout();
    let mut lines = stdin.lock().lines();

    while let Some(line_res) = lines.next() {
        let line = match line_res {
            Ok(l) => l,
            Err(_) => break, // EOF on stdin
        };

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        // Test-only commands
        if trimmed.starts_with("TEST_EXIT ") {
            let code = trimmed[10..].trim().parse().unwrap_or(0);
            std::process::exit(code);
        }

        if trimmed == "TEST_HANG" {
            loop {
                thread::sleep(Duration::from_secs(60));
            }
        }

        if trimmed.starts_with("TEST_PARTIAL ") {
            let id = trimmed[13..].trim();
            // Emit "ACK <id>" WITHOUT trailing newline, then hang so kill catches partial line
            let _ = write!(stdout, "ACK {}", id);
            let _ = stdout.flush();
            loop {
                thread::sleep(Duration::from_secs(60));
            }
        }

        if trimmed.starts_with("TEST_DELAY ") {
            let parts: Vec<&str> = trimmed.split_whitespace().collect();
            if parts.len() >= 3 {
                let id = parts[1];
                let ms: u64 = parts[2].parse().unwrap_or(50);
                thread::sleep(Duration::from_millis(ms));
                let _ = writeln!(stdout, "ACK {}", id);
                let _ = stdout.flush();
            }
            continue;
        }

        if trimmed.starts_with("TEST_ERR ") {
            let parts: Vec<&str> = trimmed.split_whitespace().collect();
            if parts.len() >= 3 {
                let id = parts[1];
                let reason = parts[2..].join(" ");
                let _ = writeln!(stdout, "ERR {} {}", id, reason);
                let _ = stdout.flush();
            }
            continue;
        }

        // Normal protocol operations
        let parts: Vec<&str> = trimmed.split_whitespace().collect();
        if parts.is_empty() {
            continue;
        }

        let op_id: u64 = match parts[0].parse() {
            Ok(id) => id,
            Err(_) => continue,
        };

        // If configured to emit a partial line on this specific op
        if Some(op_id) == partial_on_op {
            let _ = write!(stdout, "ACK {}", op_id);
            let _ = stdout.flush();
            loop {
                thread::sleep(Duration::from_secs(60));
            }
        }

        // Check for batch command: "<id> batch <count>"
        if parts.len() >= 3 && parts[1] == "batch" {
            let count: usize = parts[2].parse().unwrap_or(0);
            for _ in 0..count {
                if let Some(Ok(batch_member_line)) = lines.next() {
                    let b_parts: Vec<&str> = batch_member_line.split_whitespace().collect();
                    if b_parts.len() >= 2 {
                        let op_type = b_parts[0];
                        if let Ok(k) = b_parts[1].parse::<u64>() {
                            if op_type == "put" && b_parts.len() >= 3 {
                                state.insert(k, b_parts[2].to_string());
                            } else if op_type == "del" {
                                state.remove(&k);
                            }
                        }
                    }
                }
            }
            if let Some(ref path) = db_path {
                save_state(path, &state);
            }
            let _ = writeln!(stdout, "ACK {}", op_id);
            let _ = stdout.flush();
            continue;
        }

        // Single put: "<id> put <key> <hexval>"
        if parts.len() >= 4 && parts[1] == "put" {
            if let Ok(key) = parts[2].parse::<u64>() {
                state.insert(key, parts[3].to_string());
                if let Some(ref path) = db_path {
                    save_state(path, &state);
                }
            }
        }

        // Single del: "<id> del <key>"
        if parts.len() >= 3 && parts[1] == "del" {
            if let Ok(key) = parts[2].parse::<u64>() {
                state.remove(&key);
                if let Some(ref path) = db_path {
                    save_state(path, &state);
                }
            }
        }

        if default_delay_ms > 0 {
            thread::sleep(Duration::from_millis(default_delay_ms));
        }

        // Emit ACK and flush immediately
        let _ = writeln!(stdout, "ACK {}", op_id);
        let _ = stdout.flush();
    }
}
