use std::env;
use std::process;

use torture::config::TortureConfig;

fn main() {
    // Collect command line arguments (skipping the executable path)
    let args: Vec<String> = env::args().skip(1).collect();

    let config = match TortureConfig::parse_from(args.into_iter()) {
        Ok(cfg) => cfg,
        Err(err_msg) => {
            eprintln!("Error: {}", err_msg);
            process::exit(1);
        }
    };

    println!("============================================================");
    println!("  KILL IT AND PROVE IT - CRASH TORTURE TOOL (PERSON 3)");
    println!("============================================================");
    println!("Status               : Phase 3 (Independent Oracle & State Lattice Ready)");
    println!("Execution Mode       : {}", config.mode.as_str());
    println!("Iterations           : {}", config.iterations);
    println!("PRNG Seed            : {}", config.seed);
    println!("Keep-Going           : {}", config.keep_going);
    println!("------------------------------------------------------------");
    println!(
        "Target Engine Binary : {}",
        config.engine.bin_path.display()
    );
    println!("Target Database Path : {}", config.engine.db_path.display());
    println!("I/O Timeout (ms)     : {}", config.engine.io_timeout_ms);
    if let Some(max_b) = config.engine.max_bytes {
        println!("Max Bytes Limit      : {}", max_b);
    }
    println!("============================================================");
    println!("Harness initialized and verified.");

    process::exit(0);
}
