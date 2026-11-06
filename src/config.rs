use std::path::PathBuf;

/// Torture tool execution modes as defined in the Person 3 specification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TortureMode {
    /// Default mode: mixed operations, 2+ writers, batches, random SIGKILL.
    Default,
    /// 1 MB values to test torn writes and large record handling.
    LargeValues,
    /// 10,000-op batches to test transaction atomicity under crash.
    LargeBatches,
    /// Repeated kills during open/recovery loop.
    RecoveryCrash,
    /// Trigger compaction and kill during swap/cleanup phases.
    Compaction,
    /// Low disk threshold (--max-bytes) to verify clean error on out-of-space.
    DiskFull,
}

impl TortureMode {
    pub fn from_str_name(s: &str) -> Result<Self, String> {
        match s.to_ascii_lowercase().as_str() {
            "default" => Ok(TortureMode::Default),
            "large-values" => Ok(TortureMode::LargeValues),
            "large-batches" => Ok(TortureMode::LargeBatches),
            "recovery-crash" => Ok(TortureMode::RecoveryCrash),
            "compaction" => Ok(TortureMode::Compaction),
            "disk-full" => Ok(TortureMode::DiskFull),
            unknown => Err(format!(
                "Unknown mode '{}'. Allowed: default, large-values, large-batches, recovery-crash, compaction, disk-full",
                unknown
            )),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            TortureMode::Default => "default",
            TortureMode::LargeValues => "large-values",
            TortureMode::LargeBatches => "large-batches",
            TortureMode::RecoveryCrash => "recovery-crash",
            TortureMode::Compaction => "compaction",
            TortureMode::DiskFull => "disk-full",
        }
    }
}

/// Placeholder settings for connecting to the database engine subprocess.
/// Teammates (Persons 1 and 2) will provide the exact paths and CLI conventions.
#[derive(Debug, Clone)]
pub struct EngineSettings {
    /// Path to the engine binary (e.g. "./db" or target/debug/db)
    pub bin_path: PathBuf,
    /// Path to the database file / directory to test
    pub db_path: PathBuf,
    /// Optional limit in bytes for --mode disk-full
    pub max_bytes: Option<u64>,
    /// Child process read/write timeout in milliseconds
    pub io_timeout_ms: u64,
}

impl Default for EngineSettings {
    fn default() -> Self {
        Self {
            bin_path: PathBuf::from("./db"),
            db_path: PathBuf::from("./torture_test.db"),
            max_bytes: None,
            io_timeout_ms: 2000,
        }
    }
}

/// Top-level configuration for the torture testing tool.
#[derive(Debug, Clone)]
pub struct TortureConfig {
    /// Total number of crash iterations to run (default: 500)
    pub iterations: usize,
    /// Master PRNG seed for deterministic test generation
    pub seed: u64,
    /// Execution mode
    pub mode: TortureMode,
    /// If true, continue testing on violation rather than halting immediately
    pub keep_going: bool,
    /// Engine child process configuration
    pub engine: EngineSettings,
}

impl Default for TortureConfig {
    fn default() -> Self {
        Self {
            iterations: 500,
            seed: 12345,
            mode: TortureMode::Default,
            keep_going: false,
            engine: EngineSettings::default(),
        }
    }
}

impl TortureConfig {
    /// Parses CLI arguments using only the standard library.
    pub fn parse_from<I>(mut args: I) -> Result<Self, String>
    where
        I: Iterator<Item = String>,
    {
        let mut config = TortureConfig::default();

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--iterations" | "-n" => {
                    let val = args
                        .next()
                        .ok_or_else(|| "Missing value for --iterations".to_string())?;
                    config.iterations = val
                        .parse::<usize>()
                        .map_err(|e| format!("Invalid integer for --iterations: {}", e))?;
                }
                "--seed" | "-s" => {
                    let val = args
                        .next()
                        .ok_or_else(|| "Missing value for --seed".to_string())?;
                    config.seed = val
                        .parse::<u64>()
                        .map_err(|e| format!("Invalid integer for --seed: {}", e))?;
                }
                "--mode" | "-m" => {
                    let val = args
                        .next()
                        .ok_or_else(|| "Missing value for --mode".to_string())?;
                    config.mode = TortureMode::from_str_name(&val)?;
                }
                "--keep-going" => {
                    config.keep_going = true;
                }
                "--engine-bin" => {
                    let val = args
                        .next()
                        .ok_or_else(|| "Missing value for --engine-bin".to_string())?;
                    config.engine.bin_path = PathBuf::from(val);
                }
                "--db-path" => {
                    let val = args
                        .next()
                        .ok_or_else(|| "Missing value for --db-path".to_string())?;
                    config.engine.db_path = PathBuf::from(val);
                }
                "--help" | "-h" => {
                    return Err(Self::usage());
                }
                unknown => {
                    return Err(format!(
                        "Unrecognized argument: '{}'\n{}",
                        unknown,
                        Self::usage()
                    ));
                }
            }
        }

        Ok(config)
    }

    pub fn usage() -> String {
        format!(
            "Usage: torture [OPTIONS]\n\n\
             Options:\n  \
               --iterations <N>   Number of crash iterations (default: 500)\n  \
               --seed <S>         RNG seed for reproducibility (default: 12345)\n  \
               --mode <M>         Mode: default, large-values, large-batches, recovery-crash, compaction, disk-full\n  \
               --keep-going       Continue after failures for overnight testing\n  \
               --engine-bin <P>   Path to database binary (default: ./db)\n  \
               --db-path <P>      Path to test database file (default: ./torture_test.db)\n  \
               --help, -h         Show this help message"
        )
    }
}
