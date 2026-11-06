# Crash Torture Tool (Person 3 Deliverable)

A standalone crash-testing harness designed to subject a database engine to aggressive, randomized `SIGKILL` termination during active write operations, verify durability and atomicity invariants through an independent oracle, and generate verifiable failure bundles.

---

## 1. Project Overview & Boundaries

* **Role**: Person 3 (Torture Tool & Evidence Owner).
* **Boundaries**:
  * Does **NOT** implement database storage, indexing, recovery, or compaction.
  * Completely independent repository/folder that connects to the engine as an external child process via standard I/O pipes.
  * Decoupled adapter layer allows zero-assumption integration once Person 1 & 2 finalize the engine CLI and serve protocol.
  * Invariant verification is conducted by a client-side oracle tracking explicit ACK responses; the oracle is **never** fed by the database.

---

## 2. Prerequisites & Building

* **Toolchain**: Rust 2024 edition (`rustc` and `cargo` >= 1.85).

### Build Command
```bash
cargo build --release
```

### Running the Tool
Direct via Cargo:
```bash
cargo run --release -- --iterations 500 --seed 12345 --mode default
```

Via the POSIX launcher (required by the problem statement):
```bash
./torture --iterations 500 --seed 12345 --mode default
```

Via the Windows launcher:
```cmd
torture.bat --iterations 500 --seed 12345 --mode default
```

---

## 3. Command Line Interface

| Option | Default | Description |
| :--- | :--- | :--- |
| `--iterations <N>` | `500` | Total number of crash-kill iterations |
| `--seed <S>` | `12345` | PRNG seed for deterministic test generation |
| `--mode <M>` | `default` | Execution mode: `default`, `large-values`, `large-batches`, `recovery-crash`, `compaction`, `disk-full` |
| `--keep-going` | `false` | Continue after a failure (for overnight test campaigns) |
| `--engine-bin <P>`| `./db` | Path to engine executable |
| `--db-path <P>` | `./torture_test.db` | Target test database file / directory |

---

## 4. Execution Modes

* **`default`**: Mixed puts, deletes, and small atomic batches across 2+ concurrent writers; short random kill windows (5–300 ms).
* **`large-values`**: Tests 1 MB binary values to catch torn writes, boundary overflow, and fsync ordering defects.
* **`large-batches`**: Sends 10,000-op atomic batches; verifies crash atomicity and recovery restart under 2 seconds.
* **`recovery-crash`**: Kills the database *during* the `open` recovery phase, repeatedly, ensuring idempotency and crash-safe log replay.
* **`compaction`**: Triggers compaction on dirty workloads and kills during swap/cleanup phases.
* **`disk-full`**: Runs with small `--max-bytes` to verify clean `ERR` handling without corruption.

---

## 5. Project Layout

```text
├── Cargo.toml            # Package configuration
├── README.md             # Project documentation and protocol spec
├── PROMPTS.md            # Unedited log of all AI prompts used in development
├── torture / torture.bat # Official launcher scripts
├── src/
│   ├── main.rs           # CLI entry point and validation
│   ├── config.rs         # Execution modes, timeouts, and options
│   ├── adapter/          # Child process manager & protocol abstraction (EngineProcess trait)
│   ├── generator/        # Deterministic self-describing payload generator
│   ├── runner/           # Concurrency, random millisecond killer, and pipe drainer
│   ├── oracle/           # Independent ACK state lattice & 5-rule invariant checker
│   ├── evidence/         # Failure bundle packaging and summary reports
│   └── mocks/            # Compliant and broken mock engines (for self-tests ONLY)
├── tools/                # Auxiliary tools (e.g. byte-by-byte torn tail sweeper)
├── seeds/
│   └── regressions.txt   # Tracker for discovered failure seeds
└── results/              # Output bundles and test summaries
```

