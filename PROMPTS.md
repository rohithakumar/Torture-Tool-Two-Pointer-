# Prompt Log (PROMPTS.md)

All prompts used with AI coding assistants are recorded chronologically in this file as mandated by the Person 3 deliverable specification.

---

### Prompt 1
- **Timestamp**: 2026-11-06T14:04:09+05:30
- **Tool**: Antigravity (Gemini 3.8 Flash)
- **Prompt**:
> I am Person 3 in a team building a crash-safe database.
> 
> IMPORTANT: I am developing my work independently in a separate folder/repository. Other teammates are responsible for the database engine. I ONLY own the torture-testing tool.
> 
> Read the attached Person 3 plan completely.
> 
> Your task is to help me build an independent torture tool that can later connect to my teammates' database engine.
> 
> Strict boundaries:
> - Do NOT implement a database.
> - Do NOT modify or recreate my teammates' database engine.
> - Do NOT implement storage, recovery, compaction or database internals.
> - Do NOT assume the engine protocol without confirmation.
> - Keep my torture tool modular so I can connect it to the real engine later.
> 
> My deliverable is responsible for:
> 1. Starting the database as a child process.
> 2. Sending operations through its serve interface.
> 3. Running at least two concurrent writers.
> 4. Maintaining an independent oracle from sent operations and received ACK/ERR responses.
> 5. Randomly killing the database with SIGKILL during active operations.
> 6. Restarting the database.
> 7. Running open, check and dump.
> 8. Comparing the recovered database state against the oracle.
> 9. Detecting lost ACKed writes, resurrected deletes, partial batches and corrupted values.
> 10. Producing test logs, failure bundles and evidence.
> 
> First inspect my environment and propose a standalone project structure for ONLY the torture tool.
> 
> Design an engine adapter/interface so that I can later configure the actual serve command and protocol supplied by my teammates.
> 
> Use mock engines only for initial development. Clearly distinguish mock testing from real database verification.
> 
> Do not start coding yet. First give me the architecture, folder structure, implementation phases and the exact information I must obtain from my teammates.

---

### Prompt 2
- **Timestamp**: 2026-11-06T14:10:24+05:30
- **Tool**: Antigravity (Gemini 3.8 Flash)
- **Prompt**:
> Proceed with Phase 0 only.
> 
> Create the standalone Rust torture-tool project in the current empty workspace.
> 
> Requirements:
> 
> 1. Initialize a Cargo project.
> 2. Create the modular folder structure from the approved architecture.
> 3. Add the required dependencies only if genuinely needed. Prefer Rust standard-library functionality.
> 4. Implement a basic CLI accepting:
>    --iterations
>    --seed
>    --mode
> 5. Add a README explaining the project and how to build it.
> 6. Initialize PROMPTS.md and seeds/regressions.txt.
> 7. Add a configuration module with placeholder engine settings.
> 8. Add an EngineProcess trait/interface, but do not implement real database communication yet.
> 9. Add a simple executable that prints parsed configuration and exits successfully.
> 
> Do not implement the oracle, writers, random killer, mock database, or recovery tests yet.
> 
> Do not invent the database protocol.
> 
> Run `cargo build` and `cargo run` with sample arguments. Fix all compilation errors.
> 
> Show me the final folder structure, exact commands executed, and actual output.
> 
> Do not claim success if a command was not executed.


---

### Prompt 3
- **Timestamp**: 2026-11-06T14:16:34+05:30
- **Tool**: Antigravity (Gemini 3.8 Flash)
- **Prompt**:
> # Phase 1: Deterministic Generator, PRNG, Mock Engines & Mutation Self-Tests
> 
> Phase 0 is complete and verified. Continue working in the existing Rust project. Do not recreate or overwrite the Phase 0 structure.
> 
> Implement **Phase 1 only**, following the approved Person 3 plan.
> 
> ## 1. Deterministic PRNG streams
> - Implement a reproducible seeded PRNG.
> - The same seed and configuration must produce exactly the same generated sequence.
> - Support independent PRNG streams for different writers so adding activity to one writer does not unpredictably change another writer's sequence.
> - Avoid relying on system randomness during deterministic test generation.
> - Include tests for reproducibility and stream independence.
> 
> ## 2. Self-describing payload generator
> Generate deterministic payloads containing enough information to identify and validate them later.
> 
> Each payload should encode:
> - Run seed
> - Writer ID
> - Operation sequence number
> - Key or record identifier
> - Payload length
> - Deterministic content
> - Integrity checksum or equivalent validation data
> 
> Requirements:
> - Support configurable payload sizes.
> - Ensure generated payloads can be independently validated.
> - Include tests for payload generation, size, uniqueness, and integrity validation.
> - Do not use timestamps or other nondeterministic values in payload identity.
> 
> ## 3. Mock engines
> Create mock database processes exclusively for testing the torture tool.
> 
> Implement:
> - A compliant mock that behaves according to the expected protocol contract.
> - Deliberately broken mocks that simulate important failure conditions, such as lost acknowledged writes, partial batch application, corrupted values, and invalid error handling.
> 
> Important:
> - Keep mock implementations separate from the real engine adapter.
> - Do not invent or assume my friends' database protocol.
> - Do not implement database storage, recovery, compaction, or persistence internals.
> - These mocks must never be presented as verification of the actual database.
> 
> ## 4. Mutation self-tests
> Create tests that demonstrate the test infrastructure can distinguish compliant behavior from deliberately broken mock behavior.
> 
> For each broken mock:
> - Explain which failure it simulates.
> - Define the expected detection outcome.
> - Run the test and report its actual result.
> - Keep the test results reproducible.
> 
> Do not claim that a failure was detected unless an executed test proves it.
> 
> ## 5. Code quality
> - Follow the existing project structure and naming conventions.
> - Keep modules small and understandable.
> - Prefer Rust standard-library functionality unless a dependency is genuinely necessary.
> - Add useful comments where the logic is not obvious.
> - Handle invalid inputs safely.
> - Do not add unrelated features.
> 
> ## 6. Verification
> Run:
> - `cargo fmt --check`
> - `cargo test`
> - `cargo build`
> - Any additional targeted tests you create.
> 
> Fix all errors and warnings.
> 
> ## 7. Final report
> Show me:
> 1. Files created or modified.
> 2. Final folder structure.
> 3. PRNG design and how reproducibility is guaranteed.
> 4. Payload format and validation method.
> 5. Mock engines and the failures each simulates.
> 6. Mutation self-test results, including actual command output.
> 7. Exact commands executed.
> 8. Any incomplete work or known limitations.
> 
> **Strict boundaries:** Do not implement concurrent writers, the random killer, the real database adapter, the full oracle, restart/recovery testing, or official test modes in this phase. Those belong to later phases.
> 
> Do not fabricate command output or claim tests passed unless you actually executed them.

---

### Prompt 4
- **Timestamp**: 2026-11-06T14:26:49+05:30
- **Tool**: Antigravity (Gemini 3.8 Flash)
- **Prompt**:
> # Phase 2: Concurrent Writers, Random Killer & Process Drainer
> 
> Phase 1 is complete according to its reported test results. Continue in the existing Rust project. Do not recreate the project or overwrite completed work.
> 
> Implement Phase 2 only, following the approved Person 3 plan.
> 
> ## 0. Small Phase 1 documentation correction
> 
> Before implementing Phase 2:
> - Remove the unsupported absolute claim that independent PRNG streams are mathematically guaranteed to be non-overlapping.
> - Remove the claim that SplitMix64 passes the full BigCrush battery unless independently verified.
> - Describe stream independence accurately: each writer has a reproducible, separately initialized PRNG stream.
> - Do not change working Phase 1 functionality unnecessarily.
> 
> ## 1. Concurrent writers
> 
> Implement the writer system.
> 
> Requirements:
> - Support at least two concurrent writers.
> - Each writer must have its own deterministic PRNG stream.
> - Each writer must maintain its own monotonically increasing operation sequence.
> - No artificial think-time or sleep between normal operations.
> - Allow only one outstanding operation per writer.
> - Generate deterministic operations using the existing payload generator.
> - Support puts, deletes, and batches.
> - Include hot-key workloads and writer-owned key workloads.
> - Record every operation with its writer ID, sequence number, operation ID, key, and payload metadata.
> 
> Do not let one writer's PRNG consumption alter another writer's generated sequence.
> 
> ## 2. Process supervision
> 
> Implement a process supervisor abstraction that can:
> - Start a child process.
> - Write operations to its standard input.
> - Read its standard output and standard error without deadlocks.
> - Track process state and exit status.
> - Terminate the child process using a genuine hard kill.
> 
> Platform behavior:
> - On Unix, use SIGKILL.
> - On Windows, use the appropriate hard process termination mechanism.
> - Do not describe a graceful shutdown or ordinary child kill as SIGKILL.
> - Clearly report which termination mechanism was actually used.
> 
> ## 3. Random crash killer
> 
> Implement a deterministic random crash scheduler.
> 
> Requirements:
> - Use the existing seeded PRNG infrastructure.
> - Select kill delays from a configurable range.
> - Support very short delays, including sub-millisecond targets where the platform permits.
> - Do not claim timer precision the operating system cannot guarantee.
> - Record the scheduled delay and actual observed kill time.
> - Ensure kills can occur during active writes and batches, not exclusively between operations.
> - Record whether the process was writing, idle, or processing a batch when the kill was triggered, when observable.
> 
> ## 4. Output drainer and ACK accounting
> 
> Implement a dedicated output drainer.
> 
> Critical requirements:
> - Drain stdout continuously while the child is running.
> - Preserve complete protocol lines.
> - Count only complete ACK lines.
> - Do not count a partial final line as an ACK.
> - After killing the process, continue draining available output until the pipe reaches EOF.
> - Record complete ACKs, ERR responses, malformed lines, and incomplete trailing data separately.
> - Avoid deadlocks caused by waiting for process exit before draining output.
> 
> Do not assume that an operation was acknowledged just because it was sent.
> 
> ## 5. Internal mock child process
> 
> Create a separate executable or test helper that simulates a child database process for testing the process supervisor, concurrent writers, killer, and drainer.
> 
> Important:
> - This is an internal test harness, not my friends' database.
> - Define its test-only protocol explicitly and label it as such.
> - It must be able to simulate delayed responses, partial output lines, ACKs, ERR responses, and process termination during an operation.
> - Do not invent or implement the real database protocol.
> - Do not claim these tests verify the actual database.
> 
> ## 6. Integration tests
> 
> Add reproducible tests for:
> - Two or more concurrent writers.
> - Independent writer sequences.
> - Operations being sent while the child is alive.
> - Hard termination of the child.
> - Kill during active writes.
> - Kill during a batch.
> - Complete ACK accounting.
> - Partial final ACK line being ignored.
> - Output draining after termination.
> - Repeated runs with the same seed producing the same workload and kill schedule, subject to documented OS scheduling/timing differences.
> 
> Include at least one test where the child is deliberately terminated while output is being produced.
> 
> ## 7. Reporting and evidence
> 
> Record enough information for future crash-recovery verification:
> - Seed and iteration.
> - Writer IDs.
> - Operations sent.
> - Complete ACKs received.
> - ERR responses received.
> - Kill target and actual kill observation.
> - Process exit status.
> - Drainer statistics.
> - Any incomplete output.
> 
> Do not implement the full oracle or recovery comparison yet.
> 
> ## 8. Strict boundaries
> 
> Do NOT:
> - Implement my friends' database engine.
> - Invent the real database command-line interface or stdin/stdout protocol.
> - Implement the real database adapter.
> - Implement the full oracle or five-rule verification.
> - Implement database recovery, compaction, or persistence internals.
> - Claim mock results as real database verification.
> - Implement official judge modes in this phase.
> 
> ## 9. Verification
> 
> Run and report:
> - `cargo fmt --check`
> - `cargo check --all-targets`
> - `cargo test -- --nocapture`
> - `cargo build --release`
> - Targeted process-kill and drainer tests.
> 
> Fix all warnings and errors.
> 
> Test platform-specific behavior honestly. If only Windows is available, do not claim Unix SIGKILL behavior was tested.
> 
> ## 10. Final report
> 
> Show:
> 1. Files created and modified.
> 2. Final folder structure.
> 3. Writer architecture.
> 4. Process supervision and hard-kill implementation.
> 5. Random kill scheduling and timing limitations.
> 6. ACK drainer behavior, including partial-line handling.
> 7. Internal mock child protocol.
> 8. Integration test results and actual command output.

---

### Prompt 5
- **Timestamp**: 2026-11-06T14:46:15+05:30
- **Tool**: Antigravity (Gemini 3.8 Flash)
- **Prompt**:
> # Phase 3: Independent Oracle, State Lattice & Crash-Recovery Verification
> 
> Phase 2 is complete according to its reported results. Continue in the existing Rust project. Preserve all working Phase 0, 1, and 2 functionality.
> 
> Implement Phase 3 only, following the official Person 3 plan.
> 
> ## 0. First: inspect the existing implementation
> 
> Before coding:
> - Inspect the current PRNG, payload generator, mock engines, writers, supervisor, drainer, killer, and orchestrator.
> - Identify reusable components.
> - Identify missing interfaces and integration gaps.
> - Do not rewrite working modules unnecessarily.
> - Do not assume the Phase 2 report proves behavior that the source code does not implement.
> 
> ## 1. Independent oracle
> 
> Implement the central oracle.
> 
> The oracle must derive its expected state exclusively from:
> - Operations actually sent by the torture tool.
> - Complete ACK responses received.
> - Complete ERR responses received.
> - Operation ordering and concurrency information recorded by the harness.
> 
> Never derive expected state from the database's dump, check output, or recovery response.
> 
> Track:
> - Operation ID.
> - Writer ID.
> - Per-writer sequence number.
> - Operation type: PUT, DELETE, or BATCH.
> - Key and expected payload.
> - Dispatch event.
> - Complete ACK event.
> - Complete ERR event.
> - Whether an operation was in flight when the crash occurred.
> 
> Distinguish carefully between:
> - Sent operations.
> - Acknowledged operations.
> - Explicitly rejected operations.
> - In-flight operations whose outcome is uncertain.
> 
> ## 2. State lattice and concurrency semantics
> 
> Implement a state model that correctly handles uncertain crash outcomes.
> 
> Required rules:
> 
> 1. Every acknowledged operation must have its effect preserved unless a later valid operation supersedes it.
> 2. An in-flight operation may or may not have been applied.
> 3. An operation that received a complete ERR response must have no effect.
> 4. A batch must be atomic: all operations in the batch are visible or none are visible.
> 5. Values must not be torn, corrupted, malformed, or foreign to the current test run.
> 
> Concurrency requirements:
> - Preserve each writer's operation order.
> - Handle overlapping writes to shared hot keys without assuming an arbitrary winner.
> - Use observed dispatch and ACK ordering to establish happens-before relationships.
> - If two operations overlap and either outcome is legally possible, the oracle must accept either legal result.
> - Do not weaken the rules merely to make tests pass.
> 
> The oracle must compare the recovered state against all legally possible outcomes and reject states that match none of them.
> 
> Avoid a simplistic last-write-wins implementation that ignores concurrency.
> 
> ## 3. Payload verification
> 
> Reuse the existing self-describing payload generator.
> 
> For every recovered value:
> - Validate magic and format version.
> - Validate declared length.
> - Validate checksum.
> - Validate seed and key binding.
> - Validate writer ID and operation metadata where applicable.
> - Validate deterministic body content.
> 
> Detect:
> - Torn values.
> - Corrupted values.
> - Values belonging to another run.
> - Values stored under the wrong key.
> - Unexpected or foreign records.
> 
> ## 4. Recovery lifecycle
> 
> Implement the recovery verification workflow:
> 
> 1. Start the engine using the configured adapter.
> 2. Execute generated operations through the Phase 2 writer infrastructure.
> 3. Trigger the random hard kill.
> 4. Finish draining stdout and stderr.
> 5. Freeze the operation and ACK ledger.
> 6. Restart the engine using the same database directory.
> 7. Execute the adapter's OPEN operation.
> 8. Execute CHECK.
> 9. Obtain a normalized DUMP.
> 10. Validate the recovered dump against the independent oracle.
> 11. Save the complete iteration result.
> 
> Important:
> - Do not invent the actual database CLI or protocol.
> - Define a clean adapter interface for start, open, check, and dump.
> - Keep the actual database adapter configurable and unimplemented until my friends provide the real interface.
> - Implement a test-only adapter for your internal mock child.
> - Clearly distinguish test-only recovery results from real database verification.
> 
> ## 5. Persistent mock recovery testing
> 
> The existing mock infrastructure must support meaningful restart testing.
> 
> Implement a minimal test-only mock that:
> - Uses a temporary database directory.
> - Persists its simulated state between separate child-process launches.
> - Supports the test-only adapter interface.
> - Can simulate compliant recovery and deliberately broken recovery.
> - Can produce normalized dumps.
> - Can simulate acknowledged writes being lost, deletes being resurrected, partial batches, and corrupted values.
> 
> Do not build a production database engine.
> Do not implement real database storage or compaction internals.
> Keep this mock strictly as a test fixture.
> 
> ## 6. Oracle mutation tests
> 
> Add tests proving that the oracle:
> - Accepts a compliant recovered state.
> - Rejects a lost acknowledged write.
> - Accepts either legal outcome of an in-flight operation.
> - Rejects an explicitly rejected operation appearing in recovered state.
> - Rejects a partially applied batch.
> - Rejects a resurrected acknowledged delete.
> - Rejects torn or corrupted payloads.
> - Rejects foreign-run values.
> - Handles overlapping hot-key writes correctly.
> - Preserves per-writer ordering.
> 
> Include tests for both valid ambiguous states and genuinely invalid states. Do not only test failures.
> 
> ## 7. Evidence
> 
> Produce a structured result for each iteration containing:
> - Seed and iteration number.
> - Operations sent.
> - Complete ACKs and ERRs.
> - In-flight operations at crash time.
> - Kill observation.
> - Restart result.
> - OPEN and CHECK results.
> - Recovered dump summary.
> - Oracle verdict.
> - Exact invariant violations, if any.
> 
> A failed iteration must preserve enough information to reproduce and investigate it.
> 
> Do not let a database dump modify the oracle's expected state.
> 
> ## 8. Strict boundaries
> 
> Do NOT:
> - Implement my friends' database engine.
> - Guess or invent its protocol.
> - Claim internal mock testing verifies the actual database.
> - Implement official judge modes yet.
> - Implement compaction-specific scenarios yet.
> - Implement documentation polishing or final judge packaging.
> - Remove or weaken existing Phase 1 or Phase 2 tests.
> 
> ## 9. Verification
> 
> Run:
> - `cargo fmt --check`
> - `cargo check --all-targets`
> - `cargo test -- --nocapture`
> - `cargo build --release`
> - Targeted oracle, concurrency, batch atomicity, and recovery tests.
> 
> Fix all warnings and errors.
> 
> Run the same seed repeatedly and report whether the generated workload and oracle outcome are reproducible. Clearly distinguish deterministic workload generation from nondeterministic OS scheduling.
> 
> ## 10. Final report
> 
> Show:
> 1. Files created and modified.
> 2. Final folder structure.
> 3. Oracle architecture and state-lattice design.
> 4. How happens-before relationships are represented.
> 5. How ambiguous in-flight operations are handled.
> 6. How batch atomicity is verified.
> 7. Recovery lifecycle and adapter interface.
> 8. Persistent mock design.
> 9. Oracle mutation-test results.
> 10. Exact commands executed and actual output.
> 11. Known limitations and unfinished work.

---

### Prompt 7
- **Timestamp**: 2026-11-06T15:39:13+05:30
- **Tool**: Antigravity (Gemini 3.8 Flash)
- **Prompt**:
> # PHASE 3.1: FINAL HARDENING AND IMPLEMENTATION
> 
> We have completed Phases 0, 1, 2 and 3. The architecture review has identified several correctness vulnerabilities in the oracle and recovery implementation.
> 
> **Your task is to IMPLEMENT the Phase 3.1 fixes now. Do not produce another architecture plan. Do not stop after describing the approach.**
> 
> This is a strict implementation task.
> 
> ## 1. Scope and restrictions
> - Working ONLY on standalone torture tool.
> - STEP 1: Fix event ordering and happens-before tracking.
> - STEP 2: Fix batch mutation indexing.
> - STEP 3: Fix batch supersession and atomicity.
> - STEP 4: Implement a globally consistent oracle solver (forward search with exact canonical memoization and step budget).
> - STEP 5: Verify initial database state cleanliness.
> - Tests: T1 through T10 covering event ordering, supersession, global atomicity (Apple/Mango), duplicate keys, secondary batch keys, batch delete, in-flight all-or-nothing, branch isolation, memoization, and initial database state.

