# Performance Benchmarks & Scaling Analysis
This document outlines the performance characteristics of Tadween-core workflows under varying concurrency models against runtime environments, and memory usage.
## Runtime: Benchmark workflow runtime duration
### Experimental Setup
To identify the framework's limits, we benchmarked a "bottlenecked" linear workflow:
    Stage 1 (I/O Bound): `MockDownloadHandler` (5 thread workers). Simulates network latency and bandwidth limits.Default settings: file_size_mb=1, bandwidth_mbps=10, latency_ms=50, cpu_overhead=0.05, enable_compression=True, enable_checksum=True.
    Stage 2 (CPU Bound): The Bottleneck. Calculates the sum of squares for 50M elements.
    Stage 3 (I/O Bound): `MockDownloadHandler` (5 thread workers). However, this stage is named "upload" just for clarity.

### Variables Tested

- Worker Count: 1, 2, and 5.
- Concurrency Type: Multi-threading vs. Multi-processing (fork vs. spawn).
- Runtime Logic: Native Python vs. Numpy (GIL-releasing C-extensions).

[!NOTE]
Stage Efficiency is defined as: Task Execution Time / (Execution Time + Queue Waiting Time).

```python
workers: Literal[1, 2, 5] = 1
worker_type: Literal["thread", "process"] = "thread"
if worker_type == "process"
    start_method: Literal["fork", "spawn"] = "fork"
runtime_type: Literal['native',"numpy"]
```

All tasks are submitted simultaneously. Once a stage completes its work, it notifies dependent downstream stages, which then begin execution. The order of execution across independent stages is not guaranteed; however, dependent stages will not start until their dependencies signal completion.

Note on "tasks" in the graphs: A task in the graph is a logical unit representing a single end-to-end job, composed of subtasks — one per stage. It serves as a unified identifier across the pipeline.

Stage efficiency is defined as: task execution time / (execution time + waiting time). While this metric can be misleading in isolation, it serves as a useful signal for identifying stages suffering from queuing bottlenecks — high waiting time relative to execution time indicates the stage is starved or overloaded.

### Observations
- single thread (t1): Worst stage efficiency and longest overall execution time (~50s). The shortest-running task achieves 100% efficiency in every stage, meaning it encountered no queue wait from the moment it was submitted — a natural outcome when workers are briefly free at the start.
- Two threads (t2): Same execution time as _single thread workflow_. However nearly double stage efficiency in _CPU-Bound_ stage. This does not indicate faster throughput — the GIL serializes CPU-bound Python threads, preventing true parallelism. What increases is the frequency of context switching, which gives more workers the appearance of making progress, but the overall wall-clock time remains unchanged
- Five threads (t5): Same observation as t2. The efficiency jumps from 22% to 65%. Again efficiency is a misleading metric name (and value) if we don't know the nature of the workflow. Although all our stage have relatively higher efficiency, they all take the same time: ~50s

| Workers | Total Runtime | Stage Efficiency   | Observation                                                       |
|---------|---------------|--------------------|-------------------------------------------------------------------|
| 1 (t1)  | ~50s          | ~10%               | No contention, but serialized execution.                          |
| 2 (t2)  | ~50s          | ~20%               | Throughput is identical; workers simply fight for the GIL.        |
| 5 (t5)  | ~50s          | ~60%               | High "efficiency" is misleading; total runtime remains unchanged. |

- Single process - fork (p1-fork): Slightly faster workflow execution time. Unlike threads, a subprocess has its own independent GIL, enabling true CPU-parallelism for the offloaded task. However, with only one process worker, CPU-bound tasks are still serialized. The upstream download stage continues feeding tasks faster than the bottleneck can drain them, leading to queue buildup and low stage efficiency.
- Two  processes - fork (p2-fork): Now we're talking. reducing the workflow runtime to the half (from 50s to 25s). The CPU-Bound stage still accounts for ~80% of total runtime, with ~20% efficiency (80% of the time spent waiting), confirming it remains the dominant bottleneck even as throughput doubles.
- Five processes - fork (p5-fork): Workflow runtime halves again to ~12s. _CPU-Bound_ stage efficiency reaches ~50%, and its share of total runtime drops to ~60%. Scaling process workers continues to yield proportional gains as long as true CPU parallelism is available.
- Spawn processes: Behavior mirrors fork-based processes, with a slight runtime overhead attributable to the process initialization cost — spawn must re-import the Python interpreter and module state from scratch, unlike fork which copies the parent process memory.

---

As our _CPU-Bound_ stage is for sure bottleneck-ed. We've tried optimizing it in native python as possible. Now let's benchmark introducing optimized solutions such as numpy in our workflow. Numpy is GIL-releasing, so it would fit in a threaded workflow.

---

- Single thread - numpy(t1-numpy): _CPU-Bound_ stage efficiency is ~33%, yet it accounts for only ~10% of total workflow runtime. Overall execution time drops dramatically — from ~50s in native single-threaded mode and ~12s in the five-process configuration, to just ~7s. 
- Two threads, Five threads - numpy (t2-numpy, t5-numpy): No meaningful improvement in workflow runtime over t1-numpy. Stage efficiency appears to increase, but this is an artifact of how work is dispatched: Python threads submit tasks to NumPy's internal execution engine and then appear "idle" within our workflow's accounting. The actual computation still occurs, but outside our task queue — queuing pressure appears lower, while the real work is simply invisible to our metrics. Adding more Python threads on top of an already internally-parallel NumPy operation yields diminishing returns
- Process + NumPy (exploratory): Running the NumPy handler in a subprocess produces results nearly identical to the threaded configuration. Since NumPy already saturates available CPU resources internally, adding process-level parallelism on top provides no meaningful benefit and only introduces process overhead.

### Key Takeaways
For CPU-bound stages in pure Python, threads provide no runtime benefit due to the GIL — only processes achieve true parallelism. However, GIL-releasing libraries like NumPy can outperform a multi-process setup at a fraction of the complexity, provided the workload maps well to vectorized operations. The efficiency metric, while useful for spotting queuing pressure, must always be interpreted alongside absolute execution time and the nature of the work being measured


## Memory Benchmarks (RAM Usage)

### Experiment setup
Main memory usage comes from two main sources:
1. Result Retention: concurrent.futures.Future objects holding TaskEnvelope payloads (e.g., heavy ASR results).
2. Process Isolation Overhead: The memory cost of fork vs. spawn multiprocessing workers.

To simulate heavy payload transfers, we configured a handler that returns a 10MB bytearray after a 0.2s sleep. We ran 10 tasks (100MB total payload) across isolated sessions to prevent GC pollution.

Metrics: We tracked PSS (Proportional Set Size) to accurately account for shared memory pages between processes. The child-process discovery thread polled at 1-second intervals, while the main thread polled at 0.1-second intervals

### Variables Tested
1. Number of workers: [1, 2, 5]
2. Task queue worker type: ["thread", "process"]
3. Start method: ["fork", "spawn"]
4. Retain result: [True, False]. False means Immediate consumption

### Observations
- t1-retain: starts at ~50MB RAM usage, RAM usage increases by 10MB each ~0.2 seconds until peaks at 150MB (50MB start + 100MB the expected load) after nearly 2 seconds, then stays quite for 0.5s (constant waiting). Expected and confirms 
- t2-retain: Same pattern as before, starts at ~50MB, higher memory usage jumps due to having two working threads that keep the results. Peaks at ~150MB after 1 second and keeps still for 0.5s.
- t5-retain: Same pattern as `t2-retain`. Higher jumps, finishes faster (0.4s). starts at ~50MB and peaks at ~150MB.
- p1-fork-retain: _nearly_ the same start as the threads. However, unexpected jumps happen early. Child processes usage don't appear before the second 1 because of our child processes discovery thread. However, this jump in the beginning might be explained by having a child process that produces heavy result object: process memory + heavy result + main process. We can look at children usage and find that `p1-fork-retain-children` usage is ~30MB + ~160MB for the main process only. Another observation is runtime is worse than using single thread, expected due to process init, and pickle overhead. 160
- p2-fork-retain: Same start as `p1-fork-retain` with steeper memory usage jumps. Most important part is looking at children usage for `p2-fork-retain`. The difference between `p1-fork-retain-children` and `p2-fork-retain-children` is exactly ~20MB: 30MB and 50MB respectively. Main process memory usage: ~150MB. Less than `p1-fork-retain` by 10MB.
- p5-fork-retain: Same pattern as `p2-fork-retain`. Stepper jumps. Children memory usage jumps to 100MB, and 150MB for the Main process.
- p1-spawn-retain: Stable start at 50MB usage for the main process with 0 child processes RAM usage. Once child process starts being collected, we measure 60MB in child processes usage. Relatively larger RAM usage for single process. Peaks at 230MB = 170MB main process + 60MB children (one process for p1)
- p2-spawn-retain: Same as `p1-spawn-retain`. But steeper jumps, and Children usage raises to 120. Peaks at 290MB = 170MB Main process + 120MB children
- p5-spawn-retain: Same pattern. 405MB = 165MB Main process + 240MB children

---

**Observation**: Once child processes being collected, their usage stays stable and never changes
**Answer** because the workflow is so small that the child processes are collected only once. This is because child processes discovery is expensive, so our discovery thread runs once each 1 second, while our main process monitoring thread runs each 0.1 second. That's why main process metrics feel dynamic while child processes are still.

---

Previous results were conducted with retain_results flag set to True. The coming results are conducted in the optimized version that consumes results immediately. Consumption can be done by saving in a cache, or to repository, but the result is anyways removed from task queue registry so python GC can free up memory. As this behavior should be the default (retain_results set to False), `retain` variable will be dropped in the coming experiments tags (titles).

- t1: The expected 50MB RAM usage start. It raises to 60MB and stills quite until quiets. This is expected due to our `concurrent.futures._base.Executor` implementation, a worker would keep local reference to the result of the last task it executed until it picks up the next task.
- t2: 50MB start, but jumps to 70MB usage and stands still. *Expected*. 10MB * 2 workers + 50MB base = 70MB
- t5: 50MB start, but jumps to 100MB usage and stands still.

- p1-fork: Main process starts at 50MB, drops to 35MB (weird interaction), keeps the low usage until near the end of execution returns back to 50MB. Children usage (one process)= ~30MB. Peaks at: 80MB
- p2-fork: Same weird interaction: Main process starts at 50MB, drops to 25MB then returns again to 50MB near the end. Children usage: 50MB. Peaks at 100MB: 50MB main + 50MB children.
- p5-fork: Even weirder: Main process starts at 30MB, descends to 20MB and raises to 50MB just before execution ends. Fun observation: Although this run is expected to have higher children usage, it's actually zero usage. Why? because it was so efficient that the workflow finished  under one second that's child processes never appeared in metrics.
**The steady starts: 50MB, then drops down to 40-35MB is actually due to how PSS makes up a room for shared RAM pages.**

- p1-spawn: Main process starts at 50MB and the usage stays as it is until the end. Children usage started and stayed still at 60MB. This run was slow and took more than 2 seconds in execution that we could have two measurements for the children. Both the measurements are rounded to ~60MB. Peaks at 110MB.
- p2-spawn: Main process starts at 50MB drops to 40MB, and raises back to 50MB. Children usage: 120MB. Peaks at 170MB.
- p5-spawn: Same interaction happened at `p5-fork`. Couldn't get reliable measurements due to so fast execution.

### Key Takeaways
While process pool executor memory usage wasn't reliable due to slow children discovery process with so fast execution, results weren't reliable (actually we couldn't measure anything). However, we can rely on threads usages and metrics as it never has child processes.
Processes runs need to be re-conducted before giving any advices. However, It's clear that spawn start method is relatively more expensive in both runtime and memory usage.

1. Thread Idle Retention: Be aware that threaded workers hold onto their last payload. If memory is fiercely constrained, ensure the worker's final task is a lightweight "cleanup" operation.
2. spawn is Expensive: Using the spawn start method carries a heavy ~60MB minimum tax per worker. Use fork wherever OS support allows, unless relying on C-extensions that aggressively break fork safety.