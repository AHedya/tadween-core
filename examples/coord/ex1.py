"""Real-world use case:
Having an ASR DAG. It starts by loading audio file into memory, and branches into diarization, and transcription.
In such cases, you need two coordination points between the loader and streamline stages:
1. Loader shouldn't eagerly load audio unless *stash* quota still has space (discussed the example 2)
2. Free cached audio file if all consuming stages are done with it (diarization, and transcription)
---
        Loader
    ┌───────┴─────┐
    ↓             ↓
diarization    transcription


`track_artifact_progress` provides high-level function for tracking artifact state.
Initially, the artifact count is equals *one* (1).
An artifact is considered *done* when its count reaches *zero* (<=0).

This quiescence-like mechanism is handled automatically by `WorkflowRoutingPolicy`, but in this example,
we will manage the branching, increment, decrement ourselves.
"""

import threading
import time

from tadween_core.coord.context import WorkflowContext


def main():

    # 1. Initialize the global context
    context = WorkflowContext()

    # 2. cleanup on artifact being done.
    def cleanup_resources(
        artifact_id: str,
        cache_key: str | None = None,
        **kwargs,  # noqa
    ):
        print(f"\n[EVENT BUS] Received Completion for artifact '{artifact_id}'")
        print(f"[CLEANUP] Freeing heavy resources (Cache Key: {cache_key})...")
        # e.g., cache.pop(cache_key), gc.collect(), etc.
        print(f"[CLEANUP] Resources for '{artifact_id}' released successfully.")

    # Additional event on artifact being done.
    # `**kwargs` is important if you intend to ignore the cache_key. Or simply define `cache_key` and omit it.
    def fire_webhook(artifact_id: str, **kwargs):  # noqa: ARG001
        print(f"[WEBHOOK] {artifact_id} IS DONE")

    # Register the listener
    context.on_artifact_done(cleanup_resources)
    context.on_artifact_done(fire_webhook)

    # tadween-core automatically tracks active tasks internally via `context.track_artifact_progress()`
    # which in-turn is used by the `WorkflowRoutingPolicy`.
    artifact_id = "zxc1234_file_1"
    cache_key = "cache_001"

    print(f"\n[ENGINE] New artifact entered pipeline: {artifact_id}")
    # Router: Task created at Entry Point
    context.track_artifact_progress(artifact_id, 1)

    print("[ENGINE] Processing Stage 1: Loader")
    time.sleep(0.2)  # Simulate work

    print(
        "[ENGINE] Stage 1 Success. Branching into Stage 2.1 (Transcription) and Stage 2.2 (Diarization)"
    )
    #        Loader
    #   ┌───────┴─────┐
    #   ↓             ↓
    # diarization    transcription

    # Router: Output routes to 2 branches. Current task ends, 2 new begin. Net change = +1
    context.track_artifact_progress(artifact_id, 1)

    # Simulate parallel execution of branches
    def simulate_stage(stage_name, duration):
        print(f"[{stage_name}] Starting...")
        time.sleep(duration)
        print(f"[{stage_name}] Finished.")

        # Router: Task completes, decrement counter
        context.track_artifact_progress(artifact_id, -1, cache_key=cache_key)

    t1 = threading.Thread(target=simulate_stage, args=("Stage 2.1: Transcription", 0.3))
    t2 = threading.Thread(target=simulate_stage, args=("Stage 2.2: Diarization", 0.1))

    t1.start()
    t2.start()

    t1.join()
    t2.join()


if __name__ == "__main__":
    main()
