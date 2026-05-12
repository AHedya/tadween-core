"""
Heavy Media Processing Pipeline
Focus: Branching (Fan-out), Physical Backpressure (ResourceManager),
and `on_artifact_done` and plus Workflow-level Retries.
"""

import threading
import time
import urllib.error

from pydantic import BaseModel

from tadween_core.handler.base import BaseHandler
from tadween_core.workflow.retry import RetryPolicy
from tadween_core.workflow.workflow import Workflow


class VideoInput(BaseModel):
    video_url: str


class VideoPayload(BaseModel):
    video_id: str


class AudioOutput(BaseModel):
    video_id: str
    audio_path: str


class FrameOutput(BaseModel):
    video_id: str
    frames_path: str


class DownloaderHandler(BaseHandler[VideoInput, VideoPayload]):
    def __init__(self):
        self.attempts = 0
        self._lock = threading.Lock()

    def run(self, inputs: VideoInput) -> VideoPayload:
        with self._lock:
            self.attempts += 1
            # Simulate transient timeout on the first attempt
            if self.attempts == 1:
                raise urllib.error.URLError("Simulated Timeout")
            return VideoPayload(video_id="vid-123")


class AudioExtractorHandler(BaseHandler[VideoPayload, AudioOutput]):
    def __init__(self):
        self.execution_times = []

    def run(self, inputs: VideoPayload) -> AudioOutput:
        start_time = time.monotonic()
        time.sleep(0.2)  # Simulate heavy ML workload
        self.execution_times.append((start_time, time.monotonic()))
        return AudioOutput(video_id=inputs.video_id, audio_path="/tmp/audio.wav")


class FrameExtractorHandler(BaseHandler[VideoPayload, FrameOutput]):
    def __init__(self):
        self.execution_times = []

    def run(self, inputs: VideoPayload) -> FrameOutput:

        start_time = time.monotonic()
        time.sleep(0.2)  # Simulate heavy ML workload
        self.execution_times.append((start_time, time.monotonic()))
        return FrameOutput(video_id=inputs.video_id, frames_path="/tmp/frames/")


def test_heavy_media_pipeline(inmemory_broker):
    # 1. Physical Backpressure setup: Only 1 GPU available
    workflow = Workflow(
        broker=inmemory_broker,
        resources={"gpu": 1},
        # pass payload
        default_payload_extractor=lambda x: x,
    )

    downloader = DownloaderHandler()
    audio_extractor = AudioExtractorHandler()
    frame_extractor = FrameExtractorHandler()

    # 2. Downloader (Retries on timeout)
    workflow.add_stage(
        "downloader",
        handler=downloader,
        retry_policy=RetryPolicy(retry_on={urllib.error.URLError}, max_retries=3),
    )

    # 3. Branching & Physical Backpressure
    workflow.add_stage(
        "audio_extract",
        handler=audio_extractor,
        demands={"gpu": 1},
    )
    workflow.add_stage(
        "frame_extract",
        handler=frame_extractor,
        demands={"gpu": 1},
    )

    workflow.link("downloader", "audio_extract")
    workflow.link("downloader", "frame_extract")
    workflow.set_entry_point("downloader")
    workflow.build()

    # We track whether the fan-in completed via EVENT_ARTIFACT_DONE
    artifact_done = threading.Event()

    def package_results_callback(artifact_id, **kwargs):  # noqa: ARG001
        artifact_done.set()

    # Listen to artifact completion event
    workflow.context.on_artifact_done(package_results_callback)
    ## or it's equivalent
    # workflow.context.on(EVENT_ARTIFACT_DONE, package_results_callback)

    # 4. Execute
    # We submit the job. The downloader will fail once, then succeed.
    workflow.submit(
        VideoInput(video_url="http://example.com/video.mp4"),
        metadata={"artifact_id": "vid-job-1"},
    )

    # Wait for all stages to complete
    inmemory_broker.join(timeout=5)

    assert downloader.attempts == 2
    assert len(audio_extractor.execution_times) == 1
    assert len(frame_extractor.execution_times) == 1
    assert artifact_done.is_set()
