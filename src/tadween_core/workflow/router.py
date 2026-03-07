from collections.abc import Callable
from typing import Any

from tadween_core.broker import BaseMessageBroker
from tadween_core.exceptions import PolicyError, RoutingError
from tadween_core.stage.policy import (
    ArtifactT,
    BucketSchemaT,
    InputT,
    OutputT,
    PartNameT,
    StagePolicy,
)


class WorkflowRoutingPolicy(
    StagePolicy[InputT, OutputT, BucketSchemaT, ArtifactT, PartNameT]
):
    """
    A Decorator Policy that handles the "Transport/Routing" layer of a workflow.

    It wraps a `stage_policy` (which handles persistence/lifecycle) and adds
    automatic message forwarding to the next stage(s) upon success.
    """

    def __init__(
        self,
        stage_policy: StagePolicy[InputT, OutputT, BucketSchemaT, ArtifactT, PartNameT],
        output_topics: list[str],
        stage_name: str | None = None,
        broker: BaseMessageBroker | None = None,
        payload_extractor: Callable[[OutputT], Any] | None = None,
    ):
        self._stage_policy = stage_policy
        self._output_topics = output_topics
        self._stage_name = stage_name or "WorkflowRouter"
        self._broker = broker

        # Default: no payload passing
        self._payload_extractor = payload_extractor or (lambda x: {})

    def resolve_inputs(self, message, repo=None, cache=None):
        return self._stage_policy.resolve_inputs(message, repo, cache)

    def on_success(
        self,
        task_id,
        message,
        result,
        broker=None,
        repo=None,
        cache=None,
    ):
        active_broker = self._broker or broker

        # Execute Inner Policy (save to DB/Repo/cache)
        self._stage_policy.on_success(
            task_id, message, result, active_broker, repo, cache
        )

        # Routing (The "Glue" Logic)
        if active_broker and self._output_topics:
            try:
                payload = self._payload_extractor(result)
            except Exception as e:
                raise PolicyError(
                    message=f"Payload extractor failed: {e}",
                    stage_name=self._stage_name,
                    policy_name=self.__class__.__name__,
                    method="payload_extractor",
                    task_id=task_id,
                ) from e

            # propagate metadata
            message.metadata.update({"parent_message_id": message.id})

            for topic in self._output_topics:
                try:
                    out_msg = message.fork(topic=topic, payload=payload)
                    active_broker.publish(out_msg)
                except Exception as e:
                    raise RoutingError(
                        message=f"Failed to publish to topic {topic}: {e}",
                        stage_name=self._stage_name,
                        topic=topic,
                        task_id=task_id,
                    ) from e

        # We do this here so the wrapper owns the "Unit of Work" completion
        if active_broker:
            active_broker.ack(message.id)

    def on_error(
        self,
        message,
        error,
        broker=None,
    ):
        active_broker = self._broker or broker
        self._stage_policy.on_error(message, error, active_broker)

        if active_broker:
            # TODO: Determine requeue mechanism and control
            active_broker.nack(message.id, requeue_message=None)

    def on_done(self, message, envelope):
        self._stage_policy.on_done(message, envelope)

    def intercept(self, message, broker=None, repo=None, cache=None):
        self._stage_policy.intercept(message, broker, repo, cache)

    def on_running(self, task_id, message):
        self._stage_policy.on_running(task_id, message)
