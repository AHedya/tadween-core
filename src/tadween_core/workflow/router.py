import logging
from collections.abc import Callable

from tadween_core.broker import BaseMessageBroker
from tadween_core.exceptions import PolicyError, RoutingError
from tadween_core.repo.base import BaseArtifactRepo
from tadween_core.stage.policy import (
    ArtifactT,
    BucketSchemaT,
    InputT,
    OutputT,
    PartNameT,
    StagePolicy,
)
from tadween_core.task_queue.base import TaskEnvelope, TaskMetadata


class WorkflowRoutingPolicy(
    StagePolicy[InputT, OutputT, BucketSchemaT, ArtifactT, PartNameT]
):
    """
    A wrapper policy that handles the "Transport/Routing" layer of a workflow.

    It wraps a `stage_policy` (which handles persistence/lifecycle) and adds
    automatic message forwarding to the next stage(s) upon success.
    """

    def __init__(
        self,
        stage_policy: StagePolicy[InputT, OutputT, BucketSchemaT, ArtifactT, PartNameT],
        output_topics: list[str],
        stage_name: str | None = None,
        broker: BaseMessageBroker | None = None,
        repo: BaseArtifactRepo[ArtifactT, PartNameT] | None = None,
        # `OutputT` | `None` as intercept might not provide context payload if canceled
        payload_extractor: Callable[[OutputT | None], dict] | None = None,
        logger: logging.Logger | None = None,
    ):
        self._stage_policy = stage_policy
        self._output_topics = output_topics
        self._stage_name = stage_name or "WorkflowRouter"
        self._broker = broker
        self._repo = repo
        self.logger = logger or logging.getLogger("tadween.workflow.router")

        # Default: no payload passing
        # If returns:
        # None  -> propagate message payload
        # `x`   -> context payload
        # {}    -> No payload passing
        self._payload_extractor = payload_extractor or (lambda x: {})

    def resolve_inputs(self, message, repo=None, cache=None):
        return self._stage_policy.resolve_inputs(
            message=message, repo=self._repo or repo, cache=cache
        )

    def intercept(
        self,
        message,
        broker=None,
        repo=None,
        cache=None,
    ):
        active_broker = self._broker or broker

        context = self._stage_policy.intercept(
            message=message, broker=broker, repo=self._repo or repo, cache=cache
        )
        if not context or not context.intercepted:
            return context

        if context.action.on_done:
            self._stage_policy.on_done(
                message=message,
                envelope=TaskEnvelope(
                    payload=None,
                    metadata=TaskMetadata(
                        task_id="N/A", start_time=0, end_time=0, submit_time=0
                    ),
                    error=None,
                    success=True,
                ),
            )
        if context.action.on_success:
            self._stage_policy.on_success(
                task_id="N/A",
                message=message,
                result=context.payload,
                broker=active_broker,
                repo=self._repo or repo,
                cache=cache,
            )

        if active_broker and self._output_topics and context.action.publish:
            try:
                payload = self._payload_extractor(context.payload)
            except Exception as e:
                err = PolicyError(
                    message=f"Interception payload extractor failed: {e}",
                    stage_name=self._stage_name,
                    policy_name=self.__class__.__name__,
                    method="payload_extractor",
                    task_id="N/A",
                )
                self.logger.error(f"{err}", exc_info=True)
                raise err from e

            message.metadata.update(
                {
                    "parent_message_id": message.id,
                    "interception": {
                        "intercepted": context.intercepted,
                        "reason": context.reason,
                    },
                }
            )

            for topic in self._output_topics:
                try:
                    out_msg = message.fork(topic=topic, payload=payload)
                    active_broker.publish(out_msg)
                except Exception as e:
                    err = RoutingError(
                        message=f"Failed to publish to topic {topic}: {e}",
                        stage_name=self._stage_name,
                        topic=topic,
                        task_id="N/A",
                    )
                    self.logger.error(f"{err}", exc_info=True)
                    raise err from e
        if active_broker and context.action.ack:
            active_broker.ack(message.id)
        return context

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
            task_id=task_id,
            message=message,
            result=result,
            broker=active_broker,
            repo=repo,
            cache=cache,
        )

        # Routing (The "Glue" Logic)
        if active_broker and self._output_topics:
            try:
                payload = self._payload_extractor(result)
            except Exception as e:
                err = PolicyError(
                    message=f"Payload extractor failed: {e}",
                    stage_name=self._stage_name,
                    policy_name=self.__class__.__name__,
                    method="payload_extractor",
                    task_id=task_id,
                )
                self.logger.error(f"{err}", exc_info=True)
                raise err from e

            # propagate metadata
            message.metadata.update({"parent_message_id": message.id})

            for topic in self._output_topics:
                try:
                    out_msg = message.fork(topic=topic, payload=payload)
                    active_broker.publish(out_msg)
                except Exception as e:
                    err = RoutingError(
                        message=f"Failed to publish to topic {topic}: {e}",
                        stage_name=self._stage_name,
                        topic=topic,
                        task_id=task_id,
                    )
                    self.logger.error(f"{err}", exc_info=True)
                    raise err from e

        # We do this here so the wrapper owns the "Unit of Work" completion.
        # If any of the above (inner policy or routing) failed, Stage._on_task_done
        # will catch it and call on_error, which will call nack() -> ack().
        if active_broker:
            active_broker.ack(message.id)

    def on_error(
        self,
        message,
        error,
        broker=None,
    ):
        active_broker = self._broker or broker
        try:
            self._stage_policy.on_error(
                message=message, error=error, broker=active_broker
            )
        finally:
            if active_broker:
                # TODO: Determine requeue mechanism and control
                active_broker.nack(message.id, requeue_message=None)

    def on_done(self, message, envelope):
        self._stage_policy.on_done(message=message, envelope=envelope)

    def on_running(self, task_id, message):
        self._stage_policy.on_running(task_id=task_id, message=message)
