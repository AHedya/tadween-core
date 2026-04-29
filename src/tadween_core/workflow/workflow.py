import threading
from collections.abc import Callable
from logging import Logger, getLogger
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel
from typing_extensions import TypeVar

from tadween_core.broker import BaseMessageBroker, Message
from tadween_core.cache.base import BaseCache
from tadween_core.coord import (
    ResourceManager,
    StageContextConfig,
    WorkflowContext,
)
from tadween_core.handler import BaseHandler, HandlerFactory, InputT, OutputT
from tadween_core.repo.base import BaseArtifactRepo
from tadween_core.stage.policy import DefaultStagePolicy, StagePolicy
from tadween_core.stage.stage import Stage
from tadween_core.task_queue.base_queue import BaseTaskQueue
from tadween_core.workflow.router import WorkflowRoutingPolicy

BucketSchemaT = TypeVar("BucketSchemaT", default=Any)


class Workflow:
    """
    Orchestrator for Stages.
    Builds a Directed Acyclic Graph (DAG) of stages, manages topics,
    enforces topology rules (no fan-in, no cycles), and handles lifecycle.
    """

    def __init__(
        self,
        broker: BaseMessageBroker,
        name: str | None = None,
        cache: BaseCache[BucketSchemaT] | None = None,
        repo: BaseArtifactRepo | None = None,
        life_length: float | None = None,
        logger: Logger | None = None,
        default_payload_extractor: Callable[[Any | None], dict] | None = None,
        resources: dict[str, float] | None = None,
        context: WorkflowContext | None = None,
    ):
        """
        Args:
            name: Workflow identifier.
            broker: Singleton message broker instance.
            cache: Singleton cache instance.
            repo: Singleton repository instance.
            life_length: Time in seconds before the workflow is forcibly killed.
                None means infinite life.
            resources: Global resource pool (e.g. ``{"cuda": 1, "RAM_MB": 2048}``).
                Stages declare *demands* against these resources; the manager
                ensures no more than the configured capacity is allocated at once.
        """
        self.broker = broker
        self.name = name or f"Workflow-{id(self):x}"
        self.cache = cache
        self.repo = repo
        self.life_length = life_length
        self.logger = logger or getLogger(f"tadween.workflow.{self.name}")
        self.payload_extractor = default_payload_extractor
        self.resource_manager = ResourceManager(resources) if resources else None
        self.context = context or WorkflowContext()

        self._stages: dict[str, Stage] = {}
        # Adjacency list: source_stage -> [target_stages]
        self._adjacency_list: dict[str, list[str]] = {}
        # Reverse tracking for Fan-In detection: target_stage -> [source_stages]
        self._reverse_adj: dict[str, list[str]] = {}

        self._entry_point_config: tuple[str, str] | None = (
            None  # (stage_name, external_topic)
        )
        self._is_built = False
        self._timer: threading.Timer | None = None

    def add_stage(
        self,
        name: str,
        handler: BaseHandler[InputT, OutputT] | HandlerFactory,
        *,
        policy: StagePolicy | None = None,
        cache: BaseCache | None = None,
        repo: BaseArtifactRepo | None = None,
        task_queue: BaseTaskQueue | None = None,
        demands: dict[str, float] | None = None,
        context_config: StageContextConfig | None = None,
        queue_size: int = 0,
        log_exc_info: bool = True,
    ) -> "Workflow":
        """
        Builds a Stage from components and integrates it into the workflow.
        This is the factory method for stages.
        """
        if name in self._stages:
            raise ValueError(
                f"Stage '{name}' already exists in workflow '{self.name}'."
            )

        if isinstance(handler, HandlerFactory):
            handler_instance = handler.create()
        else:
            handler_instance = handler

        stage = Stage(
            handler=handler_instance,
            name=name,
            policy=policy or DefaultStagePolicy(),
            # override workflow-wide infra
            repo=repo or self.repo,
            cache=cache or self.cache,
            task_queue=task_queue,
            resource_manager=self.resource_manager,
            demands=demands,
            context_config=context_config,
            queue_size=queue_size,
            log_exc_info=log_exc_info,
        )

        return self.integrate_stage(name, stage)

    def integrate_stage(self, name: str, stage: Stage) -> "Workflow":
        """
        Integrates a pre-built Stage object into the workflow.

        Note: This method will WRAP the existing policy of the stage
        with a WorkflowRoutingPolicy to manage graph topology.
        """
        if name in self._stages:
            raise ValueError(
                f"Stage '{name}' already exists in workflow '{self.name}'."
            )

        if not stage.cache:
            stage.cache = self.cache

        stage.context_config.context = self.context

        # Wrap the user's policy with our RoutingPolicy to handle transport
        # We initialize with empty topics; they will be populated during `build()`
        routing_policy = WorkflowRoutingPolicy(
            stage_policy=stage.policy,
            broker=self.broker,
            output_topics=[],
            stage_name=name,
            repo=self.repo,
            payload_extractor=self.payload_extractor,
            context=self.context,
        )

        stage.policy = routing_policy

        self._stages[name] = stage
        self._adjacency_list[name] = []
        self._reverse_adj[name] = []

        return self

    def link(self, from_stage: str, to_stage: str) -> "Workflow":
        """
        Creates a directed edge: from_stage -> to_stage.
        Enforces:
        1. Stages exist.
        2. No Fan-In (to_stage has no other parents).
        3. No Cycles.
        """
        if self._is_built:
            raise RuntimeError(
                "Workflow has already been built. Cannot modify topology."
            )

        # Existence Check
        if from_stage not in self._stages:
            raise ValueError(f"Source stage '{from_stage}' not found.")
        if to_stage not in self._stages:
            raise ValueError(f"Target stage '{to_stage}' not found.")

        # Fan-In Check (No multiple parents)
        if self._reverse_adj[to_stage]:
            parents = ", ".join(self._reverse_adj[to_stage])
            raise ValueError(
                f"Fan-in is not supported. Stage '{to_stage}' already has input(s) from: [{parents}]. "
                f"Cannot link from '{from_stage}'."
            )

        # Cycle Detection
        if self._creates_cycle(from_stage, to_stage):
            raise ValueError(
                f"Cycle detected: Linking '{from_stage}' -> '{to_stage}' forms a loop. "
                "Workflows must be acyclic."
            )

        # Register Link
        self._adjacency_list[from_stage].append(to_stage)
        self._reverse_adj[to_stage].append(from_stage)

        return self

    def _creates_cycle(self, start: str, end: str) -> bool:
        """
        DFS to check if 'end' can reach 'start' via existing paths.
        If end -> ... -> start exists, then adding start -> end creates a cycle.
        """
        visited = set()
        stack = [end]

        while stack:
            node = stack.pop()
            if node == start:
                return True
            if node in visited:
                continue
            visited.add(node)
            # Add neighbors (children of node in the graph)
            stack.extend(self._adjacency_list.get(node, []))

        return False

    def _validate_resource_demands(self) -> None:
        if not self.resource_manager:
            return

        capacity = self.resource_manager.capacity
        for stage_name, stage in self._stages.items():
            if not stage.demands:
                continue
            for resource, units in stage.demands.items():
                if resource not in capacity:
                    raise ValueError(
                        f"Stage '{stage_name}' demands unknown resource '{resource}'. "
                        f"Available resources: {list(capacity.keys())}."
                    )
                if units > capacity[resource]:
                    raise ValueError(
                        f"Stage '{stage_name}' demands {units} {resource} "
                        f"but only {capacity[resource]} available."
                    )

    def set_entry_point(self, stage_name: str, topic: str | None = None) -> "Workflow":
        """
        Defines the entry point of the workflow.
        Submissions to this topic will trigger 'stage_name'.
        """
        if stage_name not in self._stages:
            raise ValueError(f"Stage '{stage_name}' not found.")

        # Check if this stage is already a target of another link (Fan-in on entry?)
        # Usually entry points are roots, so they shouldn't have parents.
        if self._reverse_adj[stage_name]:
            raise ValueError(
                f"Stage '{stage_name}' cannot be an entry point because it receives inputs "
                f"from other stages: {self._reverse_adj[stage_name]}."
            )

        if not topic:  # empty string is considered None in our case
            topic = "workflow.start"

        self._entry_point_config = (stage_name, topic)
        return self

    def build(self):
        """
        Finalizes the workflow:
        1. Validates resource demands.
        2. Generates topics.
        3. Updates RoutingPolicies.
        4. Subscribes stages to brokers.
        5. Starts the lifecycle timer.
        """
        if self._is_built:
            return
        self._is_built = True

        # Validate resource demands
        self._validate_resource_demands()

        # Wire internal links
        for src, targets in self._adjacency_list.items():
            # Generate internal topics for each link
            # Convention: topic.{src}.{dst}
            internal_topics = [f"topic.{src}.{t}" for t in targets]

            # Update the source stage's policy to publish to these topics
            src_stage = self._stages[src]
            if isinstance(src_stage.policy, WorkflowRoutingPolicy):
                src_stage.policy._output_topics = internal_topics
            else:
                raise RuntimeError(f"Stage {src} does not have a RoutingPolicy.")

            # Subscribe targets
            for topic, target_name in zip(internal_topics, targets, strict=True):
                target_stage = self._stages[target_name]
                self.broker.subscribe(
                    topic=topic,
                    handler=self.routing_handler_factory(
                        stage=target_stage, fn_name=f"route.{src}_to_{target_name}"
                    ),
                    auto_ack=False,
                )

        # Wire entry point
        if self._entry_point_config:
            stage_name, topic = self._entry_point_config
            entry_stage = self._stages[stage_name]
            self.broker.subscribe(
                topic=topic,
                handler=self.routing_handler_factory(
                    stage=entry_stage,
                    fn_name=f"route.start_to_{stage_name}",
                    is_entry_point=True,
                ),
                auto_ack=False,
            )

        if self.life_length is not None:
            self._timer = threading.Timer(self.life_length, self._kill_workflow)
            self._timer.start()

        self.logger.info(f"Workflow '{self.name}' built successfully.")

    def routing_handler_factory(
        self, stage: Stage, fn_name: str, is_entry_point: bool = False
    ):
        def handler(msg: Message, stage=stage):
            artifact_id = msg.metadata.get("artifact_id")
            if is_entry_point and artifact_id and self.context:
                self.context.track_artifact_progress(artifact_id, 1)

            try:
                stage.submit_message(msg)
            except Exception:
                if is_entry_point and artifact_id and self.context:
                    self.context.track_artifact_progress(artifact_id, -1)
                raise

        handler.__name__ = fn_name
        return handler

    def submit(
        self,
        payload: dict | BaseModel,
        metadata: dict | None = None,
        topic: str | None = None,
    ) -> str:
        """
        Submission interface.

        Args:
            payload: The data to process. Can be a dict or a Pydantic model.
            topic: The topic to publish to. If None, uses the configured entry point topic.

        Returns:
            The message ID.
        """
        if not self._is_built:
            self.build()

        target_topic = topic
        if target_topic is None:
            if not self._entry_point_config:
                raise RuntimeError("No entry point configured and no topic provided.")
            target_topic = self._entry_point_config[1]

        # Enveloping
        if isinstance(payload, BaseModel):
            payload_data = payload.model_dump()
        else:
            payload_data = payload

        msg = Message(topic=target_topic, payload=payload_data, metadata=metadata)
        self.broker.publish(msg)
        return msg.id

    def _kill_workflow(self, timeout: float | None = None):
        """
        Forcibly closes the workflow and its resources.
        """
        self.logger.critical(
            f"Workflow '{self.name}' life length exceeded. Killing workflow..."
        )
        self.close(timeout=timeout, force=True)

    def close(self, timeout: float | None = None, force: bool = False):
        """Cleanup resources."""
        if self._timer:
            self._timer.cancel()

        self.logger.info(f"Closing workflow '{self.name}'...")

        if self.resource_manager and not self.resource_manager.is_shutdown:
            self.resource_manager.shutdown()

        if self.context and not self.context.is_shutdown:
            self.context.shutdown()

        if hasattr(self.broker, "close"):
            # If InMemoryBroker, it's fine. If shared RabbitMQ connection, be careful.
            self.broker.close(timeout=timeout, force=force)

        for stage in self._stages.values():
            stage.close(force=force)

    def visualize(
        self,
        output_path: str | Path | None = None,
        format: Literal["graphviz", "mermaid", "dot"] = "graphviz",
        include_topics: bool = True,
        include_metadata: bool = False,
        view: bool = False,
        engine: Literal["dot", "neato", "fdp", "circo"] = "dot",
    ) -> str:
        """
        Generate a visual representation of the workflow DAG.

        Args:
            output_path: Where to save the output file. If None, returns source code only.
                        For graphviz: saves as .png/.svg/.pdf (based on extension)
                        For mermaid: saves as .mmd text file
            format: Output format - 'graphviz', 'mermaid', or 'dot' (raw DOT source)
            include_topics: Whether to show topic names on edges (graphviz only)
            include_metadata: Whether to include handler/policy info in nodes
            view: Whether to automatically open/display the visualization (graphviz only)
            engine: Graphviz layout engine ('dot' for hierarchical, 'neato' for spring,
                    'fdp' for force-directed, 'circo' for circular)

        Returns:
            The generated source code (DOT or Mermaid) as a string, or path to saved file
        """
        from .visualizer import WorkflowVisualizer

        return WorkflowVisualizer(self).render(
            output_path=output_path,
            format=format,
            include_topics=include_topics,
            include_metadata=include_metadata,
            view=view,
            engine=engine,
        )

    def get_topology_info(self) -> dict:
        """
        Get topology information about the workflow.

        Returns:
            Dictionary with workflow topology details including:
            - stages: list of stage names
            - edges: list of (source, target) tuples
            - entry_point: (stage_name, topic) or None
            - leaf_stages: stages with no outgoing edges
            - depth: maximum path length from entry to any leaf
        """
        info = {
            "name": self.name,
            "num_stages": len(self._stages),
            "stages": list(self._stages.keys()),
            "edges": [
                (src, tgt)
                for src, targets in self._adjacency_list.items()
                for tgt in targets
            ],
            "entry_point": self._entry_point_config,
            "is_built": self._is_built,
        }

        # Find leaf stages (no outgoing edges)
        leaf_stages = [
            name for name, targets in self._adjacency_list.items() if not targets
        ]
        info["leaf_stages"] = leaf_stages

        # Calculate depth (longest path from entry point)
        if self._entry_point_config:
            entry_stage = self._entry_point_config[0]
            info["depth"] = self._calculate_max_depth(entry_stage)
        else:
            info["depth"] = None

        return info

    def _calculate_max_depth(self, start_stage: str, visited: set | None = None) -> int:
        """Calculate maximum depth from a starting stage using DFS."""
        if visited is None:
            visited = set()

        if start_stage in visited:
            return 0

        visited.add(start_stage)
        targets = self._adjacency_list.get(start_stage, [])

        if not targets:
            return 0

        max_child_depth = max(
            self._calculate_max_depth(target, visited.copy()) for target in targets
        )
        return 1 + max_child_depth
