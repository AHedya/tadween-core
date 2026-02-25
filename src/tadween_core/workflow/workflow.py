import threading
from logging import Logger, getLogger
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel
from typing_extensions import TypeVar

from tadween_core.broker import BaseMessageBroker, Message
from tadween_core.cache import Cache
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
        cache: Cache[BucketSchemaT] | None = None,
        life_length: float | None = None,
        logger: Logger | None = None,
    ):
        """
        Args:
            name: Workflow identifier.
            broker: Singleton message broker instance.
            cache: Singleton cache instance.
            life_length: Time in seconds before the workflow is forcibly killed.
            None means infinite life.
        """
        self.broker = broker
        self.name = name or f"Workflow-{id(self)}"
        self.cache = cache
        self.life_length = life_length
        self.logger = logger or getLogger(__name__)

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
        repo: BaseArtifactRepo | None = None,
        task_queue: BaseTaskQueue | None = None,
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
            handler_instance = handler.create_and_warmup()
        else:
            handler_instance = handler

        stage = Stage(
            handler=handler_instance,
            name=name,
            policy=policy or DefaultStagePolicy(),
            repo=repo,
            cache=self.cache,
            task_queue=task_queue,
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

        # Wrap the user's policy with our RoutingPolicy to handle transport
        # We initialize with empty topics; they will be populated during `build()`
        routing_policy = WorkflowRoutingPolicy(
            stage_policy=stage.policy,
            broker=self.broker,
            output_topics=[],
            stage_name=name,
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
        1. Generates topics.
        2. Updates RoutingPolicies.
        3. Subscribes stages to brokers.
        4. Starts the lifecycle timer.
        """
        if self._is_built:
            return
        self._is_built = True

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
                    stage=entry_stage, fn_name=f"route.start_to_{stage_name}"
                ),
                auto_ack=False,
            )

        if self.life_length is not None:
            self._timer = threading.Timer(self.life_length, self._kill_workflow)
            self._timer.start()

        self.logger.info(f"Workflow '{self.name}' built successfully.")

    def routing_handler_factory(self, stage: Stage, fn_name: str):
        def handler(msg: Message, stage=stage):
            stage.submit_message(msg)

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
        self.close(timeout=timeout)

    def close(self, timeout: float | None = None, join_timeout: float | None = None):
        """Cleanup resources."""
        if self._timer:
            self._timer.cancel()

        self.logger.info(f"Closing workflow '{self.name}'...")

        if hasattr(self.broker, "join"):
            self.broker.join(join_timeout)

        for stage in self._stages.values():
            stage.close()

        # Assuming the broker is scoped to the workflow or we are allowed to stop consuming.
        if hasattr(self.broker, "close"):
            # If InMemoryBroker, it's fine. If shared RabbitMQ connection, be careful.
            self.broker.close(timeout)

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
        ## AI generated. wasn't extensively tests
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

        Raises:
            ImportError: If graphviz package is not installed when using graphviz format
            ValueError: If invalid format is specified

        Example:
            >>> workflow.visualize("workflow.png", format="graphviz")
            >>> workflow.visualize("workflow.mmd", format="mermaid")
            >>> dot_source = workflow.visualize(format="dot")  # Get DOT source without saving
        """
        if format == "graphviz":
            return self._visualize_graphviz(
                output_path=output_path,
                include_topics=include_topics,
                include_metadata=include_metadata,
                view=view,
                engine=engine,
            )
        elif format == "mermaid":
            return self._visualize_mermaid(
                output_path=output_path,
                include_metadata=include_metadata,
            )
        elif format == "dot":
            return self._generate_dot_source(
                include_topics=include_topics,
                include_metadata=include_metadata,
            )
        else:
            raise ValueError(
                f"Unknown format: {format}. Use 'graphviz', 'mermaid', or 'dot'."
            )

    def _generate_dot_source(
        self, include_topics: bool = True, include_metadata: bool = True
    ) -> str:
        """Generate DOT source code for Graphviz."""
        lines = [
            f'digraph "{self.name}" {{',
            "    rankdir=LR;  // Left to right layout",
            '    node [shape=box, style="rounded,filled", fillcolor=lightblue, fontname="Arial"];',
            '    edge [fontname="Arial", fontsize=10];',
            "",
        ]

        # Add nodes with metadata
        for stage_name, stage in self._stages.items():
            is_entry = (
                self._entry_point_config and self._entry_point_config[0] == stage_name
            )

            # Build label
            label_parts = [stage_name]

            if include_metadata:
                # Add handler type
                handler_type = type(stage.handler).__name__
                label_parts.append(f"Handler: {handler_type}")

                # Add policy type (unwrap routing policy)
                if isinstance(stage.policy, WorkflowRoutingPolicy):
                    inner_policy = type(stage.policy._stage_policy).__name__
                    label_parts.append(f"Policy: {inner_policy}")

                # Add indicators for optional components
                components = []
                if stage.repo:
                    components.append("Repo")
                if stage.task_queue:
                    components.append("Queue")
                if components:
                    label_parts.append(f"Components: {', '.join(components)}")

            label = "\\n".join(label_parts)

            # Style entry points differently
            node_style = 'shape=box, style="rounded,filled,bold"'
            if is_entry:
                node_style += ", fillcolor=lightgreen, penwidth=2"
            else:
                node_style += ", fillcolor=lightblue"

            lines.append(f'    "{stage_name}" [{node_style}, label="{label}"];')

        lines.append("")

        # Add edges
        for src_stage, target_stages in self._adjacency_list.items():
            for target_stage in target_stages:
                edge_label = ""
                if include_topics:
                    topic = f"topic.{src_stage}.{target_stage}"
                    edge_label = f' [label="{topic}"]'

                lines.append(f'    "{src_stage}" -> "{target_stage}"{edge_label};')

        # Add entry point indicator
        if self._entry_point_config:
            stage_name, topic = self._entry_point_config
            lines.append("")
            lines.append("    // Entry point")
            lines.append(
                f'    start [shape=circle, fillcolor=yellow, label="START\\n{topic}"];'
            )
            lines.append(f'    start -> "{stage_name}";')

        lines.append("}")
        return "\n".join(lines)

    def _visualize_graphviz(
        self,
        output_path: str | Path | None = None,
        include_topics: bool = True,
        include_metadata: bool = True,
        view: bool = False,
        engine: str = "dot",
    ) -> str:
        """Generate visualization using Graphviz."""
        try:
            import graphviz
        except ImportError:
            raise ImportError(
                "graphviz package is required for visualization. "
                "Install it with: pip install graphviz"
            )

        # Generate DOT source
        dot_source = self._generate_dot_source(include_topics, include_metadata)

        # Create Graphviz object
        graph = graphviz.Source(dot_source, engine=engine)

        # Handle output
        if output_path:
            output_path = Path(output_path)

            # Determine format from extension
            suffix = output_path.suffix.lower()
            if suffix in [".png", ".svg", ".pdf", ".jpg"]:
                output_format = suffix[1:]  # Remove the dot
                output_file = str(output_path.with_suffix(""))
            else:
                # Default to PNG if no recognized extension
                output_format = "png"
                output_file = str(output_path)

            # Render
            rendered_path = graph.render(
                filename=output_file,
                format=output_format,
                cleanup=True,  # Remove intermediate DOT file
                view=view,
            )

            self.logger.info(f"Workflow visualization saved to: {rendered_path}")
            return rendered_path
        else:
            # Just return the source if no output path
            if view:
                graph.view()
            return dot_source

    def _visualize_mermaid(
        self, output_path: str | Path | None = None, include_metadata: bool = True
    ) -> str:
        """Generate Mermaid diagram source code."""
        lines = [
            "graph LR",
            f"    %% Workflow: {self.name}",
            "",
        ]

        # Define nodes
        for stage_name, stage in self._stages.items():
            is_entry = (
                self._entry_point_config and self._entry_point_config[0] == stage_name
            )

            # Build label
            if include_metadata:
                handler_type = type(stage.handler).__name__
                label = f"{stage_name}<br/><small>{handler_type}</small>"
            else:
                label = stage_name

            # Entry points get special styling
            if is_entry:
                lines.append(f'    {stage_name}["{label}"]')
                lines.append(
                    f"    style {stage_name} fill:#90EE90,stroke:#333,stroke-width:3px"
                )
            else:
                lines.append(f'    {stage_name}["{label}"]')
                lines.append(f"    style {stage_name} fill:#ADD8E6,stroke:#333")

        lines.append("")

        # Add entry point
        if self._entry_point_config:
            stage_name, topic = self._entry_point_config
            lines.append(f"    START((START<br/>{topic}))")
            lines.append("    style START fill:#FFFF99,stroke:#333")
            lines.append(f"    START --> {stage_name}")

        # Add edges
        for src_stage, target_stages in self._adjacency_list.items():
            for target_stage in target_stages:
                topic = f"topic.{src_stage}.{target_stage}"
                lines.append(f"    {src_stage} -->|{topic}| {target_stage}")

        mermaid_source = "\n".join(lines)

        # Save if path provided
        if output_path:
            output_path = Path(output_path)
            output_path.write_text(mermaid_source)
            self.logger.info(f"Mermaid diagram saved to: {output_path}")
            return str(output_path)

        return mermaid_source

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
