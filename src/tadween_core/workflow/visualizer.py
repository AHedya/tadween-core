import logging
from pathlib import Path
from typing import TYPE_CHECKING, Literal

from tadween_core.workflow.router import WorkflowRoutingPolicy

if TYPE_CHECKING:
    from .workflow import Workflow


class WorkflowVisualizer:
    """
    Handles visualization logic for Workflow DAGs.
    Decouples visualization from core orchestration logic.
    """

    def __init__(self, workflow: "Workflow"):
        self.workflow = workflow
        self.logger = logging.getLogger(f"tadween.workflow.visualizer.{workflow.name}")

    def render(
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
            output_path: Where to save the output file.
            format: Output format - 'graphviz', 'mermaid', or 'dot'.
            include_topics: Whether to show topic names on edges.
            include_metadata: Whether to include handler/policy info in nodes.
            view: Whether to automatically open/display the visualization.
            engine: Graphviz layout engine.

        Returns:
            The generated source code as a string, or path to saved file.
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
            f'digraph "{self.workflow.name}" {{',
            "    rankdir=LR;  // Left to right layout",
            '    node [shape=box, style="rounded,filled", fillcolor=lightblue, fontname="Arial"];',
            '    edge [fontname="Arial", fontsize=10];',
            "",
        ]

        # Add nodes with metadata
        for stage_name, stage in self.workflow._stages.items():
            is_entry = (
                self.workflow._entry_point_config
                and self.workflow._entry_point_config[0] == stage_name
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
        for src_stage, target_stages in self.workflow._adjacency_list.items():
            for target_stage in target_stages:
                edge_label = ""
                if include_topics:
                    topic = f"topic.{src_stage}.{target_stage}"
                    edge_label = f' [label="{topic}"]'

                lines.append(f'    "{src_stage}" -> "{target_stage}"{edge_label};')

        # Add entry point indicator
        if self.workflow._entry_point_config:
            stage_name, topic = self.workflow._entry_point_config
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
            f"    %% Workflow: {self.workflow.name}",
            "",
        ]

        # Define nodes
        for stage_name, stage in self.workflow._stages.items():
            is_entry = (
                self.workflow._entry_point_config
                and self.workflow._entry_point_config[0] == stage_name
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
        if self.workflow._entry_point_config:
            stage_name, topic = self.workflow._entry_point_config
            lines.append(f"    START((START<br/>{topic}))")
            lines.append("    style START fill:#FFFF99,stroke:#333")
            lines.append(f"    START --> {stage_name}")

        # Add edges
        for src_stage, target_stages in self.workflow._adjacency_list.items():
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
