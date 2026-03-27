import pytest
from pydantic import BaseModel

from tadween_core.broker import InMemoryBroker
from tadween_core.handler.base import BaseHandler
from tadween_core.stage.policy import DefaultStagePolicy
from tadween_core.workflow.workflow import Workflow


class InputModel(BaseModel):
    value: int


class OutputModel(BaseModel):
    result: str


class SimpleHandler(BaseHandler[InputModel, OutputModel]):
    def run(self, inputs: InputModel) -> OutputModel:
        return OutputModel(result=f"output_{inputs.value}")


class PassThroughHandler(BaseHandler[InputModel, InputModel]):
    def run(self, inputs: InputModel) -> InputModel:
        return InputModel(value=inputs.value + 10)


class TestWorkflowAddStage:
    def test_add_stage_creates_stage(self):
        broker = InMemoryBroker()
        workflow = Workflow(broker=broker, name="TestWorkflow")

        handler = SimpleHandler()
        workflow.add_stage("stage_a", handler=handler)

        assert "stage_a" in workflow._stages
        broker.close(timeout=1.0)

    def test_add_stage_returns_workflow_for_chaining(self):
        broker = InMemoryBroker()
        workflow = Workflow(broker=broker)

        handler = SimpleHandler()
        result = workflow.add_stage("stage_a", handler=handler)

        assert result is workflow
        broker.close(timeout=1.0)

    def test_add_stage_rejects_duplicate_name(self):
        broker = InMemoryBroker()
        workflow = Workflow(broker=broker)

        handler = SimpleHandler()
        workflow.add_stage("stage_a", handler=handler)

        with pytest.raises(ValueError, match="already exists"):
            workflow.add_stage("stage_a", handler=SimpleHandler())

        broker.close(timeout=1.0)

    def test_add_stage_with_custom_policy(self):
        broker = InMemoryBroker()
        workflow = Workflow(broker=broker)

        policy = DefaultStagePolicy[InputModel, OutputModel, dict, None, str]()
        handler = SimpleHandler()
        workflow.add_stage("stage_a", handler=handler, policy=policy)

        assert "stage_a" in workflow._stages
        broker.close(timeout=1.0)


class TestWorkflowLink:
    def test_link_creates_edge(self):
        broker = InMemoryBroker()
        workflow = Workflow(broker=broker)

        workflow.add_stage("a", handler=SimpleHandler())
        workflow.add_stage("b", handler=SimpleHandler())

        workflow.link("a", "b")

        assert "b" in workflow._adjacency_list["a"]
        broker.close(timeout=1.0)

    def test_link_returns_workflow_for_chaining(self):
        broker = InMemoryBroker()
        workflow = Workflow(broker=broker)

        workflow.add_stage("a", handler=SimpleHandler())
        workflow.add_stage("b", handler=SimpleHandler())
        workflow.add_stage("c", handler=SimpleHandler())

        result = workflow.link("a", "b").link("b", "c")

        assert result is workflow
        broker.close(timeout=1.0)

    def test_link_rejects_nonexistent_source(self):
        broker = InMemoryBroker()
        workflow = Workflow(broker=broker)

        workflow.add_stage("b", handler=SimpleHandler())

        with pytest.raises(ValueError, match="Source stage 'a' not found"):
            workflow.link("a", "b")

        broker.close(timeout=1.0)

    def test_link_rejects_nonexistent_target(self):
        broker = InMemoryBroker()
        workflow = Workflow(broker=broker)

        workflow.add_stage("a", handler=SimpleHandler())

        with pytest.raises(ValueError, match="Target stage 'b' not found"):
            workflow.link("a", "b")

        broker.close(timeout=1.0)

    def test_link_prevents_fan_in(self):
        broker = InMemoryBroker()
        workflow = Workflow(broker=broker)

        workflow.add_stage("a", handler=SimpleHandler())
        workflow.add_stage("b", handler=SimpleHandler())
        workflow.add_stage("c", handler=SimpleHandler())

        workflow.link("a", "c")

        with pytest.raises(ValueError, match="Fan-in is not supported"):
            workflow.link("b", "c")

        broker.close(timeout=1.0)

    def test_link_prevents_cycles(self):
        broker = InMemoryBroker()
        workflow = Workflow(broker=broker)

        workflow.add_stage("a", handler=SimpleHandler())
        workflow.add_stage("b", handler=SimpleHandler())
        workflow.add_stage("c", handler=SimpleHandler())

        workflow.link("a", "b")
        workflow.link("b", "c")

        with pytest.raises(ValueError, match="Cycle detected"):
            workflow.link("c", "a")

        broker.close(timeout=1.0)

    def test_link_prevents_cycles_longer_path(self):
        broker = InMemoryBroker()
        workflow = Workflow(broker=broker)

        for name in ["a", "b", "c", "d"]:
            workflow.add_stage(name, handler=SimpleHandler())

        workflow.link("a", "b")
        workflow.link("b", "c")
        workflow.link("c", "d")

        with pytest.raises(ValueError, match="Cycle detected"):
            workflow.link("d", "a")

        broker.close(timeout=1.0)


class TestWorkflowSetEntryPoint:
    def test_set_entry_point(self):
        broker = InMemoryBroker()
        workflow = Workflow(broker=broker)

        workflow.add_stage("start", handler=SimpleHandler())
        workflow.set_entry_point("start")

        assert workflow._entry_point_config == ("start", "workflow.start")
        broker.close(timeout=1.0)

    def test_set_entry_point_with_custom_topic(self):
        broker = InMemoryBroker()
        workflow = Workflow(broker=broker)

        workflow.add_stage("start", handler=SimpleHandler())
        workflow.set_entry_point("start", topic="custom.topic")

        assert workflow._entry_point_config == ("start", "custom.topic")
        broker.close(timeout=1.0)

    def test_set_entry_point_rejects_nonexistent_stage(self):
        broker = InMemoryBroker()
        workflow = Workflow(broker=broker)

        with pytest.raises(ValueError, match="Stage 'nonexistent' not found"):
            workflow.set_entry_point("nonexistent")

        broker.close(timeout=1.0)

    def test_set_entry_point_rejects_stage_with_parents(self):
        broker = InMemoryBroker()
        workflow = Workflow(broker=broker)

        workflow.add_stage("a", handler=SimpleHandler())
        workflow.add_stage("b", handler=SimpleHandler())
        workflow.link("a", "b")

        with pytest.raises(ValueError, match="cannot be an entry point"):
            workflow.set_entry_point("b")

        broker.close(timeout=1.0)


class TestWorkflowBuild:
    def test_build_wires_stages(self):
        broker = InMemoryBroker()
        workflow = Workflow(broker=broker)

        workflow.add_stage("a", handler=SimpleHandler())
        workflow.add_stage("b", handler=SimpleHandler())
        workflow.link("a", "b")
        workflow.set_entry_point("a")

        workflow.build()

        assert workflow._is_built is True
        broker.close(timeout=1.0)

    def test_build_is_idempotent(self):
        broker = InMemoryBroker()
        workflow = Workflow(broker=broker)

        workflow.add_stage("a", handler=SimpleHandler())
        workflow.set_entry_point("a")

        workflow.build()
        workflow.build()

        assert workflow._is_built is True
        broker.close(timeout=1.0)

    def test_build_prevents_modification_after_build(self):
        broker = InMemoryBroker()
        workflow = Workflow(broker=broker)

        workflow.add_stage("a", handler=SimpleHandler())
        workflow.add_stage("b", handler=SimpleHandler())
        workflow.build()

        with pytest.raises(RuntimeError, match="already been built"):
            workflow.link("a", "b")

        broker.close(timeout=1.0)


class TestWorkflowSubmit:
    def test_submit_auto_builds(self):
        broker = InMemoryBroker()
        workflow = Workflow(broker=broker)

        workflow.add_stage("a", handler=PassThroughHandler())
        workflow.set_entry_point("a")

        msg_id = workflow.submit({"value": 5})

        assert msg_id is not None
        assert workflow._is_built is True
        broker.close(timeout=1.0)

    def test_submit_returns_message_id(self):
        broker = InMemoryBroker()
        workflow = Workflow(broker=broker)

        workflow.add_stage("a", handler=SimpleHandler())
        workflow.set_entry_point("a")

        msg_id = workflow.submit({"value": 42})

        assert isinstance(msg_id, str)
        assert len(msg_id) > 0
        broker.close(timeout=1.0)

    def test_submit_with_pydantic_model(self):
        broker = InMemoryBroker()
        workflow = Workflow(broker=broker)

        workflow.add_stage("a", handler=SimpleHandler())
        workflow.set_entry_point("a")

        msg_id = workflow.submit(InputModel(value=100))

        assert msg_id is not None
        broker.close(timeout=1.0)

    def test_submit_raises_without_entry_point(self):
        broker = InMemoryBroker()
        workflow = Workflow(broker=broker)

        workflow.add_stage("a", handler=SimpleHandler())

        with pytest.raises(RuntimeError, match="No entry point configured"):
            workflow.submit({"value": 1})

        broker.close(timeout=1.0)


class TestWorkflowClose:
    def test_close_closes_broker(self):
        broker = InMemoryBroker()
        workflow = Workflow(broker=broker)

        workflow.add_stage("a", handler=SimpleHandler())
        workflow.close()

        assert broker._running is False

    def test_close_closes_stages(self):
        broker = InMemoryBroker()
        workflow = Workflow(broker=broker)

        handler = SimpleHandler()
        workflow.add_stage("a", handler=handler)

        workflow.close()

        assert workflow._stages["a"].task_queue._closed is True
        broker.close(timeout=0.1)

    def test_close_cancels_life_length_timer(self):
        broker = InMemoryBroker()
        workflow = Workflow(broker=broker, life_length=10.0)

        workflow.close()

        assert workflow._timer is None or workflow._timer.is_alive() is False
        broker.close(timeout=0.1)


class TestWorkflowTopologyInfo:
    def test_get_topology_info_single_stage(self):
        broker = InMemoryBroker()
        workflow = Workflow(broker=broker)

        workflow.add_stage("start", handler=SimpleHandler())
        workflow.set_entry_point("start")
        workflow.build()

        info = workflow.get_topology_info()

        assert info["name"] == workflow.name
        assert info["num_stages"] == 1
        assert info["stages"] == ["start"]
        assert info["edges"] == []
        assert info["entry_point"] == ("start", "workflow.start")
        assert info["is_built"] is True
        assert info["leaf_stages"] == ["start"]
        broker.close(timeout=1.0)

    def test_get_topology_info_linear_chain(self):
        broker = InMemoryBroker()
        workflow = Workflow(broker=broker)

        workflow.add_stage("a", handler=SimpleHandler())
        workflow.add_stage("b", handler=SimpleHandler())
        workflow.add_stage("c", handler=SimpleHandler())

        workflow.link("a", "b")
        workflow.link("b", "c")
        workflow.set_entry_point("a")
        workflow.build()

        info = workflow.get_topology_info()

        assert info["num_stages"] == 3
        assert info["edges"] == [("a", "b"), ("b", "c")]
        assert set(info["stages"]) == {"a", "b", "c"}
        assert info["leaf_stages"] == ["c"]
        broker.close(timeout=1.0)


class TestWorkflowIntegrateStage:
    def test_integrate_stage_accepts_pre_built_stage(self):
        from tadween_core.stage.stage import Stage

        broker = InMemoryBroker()
        workflow = Workflow(broker=broker)

        handler = SimpleHandler()
        stage = Stage(handler=handler, name="integrated_stage")
        workflow.integrate_stage("integrated", stage)

        assert "integrated" in workflow._stages
        broker.close(timeout=1.0)

    def test_integrate_stage_wraps_policy_with_routing(self):
        from tadween_core.stage.stage import Stage
        from tadween_core.workflow.router import WorkflowRoutingPolicy

        broker = InMemoryBroker()
        workflow = Workflow(broker=broker)

        handler = SimpleHandler()
        stage = Stage(handler=handler, name="wrapped_stage")
        workflow.integrate_stage("wrapped", stage)

        assert isinstance(workflow._stages["wrapped"].policy, WorkflowRoutingPolicy)
        broker.close(timeout=1.0)


class TestWorkflowCycleDetection:
    def test_detects_direct_cycle(self):
        broker = InMemoryBroker()
        workflow = Workflow(broker=broker)

        workflow.add_stage("a", handler=SimpleHandler())

        with pytest.raises(ValueError, match="Cycle detected"):
            workflow.link("a", "a")

        broker.close(timeout=1.0)

    def test_detects_indirect_cycle(self):
        broker = InMemoryBroker()
        workflow = Workflow(broker=broker)

        workflow.add_stage("a", handler=SimpleHandler())
        workflow.add_stage("b", handler=SimpleHandler())
        workflow.add_stage("c", handler=SimpleHandler())

        workflow.link("a", "b")
        workflow.link("b", "c")

        with pytest.raises(ValueError, match="Cycle detected"):
            workflow.link("c", "a")

        broker.close(timeout=1.0)

    def test_allows_valid_dag(self):
        broker = InMemoryBroker()
        workflow = Workflow(broker=broker)

        workflow.add_stage("a", handler=SimpleHandler())
        workflow.add_stage("b", handler=SimpleHandler())
        workflow.add_stage("c", handler=SimpleHandler())

        workflow.link("a", "b")
        workflow.link("a", "c")

        assert "b" in workflow._adjacency_list["a"]
        assert "c" in workflow._adjacency_list["a"]
        broker.close(timeout=1.0)


class TestWorkflowVisualize:
    def test_visualize_returns_mermaid_source(self):
        broker = InMemoryBroker()
        workflow = Workflow(broker=broker)

        workflow.add_stage("a", handler=SimpleHandler())
        workflow.add_stage("b", handler=SimpleHandler())
        workflow.link("a", "b")
        workflow.set_entry_point("a")

        mermaid = workflow.visualize(format="mermaid")

        assert "graph LR" in mermaid
        assert "a" in mermaid
        assert "b" in mermaid
        broker.close(timeout=1.0)

    def test_visualize_returns_dot_source(self):
        broker = InMemoryBroker()
        workflow = Workflow(broker=broker)

        workflow.add_stage("a", handler=SimpleHandler())

        dot = workflow.visualize(format="dot")

        assert "digraph" in dot
        assert "a" in dot
        broker.close(timeout=1.0)

    def test_visualize_invalid_format_raises(self):
        broker = InMemoryBroker()
        workflow = Workflow(broker=broker)

        workflow.add_stage("a", handler=SimpleHandler())

        with pytest.raises(ValueError, match="Unknown format"):
            workflow.visualize(format="invalid")

        broker.close(timeout=1.0)


class TestWorkflowLifeLength:
    def test_workflow_with_life_length_sets_timer(self):
        broker = InMemoryBroker()
        workflow = Workflow(broker=broker, life_length=60.0)

        workflow.add_stage("a", handler=SimpleHandler())
        workflow.set_entry_point("a")
        workflow.build()

        assert workflow._timer is not None
        workflow.close()

        broker.close(timeout=1.0)
