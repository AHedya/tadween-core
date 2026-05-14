import threading
from queue import Queue
from unittest.mock import MagicMock

import pytest
from pydantic import BaseModel

from tadween_core.broker import InMemoryBroker
from tadween_core.coord.context import WorkflowContext
from tadween_core.exceptions import ResourceError
from tadween_core.handler.base import BaseHandler
from tadween_core.stage.policy import DefaultStagePolicy
from tadween_core.stage.stage import Stage
from tadween_core.workflow.workflow import RetryPolicy, Workflow, WorkflowRoutingPolicy


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
    def test_add_stage_creates_stage(self, workflow):
        handler = SimpleHandler()
        workflow.add_stage("stage_a", handler=handler)

        assert "stage_a" in workflow._stages

    def test_add_stage_returns_workflow_for_chaining(self, workflow):
        handler = SimpleHandler()
        result = workflow.add_stage("stage_a", handler=handler)

        assert result is workflow

    def test_add_stage_rejects_duplicate_name(self, workflow):
        handler = SimpleHandler()
        workflow.add_stage("stage_a", handler=handler)

        with pytest.raises(ValueError, match="already exists"):
            workflow.add_stage("stage_a", handler=SimpleHandler())

    def test_add_stage_with_custom_policy(self, workflow):
        policy = DefaultStagePolicy[InputModel, OutputModel, dict, None, str]()
        handler = SimpleHandler()
        workflow.add_stage("stage_a", handler=handler, policy=policy)

        assert "stage_a" in workflow._stages

    def test_add_stage_with_retry_policy(self, workflow):
        handler = SimpleHandler()
        workflow.add_stage(
            "stage_retry",
            handler=handler,
            retry_policy=RetryPolicy(retry_on={ValueError}, max_retries=5),
        )

        stage = workflow._stages["stage_retry"]
        assert isinstance(stage.policy, WorkflowRoutingPolicy)
        assert stage.policy.retry_policy is not None
        assert stage.policy.retry_policy.max_retries == 5
        assert ValueError in stage.policy.retry_policy.retry_on


class TestWorkflowLink:
    def test_link_creates_edge(self, workflow):
        workflow.add_stage("a", handler=SimpleHandler())
        workflow.add_stage("b", handler=SimpleHandler())
        workflow.link("a", "b")

        assert "b" in workflow._adjacency_list["a"]

    def test_link_returns_workflow_for_chaining(self, workflow):
        workflow.add_stage("a", handler=SimpleHandler())
        workflow.add_stage("b", handler=SimpleHandler())
        workflow.add_stage("c", handler=SimpleHandler())

        result = workflow.link("a", "b").link("b", "c")

        assert result is workflow

    def test_link_rejects_nonexistent_source(self, workflow):
        workflow.add_stage("b", handler=SimpleHandler())

        with pytest.raises(ValueError, match="Source stage 'a' not found"):
            workflow.link("a", "b")

    def test_link_rejects_nonexistent_target(self, workflow):
        workflow.add_stage("a", handler=SimpleHandler())

        with pytest.raises(ValueError, match="Target stage 'b' not found"):
            workflow.link("a", "b")

    def test_link_prevents_fan_in(self, workflow):
        workflow.add_stage("a", handler=SimpleHandler())
        workflow.add_stage("b", handler=SimpleHandler())
        workflow.add_stage("c", handler=SimpleHandler())
        workflow.link("a", "c")

        with pytest.raises(ValueError, match="Fan-in is not supported"):
            workflow.link("b", "c")

    def test_link_prevents_cycles(self, workflow):
        workflow.add_stage("a", handler=SimpleHandler())
        workflow.add_stage("b", handler=SimpleHandler())
        workflow.add_stage("c", handler=SimpleHandler())
        workflow.link("a", "b")
        workflow.link("b", "c")

        with pytest.raises(ValueError, match="Cycle detected"):
            workflow.link("c", "a")

    def test_link_prevents_cycles_longer_path(self, workflow):
        for name in ["a", "b", "c", "d"]:
            workflow.add_stage(name, handler=SimpleHandler())
        workflow.link("a", "b")
        workflow.link("b", "c")
        workflow.link("c", "d")

        with pytest.raises(ValueError, match="Cycle detected"):
            workflow.link("d", "a")


class TestWorkflowSetEntryPoint:
    def test_set_entry_point(self, workflow):
        workflow.add_stage("start", handler=SimpleHandler())
        workflow.set_entry_point("start")

        assert workflow._entry_point_config == ("start", "workflow.start")

    def test_set_entry_point_with_custom_topic(self, workflow):
        workflow.add_stage("start", handler=SimpleHandler())
        workflow.set_entry_point("start", topic="custom.topic")

        assert workflow._entry_point_config == ("start", "custom.topic")

    def test_set_entry_point_rejects_nonexistent_stage(self, workflow):
        with pytest.raises(ValueError, match="Stage 'nonexistent' not found"):
            workflow.set_entry_point("nonexistent")

    def test_set_entry_point_rejects_stage_with_parents(self, workflow):
        workflow.add_stage("a", handler=SimpleHandler())
        workflow.add_stage("b", handler=SimpleHandler())
        workflow.link("a", "b")

        with pytest.raises(ValueError, match="cannot be an entry point"):
            workflow.set_entry_point("b")


class TestWorkflowBuild:
    def test_build_wires_stages(self, workflow):
        workflow.add_stage("a", handler=SimpleHandler())
        workflow.add_stage("b", handler=SimpleHandler())
        workflow.link("a", "b")
        workflow.set_entry_point("a")
        workflow.build()

        assert workflow._is_built is True

    def test_build_is_idempotent(self, workflow):
        workflow.add_stage("a", handler=SimpleHandler())
        workflow.set_entry_point("a")
        workflow.build()
        workflow.build()

        assert workflow._is_built is True

    def test_build_prevents_modification_after_build(self, workflow):
        workflow.add_stage("a", handler=SimpleHandler())
        workflow.add_stage("b", handler=SimpleHandler())
        workflow.build()

        with pytest.raises(RuntimeError, match="already been built"):
            workflow.link("a", "b")


class TestWorkflowSubmit:
    def test_submit_auto_builds(self, workflow):
        workflow.add_stage("a", handler=PassThroughHandler())
        workflow.set_entry_point("a")

        msg_id = workflow.submit({"value": 5})

        assert msg_id is not None
        assert workflow._is_built is True

    def test_submit_returns_message_id(self, workflow):
        workflow.add_stage("a", handler=SimpleHandler())
        workflow.set_entry_point("a")

        msg_id = workflow.submit({"value": 42})

        assert isinstance(msg_id, str)
        assert len(msg_id) > 0

    def test_submit_with_pydantic_model(self, workflow):
        workflow.add_stage("a", handler=SimpleHandler())
        workflow.set_entry_point("a")

        msg_id = workflow.submit(InputModel(value=100))

        assert msg_id is not None

    def test_submit_raises_without_entry_point(self, workflow):
        workflow.add_stage("a", handler=SimpleHandler())

        with pytest.raises(RuntimeError, match="No entry point configured"):
            workflow.submit({"value": 1})


class TestWorkflowClose:
    def test_close_closes_broker(self):
        broker = InMemoryBroker()
        workflow = Workflow(broker=broker)
        try:
            workflow.add_stage("a", handler=SimpleHandler())
            workflow.close(force=True)

            assert broker._running is False
        finally:
            workflow.close()

    def test_close_closes_stages(self):
        broker = InMemoryBroker()
        workflow = Workflow(broker=broker)
        handler = SimpleHandler()
        workflow.add_stage("a", handler=handler)
        try:
            workflow.close()
            assert workflow._stages["a"].task_queue._closed is True
        finally:
            workflow.close()

    def test_close_cancels_life_length_timer(self):
        broker = InMemoryBroker()
        workflow = Workflow(broker=broker, life_length=10.0)
        try:
            assert workflow._timer is None or workflow._timer.is_alive() is False
        finally:
            workflow.close()


class TestWorkflowWaitForCompletion:
    def test_wait_for_completion_happy_path(self, inmemory_broker):
        wf = Workflow(broker=inmemory_broker)
        # Mock broker join to return immediately
        original_join = inmemory_broker.join
        inmemory_broker.join = MagicMock(return_value=True)

        try:
            wf.wait_for_completion()

            assert inmemory_broker.join.called
            assert wf._is_closed is True
        finally:
            inmemory_broker.join = original_join

    def test_wait_for_completion_keyboard_interrupt(self, inmemory_broker):
        wf = Workflow(broker=inmemory_broker)
        original_join = inmemory_broker.join
        inmemory_broker.join = MagicMock(side_effect=KeyboardInterrupt())

        try:
            with pytest.raises(KeyboardInterrupt):
                wf.wait_for_completion()

            assert wf._is_closed is True
        finally:
            inmemory_broker.join = original_join

    def test_wait_for_completion_system_exit(self, inmemory_broker):
        wf = Workflow(broker=inmemory_broker)
        original_join = inmemory_broker.join
        inmemory_broker.join = MagicMock(side_effect=SystemExit(1))

        try:
            with pytest.raises(SystemExit):
                wf.wait_for_completion()

            assert wf._is_closed is True
        finally:
            inmemory_broker.join = original_join

    def test_wait_for_completion_generic_exception(self, inmemory_broker):
        wf = Workflow(broker=inmemory_broker)
        original_join = inmemory_broker.join
        inmemory_broker.join = MagicMock(side_effect=ValueError("Test Error"))

        try:
            with pytest.raises(ValueError, match="Test Error"):
                wf.wait_for_completion()

            assert wf._is_closed is True
        finally:
            inmemory_broker.join = original_join

    def test_wait_for_completion_no_join_method(self):
        # Mock broker without join method
        mock_broker = MagicMock()
        # Ensure it doesn't have 'join'
        if hasattr(mock_broker, "join"):
            del mock_broker.join

        wf = Workflow(broker=mock_broker)
        wf.wait_for_completion()

        assert wf._is_closed is True

    def test_close_is_idempotent(self, inmemory_broker):
        wf = Workflow(broker=inmemory_broker)
        wf.close()
        assert wf._is_closed is True
        # Calling again should not raise error
        wf.close()
        assert wf._is_closed is True


class TestWorkflowTopologyInfo:
    def test_get_topology_info_single_stage(self, workflow):
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

    def test_get_topology_info_linear_chain(self, workflow):
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


class TestWorkflowIntegrateStage:
    def test_integrate_stage_accepts_pre_built_stage(self, workflow):

        handler = SimpleHandler()
        stage = Stage(handler=handler, name="integrated_stage")
        workflow.integrate_stage("integrated", stage)

        assert "integrated" in workflow._stages

    def test_integrate_stage_wraps_policy_with_routing(self, workflow):

        handler = SimpleHandler()
        stage = Stage(handler=handler, name="wrapped_stage")
        workflow.integrate_stage("wrapped", stage)

        assert isinstance(workflow._stages["wrapped"].policy, WorkflowRoutingPolicy)


class TestWorkflowCycleDetection:
    def test_detects_direct_cycle(self, workflow):
        workflow.add_stage("a", handler=SimpleHandler())

        with pytest.raises(ValueError, match="Cycle detected"):
            workflow.link("a", "a")

    def test_detects_indirect_cycle(self, workflow):
        workflow.add_stage("a", handler=SimpleHandler())
        workflow.add_stage("b", handler=SimpleHandler())
        workflow.add_stage("c", handler=SimpleHandler())
        workflow.link("a", "b")
        workflow.link("b", "c")

        with pytest.raises(ValueError, match="Cycle detected"):
            workflow.link("c", "a")

    def test_allows_valid_dag(self, workflow):
        workflow.add_stage("a", handler=SimpleHandler())
        workflow.add_stage("b", handler=SimpleHandler())
        workflow.add_stage("c", handler=SimpleHandler())
        workflow.link("a", "b")
        workflow.link("a", "c")

        assert "b" in workflow._adjacency_list["a"]
        assert "c" in workflow._adjacency_list["a"]


class TestWorkflowVisualize:
    def test_visualize_returns_mermaid_source(self, workflow):
        workflow.add_stage("a", handler=SimpleHandler())
        workflow.add_stage("b", handler=SimpleHandler())
        workflow.link("a", "b")
        workflow.set_entry_point("a")

        mermaid = workflow.visualize(format="mermaid")

        assert "graph LR" in mermaid
        assert "a" in mermaid
        assert "b" in mermaid

    def test_visualize_returns_dot_source(self, workflow):
        workflow.add_stage("a", handler=SimpleHandler())

        dot = workflow.visualize(format="dot")

        assert "digraph" in dot
        assert "a" in dot

    def test_visualize_invalid_format_raises(self, workflow):
        workflow.add_stage("a", handler=SimpleHandler())

        with pytest.raises(ValueError, match="Unknown format"):
            workflow.visualize(format="invalid")


class TestWorkflowLifeLength:
    def test_workflow_with_life_length_sets_timer(self, inmemory_broker):
        workflow = Workflow(broker=inmemory_broker, life_length=60.0)
        try:
            workflow.add_stage("a", handler=SimpleHandler())
            workflow.set_entry_point("a")
            workflow.build()

            assert workflow._timer is not None
        finally:
            workflow.close()

    def test_close_shutdowns_resource_manager(self, inmemory_broker):
        workflow = Workflow(broker=inmemory_broker, resources={"cuda": 1})
        workflow.add_stage("a", handler=SimpleHandler(), demands={"cuda": 1})
        workflow.set_entry_point("a")
        workflow.build()
        workflow.close()

        assert workflow.resource_manager.is_shutdown

    def test_close_unblocks_collector_waiting_on_context(self, inmemory_broker):
        workflow = Workflow(broker=inmemory_broker)
        workflow.context.increment("slot", 1)

        failed = Queue()
        acquired = 0

        def acquire_resource():
            nonlocal acquired
            try:
                workflow.context.wait_for(
                    "slot-free",
                    lambda ctx, _: ctx.state.get("slot", 0) <= 0,
                    poll_interval=None,
                )
                workflow.context.decrement("slot", 1)
                acquired += 1

            except Exception:
                failed.put(1)

        threads = [
            threading.Thread(target=acquire_resource, daemon=True) for i in range(3)
        ]

        for t in threads:
            t.start()

        workflow.context.notify("slot-free")
        workflow.close()
        for t in threads:
            t.join()

        assert workflow.context.is_shutdown
        f = 0
        while not failed.empty():
            f += failed.get()
        assert f == 2
        assert acquired == 1

    def test_close_unblocks_collector_waiting_on_resources(self, inmemory_broker):

        workflow = Workflow(broker=inmemory_broker, resources={"gpu": 1})
        failed = Queue()
        acquired = 0

        def acquire_resource():
            nonlocal acquired
            try:
                workflow.resource_manager.acquire({"gpu": 1})
                acquired += 1
            except ResourceError:
                failed.put(1)

        threads = [
            threading.Thread(target=acquire_resource, daemon=True) for i in range(3)
        ]

        for t in threads:
            t.start()
        workflow.close()
        for t in threads:
            t.join()

        assert workflow.resource_manager.is_shutdown
        f = 0
        while not failed.empty():
            f += failed.get()
        assert f == 2
        assert acquired == 1

    def test_close_topological_robustness(self, inmemory_broker):
        """
        Verify that shutting down stages doesn't cause errors in a DAG
        even if they are closed in arbitrary order.
        """
        workflow = Workflow(
            broker=inmemory_broker, default_payload_extractor=lambda x: None
        )

        # A -> B
        workflow.add_stage("a", handler=SimpleHandler())
        workflow.add_stage("b", handler=SimpleHandler())
        workflow.link("a", "b")
        workflow.set_entry_point("a")
        workflow.build()

        # Flood with messages
        for i in range(10):
            workflow.submit({"value": i})

        # Close immediately - should not raise errors even if A tries to send to B during B's shutdown
        workflow.close(timeout=5.0)
        assert True


class FlakyInputModel(BaseModel):
    artifact_id: str
    value: int


class FlakyOutputModel(BaseModel):
    artifact_id: str
    result: str


class FlakyHandler(BaseHandler[FlakyInputModel, FlakyOutputModel]):
    def __init__(self, fails_before_success: int):
        self.fails_before_success = fails_before_success
        self.attempts = 0

    def run(self, inputs: FlakyInputModel) -> FlakyOutputModel:
        self.attempts += 1
        if self.attempts <= self.fails_before_success:
            raise ValueError(f"Failed at attempt {self.attempts}")
        return FlakyOutputModel(
            artifact_id=inputs.artifact_id, result=f"success_{inputs.value}"
        )


class TestRetryMechanism:
    def test_retry_happy_path(self, inmemory_broker):
        context = WorkflowContext()
        wf = Workflow(
            broker=inmemory_broker,
            context=context,
            default_payload_extractor=lambda x: x,
        )

        # Needs 2 failures, succeeds on 3rd attempt
        flaky_handler = FlakyHandler(fails_before_success=2)
        wf.add_stage(
            "flaky",
            handler=flaky_handler,
            retry_policy=RetryPolicy(retry_on={ValueError}, max_retries=3),
        )
        wf.set_entry_point("flaky")
        wf.build()

        artifact_id = "art-happy"
        wf.submit(
            FlakyInputModel(artifact_id=artifact_id, value=10),
            metadata={"artifact_id": artifact_id},
        )

        wf._stages["flaky"].wait_all(timeout=5)
        inmemory_broker.join(timeout=5)

        assert flaky_handler.attempts == 3
        assert context.state_get(f"_artifacts:{artifact_id}") == 0

    def test_retry_terminal_failure(self, inmemory_broker):
        context = WorkflowContext()
        wf = Workflow(
            broker=inmemory_broker,
            context=context,
            default_payload_extractor=lambda x: None,
        )

        # Needs 5 failures, succeeds on 6th attempt. But max_retries is 2.
        # So it will terminally fail on 3rd attempt.
        flaky_handler = FlakyHandler(fails_before_success=5)
        wf.add_stage(
            "flaky",
            handler=flaky_handler,
            retry_policy=RetryPolicy(retry_on={ValueError}, max_retries=2),
        )
        wf.set_entry_point("flaky")
        wf.build()

        artifact_id = "art-terminal"
        wf.submit(
            FlakyInputModel(artifact_id=artifact_id, value=20),
            metadata={"artifact_id": artifact_id},
        )

        wf._stages["flaky"].wait_all(timeout=5)
        inmemory_broker.join(timeout=5)

        assert flaky_handler.attempts == 3
        assert context.state_get(f"_artifacts:{artifact_id}") == 0
