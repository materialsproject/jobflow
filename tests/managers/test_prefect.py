"""Tests for the Prefect manager."""

import pytest
from unittest.mock import patch, MagicMock

import jobflow
from jobflow import job, Job, Flow


@job
def add(a, b):
    """Simple job to add two numbers."""
    return a + b


@job
def multiply(a, b):
    """Simple job to multiply two numbers."""
    return a * b


class TestPrefectManager:
    """Test the PrefectManager class."""

    def test_import_without_prefect(self):
        """Test that imports work even without Prefect installed."""
        with patch.dict('sys.modules', {'prefect': None}):
            # This should not raise an error
            from jobflow.managers import prefect as prefect_manager
            assert prefect_manager.PREFECT_AVAILABLE is False

    def test_prefect_manager_init_without_prefect(self):
        """Test PrefectManager initialization without Prefect."""
        with patch('jobflow.managers.prefect.PREFECT_AVAILABLE', False):
            with pytest.raises(ImportError, match="Prefect is not installed"):
                from jobflow.managers.prefect import PrefectManager
                PrefectManager()

    @pytest.mark.parametrize("prefect_available", [True, False])
    def test_flow_to_prefect_flow_availability(self, prefect_available):
        """Test flow_to_prefect_flow with and without Prefect."""
        with patch('jobflow.managers.prefect.PREFECT_AVAILABLE', prefect_available):
            from jobflow.managers.prefect import flow_to_prefect_flow
            
            # Create a simple job
            simple_job = add(1, 2)
            
            if prefect_available:
                # Mock Prefect components
                with patch('jobflow.managers.prefect.flow') as mock_flow, \
                     patch('jobflow.managers.prefect.ConcurrentTaskRunner'), \
                     patch('jobflow.managers.prefect.get_flow') as mock_get_flow:
                    
                    mock_get_flow.return_value = MagicMock()
                    mock_get_flow.return_value.iterflow.return_value = [(simple_job, [])]
                    mock_get_flow.return_value.name = "test_flow"
                    
                    result = flow_to_prefect_flow(simple_job)
                    assert result is not None
                    mock_flow.assert_called()
            else:
                with pytest.raises(ImportError, match="Prefect is not installed"):
                    flow_to_prefect_flow(simple_job)

    def test_single_job_conversion(self):
        """Test converting a single job to Prefect flow."""
        with patch('jobflow.managers.prefect.PREFECT_AVAILABLE', True), \
             patch('jobflow.managers.prefect.flow') as mock_flow, \
             patch('jobflow.managers.prefect.task') as mock_task, \
             patch('jobflow.managers.prefect.ConcurrentTaskRunner'), \
             patch('jobflow.managers.prefect.get_flow') as mock_get_flow:
            
            from jobflow.managers.prefect import flow_to_prefect_flow
            
            # Create a simple job
            simple_job = add(1, 2)
            
            # Mock the flow object
            mock_flow_obj = MagicMock()
            mock_flow_obj.iterflow.return_value = [(simple_job, [])]
            mock_flow_obj.name = "test_flow"
            mock_get_flow.return_value = mock_flow_obj
            
            # Test conversion
            result = flow_to_prefect_flow(simple_job)
            
            # Verify flow decorator was called
            mock_flow.assert_called()
            call_kwargs = mock_flow.call_args[1]
            assert call_kwargs['name'] == 'test_flow'
            assert call_kwargs['log_prints'] is True

    def test_flow_with_dependencies(self):
        """Test converting a flow with job dependencies."""
        with patch('jobflow.managers.prefect.PREFECT_AVAILABLE', True), \
             patch('jobflow.managers.prefect.flow') as mock_flow, \
             patch('jobflow.managers.prefect.task') as mock_task, \
             patch('jobflow.managers.prefect.ConcurrentTaskRunner'), \
             patch('jobflow.managers.prefect.get_flow') as mock_get_flow:
            
            from jobflow.managers.prefect import flow_to_prefect_flow
            
            # Create jobs with dependencies
            job1 = add(1, 2)
            job2 = multiply(job1.output, 3)
            flow_obj = Flow([job1, job2])
            
            # Mock the flow object
            mock_flow_obj = MagicMock()
            mock_flow_obj.iterflow.return_value = [
                (job1, []),
                (job2, [job1.uuid])
            ]
            mock_flow_obj.name = "dependency_flow"
            mock_get_flow.return_value = mock_flow_obj
            
            # Test conversion
            result = flow_to_prefect_flow(flow_obj)
            
            # Verify flow decorator was called
            mock_flow.assert_called()

    def test_job_to_prefect_task(self):
        """Test converting a single job to Prefect task."""
        with patch('jobflow.managers.prefect.PREFECT_AVAILABLE', True), \
             patch('jobflow.managers.prefect.task') as mock_task:
            
            from jobflow.managers.prefect import job_to_prefect_task
            
            # Create a simple job
            simple_job = add(1, 2)
            
            # Test conversion
            result = job_to_prefect_task(simple_job)
            
            # Verify task decorator was called
            mock_task.assert_called()
            call_kwargs = mock_task.call_args[1]
            assert 'name' in call_kwargs
            assert call_kwargs['log_prints'] is True

    def test_run_on_prefect_without_prefect(self):
        """Test run_on_prefect without Prefect installed."""
        with patch('jobflow.managers.prefect.PREFECT_AVAILABLE', False):
            from jobflow.managers.prefect import run_on_prefect
            
            simple_job = add(1, 2)
            with pytest.raises(ImportError, match="Prefect is not installed"):
                run_on_prefect(simple_job)

    def test_run_on_prefect_with_prefect(self):
        """Test run_on_prefect with Prefect available."""
        with patch('jobflow.managers.prefect.PREFECT_AVAILABLE', True), \
             patch('jobflow.managers.prefect.flow_to_prefect_flow') as mock_convert:
            
            from jobflow.managers.prefect import run_on_prefect
            
            # Mock the converted flow
            mock_flow_func = MagicMock()
            mock_flow_func.return_value = {"result": "success"}
            mock_convert.return_value = mock_flow_func
            
            simple_job = add(1, 2)
            
            # Test execution
            result = run_on_prefect(simple_job)
            
            # Verify conversion and execution
            mock_convert.assert_called_once()
            mock_flow_func.assert_called_once()
            assert result == {"result": "success"}


class TestPrefectManagerIntegration:
    """Integration tests for PrefectManager."""

    def test_manager_creation_and_flow_submission(self):
        """Test creating manager and submitting a flow."""
        with patch('jobflow.managers.prefect.PREFECT_AVAILABLE', True), \
             patch('jobflow.managers.prefect.ConcurrentTaskRunner'), \
             patch('jobflow.managers.prefect.flow_to_prefect_flow') as mock_convert:
            
            from jobflow.managers.prefect import PrefectManager
            
            # Mock the converted flow
            mock_flow_func = MagicMock()
            mock_flow_func.return_value = MagicMock()
            mock_convert.return_value = mock_flow_func
            
            # Create manager
            manager = PrefectManager(task_runner="sequential")
            
            # Create and submit a job
            simple_job = add(1, 2)
            
            # Use asyncio.run for the async method in a test
            import asyncio
            result = asyncio.run(manager.submit_flow(simple_job, flow_name="test_flow"))
            
            # Verify conversion was called
            mock_convert.assert_called_once()
            mock_flow_func.assert_called_once()

    def test_deployment_creation(self):
        """Test creating a Prefect deployment."""
        with patch('jobflow.managers.prefect.PREFECT_AVAILABLE', True), \
             patch('jobflow.managers.prefect.ConcurrentTaskRunner'), \
             patch('jobflow.managers.prefect.flow_to_prefect_flow') as mock_convert, \
             patch('jobflow.managers.prefect.Deployment') as mock_deployment:
            
            from jobflow.managers.prefect import PrefectManager
            
            # Mock the converted flow and deployment
            mock_flow_func = MagicMock()
            mock_convert.return_value = mock_flow_func
            mock_deployment_obj = MagicMock()
            mock_deployment.build_from_flow.return_value = mock_deployment_obj
            
            # Create manager
            manager = PrefectManager()
            
            # Create deployment
            simple_job = add(1, 2)
            deployment = manager.create_deployment(
                simple_job,
                deployment_name="test_deployment",
                work_pool_name="test_pool"
            )
            
            # Verify deployment creation
            mock_convert.assert_called_once()
            mock_deployment.build_from_flow.assert_called_once()
            assert deployment == mock_deployment_obj


class TestPrefectManagerEdgeCases:
    """Test edge cases and error handling."""

    def test_invalid_task_runner(self):
        """Test with invalid task runner."""
        with patch('jobflow.managers.prefect.PREFECT_AVAILABLE', True), \
             patch('jobflow.managers.prefect.ConcurrentTaskRunner') as mock_concurrent, \
             patch('jobflow.managers.prefect.flow') as mock_flow, \
             patch('jobflow.managers.prefect.get_flow') as mock_get_flow:
            
            from jobflow.managers.prefect import flow_to_prefect_flow
            
            # Mock flow object
            mock_flow_obj = MagicMock()
            mock_flow_obj.iterflow.return_value = []
            mock_flow_obj.name = "test"
            mock_get_flow.return_value = mock_flow_obj
            
            simple_job = add(1, 2)
            
            # Test with invalid task runner - should default to concurrent
            result = flow_to_prefect_flow(simple_job, task_runner="invalid")
            
            # Should use ConcurrentTaskRunner as default
            mock_concurrent.assert_called()

    def test_empty_flow(self):
        """Test with an empty flow."""
        with patch('jobflow.managers.prefect.PREFECT_AVAILABLE', True), \
             patch('jobflow.managers.prefect.flow') as mock_flow, \
             patch('jobflow.managers.prefect.ConcurrentTaskRunner'), \
             patch('jobflow.managers.prefect.get_flow') as mock_get_flow:
            
            from jobflow.managers.prefect import flow_to_prefect_flow
            
            # Mock empty flow
            mock_flow_obj = MagicMock()
            mock_flow_obj.iterflow.return_value = []  # Empty iterator
            mock_flow_obj.name = "empty_flow"
            mock_get_flow.return_value = mock_flow_obj
            
            empty_flow = Flow([])
            
            # Should handle empty flow gracefully
            result = flow_to_prefect_flow(empty_flow)
            
            mock_flow.assert_called()
            assert result is not None