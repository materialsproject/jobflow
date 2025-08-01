"""
Example demonstrating how to use jobflow with Prefect.

This example shows how to:
1. Create jobflow Jobs and Flows
2. Convert them to Prefect workflows
3. Run them locally using Prefect
4. Create deployments for a Prefect cluster
"""

import jobflow
from jobflow import job, Job, Flow
from jobflow.managers.prefect import PrefectManager, run_on_prefect, flow_to_prefect_flow


# Define some simple jobs
@job
def add(a: int, b: int) -> int:
    """Add two numbers."""
    print(f"Adding {a} + {b}")
    return a + b


@job
def multiply(a: int, b: int) -> int:
    """Multiply two numbers."""
    print(f"Multiplying {a} * {b}")
    return a * b


@job
def subtract(a: int, b: int) -> int:
    """Subtract two numbers."""
    print(f"Subtracting {a} - {b}")
    return a - b


def example_single_job():
    """Example of running a single job with Prefect."""
    print("\n=== Single Job Example ===")
    
    # Create a simple job
    job1 = add(5, 3)
    
    try:
        # Run using Prefect
        result = run_on_prefect(job1, flow_name="single_add_job")
        print(f"Result: {result}")
    except ImportError:
        print("Prefect is not installed. Please install with: pip install prefect")
    except Exception as e:
        print(f"Error running job: {e}")


def example_simple_flow():
    """Example of running a simple flow with Prefect."""
    print("\n=== Simple Flow Example ===")
    
    # Create jobs
    job1 = add(5, 3)
    job2 = multiply(job1.output, 2)
    job3 = subtract(job2.output, 1)
    
    # Create flow
    flow = Flow([job1, job2, job3], output=job3.output, name="arithmetic_flow")
    
    try:
        # Run using Prefect
        result = run_on_prefect(flow, flow_name="arithmetic_workflow")
        print(f"Flow result: {result}")
    except ImportError:
        print("Prefect is not installed. Please install with: pip install prefect")
    except Exception as e:
        print(f"Error running flow: {e}")


def example_parallel_flow():
    """Example of running a parallel flow with Prefect."""
    print("\n=== Parallel Flow Example ===")
    
    # Create independent jobs that can run in parallel
    job1 = add(1, 2)
    job2 = add(3, 4)
    job3 = add(5, 6)
    
    # These jobs depend on the parallel jobs
    job4 = multiply(job1.output, job2.output)
    job5 = multiply(job3.output, 2)
    
    # Final job combines everything
    job6 = add(job4.output, job5.output)
    
    # Create flow
    flow = Flow(
        [job1, job2, job3, job4, job5, job6], 
        output=job6.output, 
        name="parallel_arithmetic_flow"
    )
    
    try:
        # Run using Prefect with concurrent task runner
        result = run_on_prefect(
            flow, 
            flow_name="parallel_workflow",
            task_runner="concurrent"
        )
        print(f"Parallel flow result: {result}")
    except ImportError:
        print("Prefect is not installed. Please install with: pip install prefect")
    except Exception as e:
        print(f"Error running parallel flow: {e}")


def example_prefect_manager():
    """Example using the PrefectManager class."""
    print("\n=== PrefectManager Example ===")
    
    try:
        # Create manager
        manager = PrefectManager(task_runner="sequential")
        
        # Create a flow
        job1 = add(10, 5)
        job2 = multiply(job1.output, 3)
        flow = Flow([job1, job2], output=job2.output, name="manager_test_flow")
        
        # Submit flow (this would be async in real usage)
        print("Creating Prefect flow...")
        prefect_flow = flow_to_prefect_flow(flow, flow_name="manager_workflow")
        print(f"Created Prefect flow: {prefect_flow}")
        
        # You could also create a deployment like this:
        # deployment = manager.create_deployment(
        #     flow,
        #     deployment_name="my_deployment",
        #     work_pool_name="my_work_pool"
        # )
        # print(f"Created deployment: {deployment}")
        
    except ImportError:
        print("Prefect is not installed. Please install with: pip install prefect")
    except Exception as e:
        print(f"Error with PrefectManager: {e}")


def example_error_handling():
    """Example showing error handling in Prefect workflows."""
    print("\n=== Error Handling Example ===")
    
    @job
    def divide(a: int, b: int) -> float:
        """Divide two numbers."""
        if b == 0:
            raise ValueError("Cannot divide by zero!")
        return a / b
    
    # First, show a successful division
    working_job = divide(10, 2)
    try:
        result = run_on_prefect(working_job, flow_name="working_division")
        print(f"Successful division result: {list(result.values())[0]}")
    except Exception as e:
        print(f"Error in working job: {e}")
        return
    
    # Now show error handling (this will generate Prefect error logs, but that's expected)
    print("Testing error handling (expect to see error logs below)...")
    failing_job = divide(10, 0)
    
    try:
        result = run_on_prefect(failing_job, flow_name="failing_job")
        print(f"❌ Unexpected success: {result}")
    except ImportError:
        print("Prefect is not installed. Please install with: pip install prefect")
    except Exception as e:
        print(f"✅ Expected error caught and handled: {type(e).__name__}: {e}")
        print("   (The error traceback above is expected - Prefect logs all failures)")


if __name__ == "__main__":
    print("Jobflow + Prefect Integration Examples")
    print("=====================================")
    print("NOTE: Error tracebacks in logs are expected for the error handling example.")
    print("      The examples demonstrate both successful execution and error handling.")
    print()
    
    # Check if Prefect is available
    try:
        import prefect
        print(f"Prefect version: {prefect.__version__}")
    except ImportError:
        print("Prefect is not installed. Some examples will not work.")
        print("Install with: pip install prefect")
    
    # Run examples
    example_single_job()
    example_simple_flow()
    example_parallel_flow()
    example_prefect_manager()
    example_error_handling()
    
    print("\n=== Examples Complete ===")
    print("To deploy to a Prefect cluster:")
    print("1. Start Prefect server: prefect server start")
    print("2. Create work pool: prefect work-pool create --type process my_pool")
    print("3. Create deployment using PrefectManager.create_deployment()")
    print("4. Run worker: prefect worker start --pool my_pool")