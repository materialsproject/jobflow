# jobflow

<a href="https://codecov.io/gh/hackingmaterials/jobflow/"><img alt="code coverage" src="https://img.shields.io/codecov/c/gh/hackingmaterials/jobflow"> </a>
<a href="https://github.com/hackingmaterials/jobflow/actions?query=workflow%3ARun%20tests"><img alt="code coverage" src="https://img.shields.io/github/workflow/status/hackingmaterials/jobflow/Run%20tests"> </a>

Jobflow is a free, open-source library for writing and executing workflows. Complex
workflows can be defined using simple python functions and executed locally or on
arbitrary computing resources using the [FireWorks][fireworks] workflow manager.

Some features that distinguish jobflow are dynamic workflows, easy compositing and
connecting of workflows, and the ability to store workflow outputs across multiple
databases.

## Is jobflow for me?

jobflow is intended to be a friendly workflow software that is easy to get started with,
but flexible enough to handle complicated use cases.

Some of its features include:

- A clean and flexible Python API.
- A powerful approach to compositing and connecting workflows — information passing
  between jobs is a key goal of jobflow. Workflows can be nested allowing for a natural
  way to build complex workflows.
- Integration with multiple databases (MongoDB, S3, GridFS, and more) through the
  [Maggma][maggma] package.
- Support for the [FireWorks][fireworks] workflow management system, allowing workflow execution on
  multicore machines or through a queue, on a single machine or multiple machines.
- Support for dynamic workflows — workflows that modify themselves or create new ones
  based on what happens during execution.

## Workflow model

Workflows in jobflows are made up of two main components:

- A `Job` is an atomic computing job. Essentially any python function can be `Job`,
  provided its return values can be serialized to json. Anything returned by the job is
  considered an "output" and is stored in the jobflow database.
- A `Flow` is a collection of `Job` objects or other `Flow` objects. The connectivity
  between jobs is determined automatically from the job inputs. The execution order
  of jobs is automatically determined based on their connectivity.

Python functions can be easily converted in to `Job` objects using the `@job` decorator.
In the example below, we define a job to add two numbers.

```python
from jobflow import job, Flow


@job
def add(a, b):
    return a + b


add_first = add(1, 5)
add_second = add(add_first.output, 5)

flow = Flow([add_first, add_second])
flow.draw_graph().show()
```

The output of the job is accessed using the `output` attribute. As the job has not
yet been run, `output` contains be a reference to a future output. Outputs can be used
as inputs to other jobs and will be automatically "resolved" before the job is
executed.

Finally, we created a flow using the two `Job` objects. The connectivity between
the jobs is determined automatically (the order doesn't matter) and can be visualised
using the flow graph.

<p align="center">
<img alt="simple flow graph" src="https://raw.githubusercontent.com/hackingmaterials/jobflow/main/docs/src/_static/img/simple_flow.png" width="50%" height="50%">
</p>

## Installation

The jobflow is a Python 3.7+ library and can be installed using pip.

```bash
pip install jobflow
```

## Quickstart and tutorials

To get a first glimpse of jobflow, we suggest that you follow our quickstart tutorial.
Later tutorials delve into the advanced features of jobflow.

- 5-minute quickstart tutorial
- Defining Jobs using jobflow
- Creating Flows
- Dynamic and nested Flows
- Configuring the jobflow database
- Using jobflow with FireWorks

## Need help?

Ask questions about jobflow on the [jobflow support forum][help-forum].
If you've found an issue with jobflow, please submit a bug report on [GitHub Issues][issues].

## What’s new?

Track changes to jobflow through the [changelog][changelog].

## Contributing

We greatly appreciate any contributions in the form of a pull request.
Additional information on contributing to jobflow can be found [here][contributing].
We maintain a list of all contributors [here][contributors].

## License

jobflow is released under a modified BSD license; the full text can be found [here][license].

## Acknowledgements

Jobflow was designed and developed by Alex Ganose while in the group of Anubhav Jain.

[maggma]: https://materialsproject.github.io/maggma/
[fireworks]: https://materialsproject.github.io/fireworks/
[help-forum]: https://matsci.org/c/fireworks
[issues]: https://github.com/hackingmaterials/jobflow/issues
[changelog]: https://hackingmaterials.github.io/jobflow/changelog.html
[contributing]: https://hackingmaterials.github.io/jobflow/contributing.html
[contributors]: https://hackingmaterials.github.io/jobflow/contributors.html
[license]: https://raw.githubusercontent.com/hackingmaterials/jobflow/main/LICENSE
