# Running Jobflow with FireWorks

## Introduction

[FireWorks](https://materialsproject.github.io/fireworks/) is a powerful software package to manage and execute complex workflows. Jobflow comes with native support to convert a `Job` or `Flow` into a FireWorks `firework` or `workflow`, respectively.

## Converting a Job to a Firework

To convert a `Job` to a `firework` and add it to your launch pad:

```python
from fireworks import LaunchPad
from jobflow.managers.fireworks import job_to_firework

fw = job_to_firework(job)
lpad = LaunchPad.auto_load()
lpad.add_wf(fw)
```

## Converting a Flow to a Workflow

To convert a `Flow` to a `workflow` and add it to your launch pad:

```python
from fireworks import LaunchPad
from jobflow.managers.fireworks import flow_to_workflow

wf = flow_to_workflow(flow)
lpad = LaunchPad.auto_load()
lpad.add_wf(wf)
```

## Dispatching Calculations

With a workflow added to your launch pad, on the desired machine of choice, you can run `qlaunch rapidfire --nlaunches <N>` (where `<N>` is the number of jobs to submit) in the command line to submit your workflows to the job scheduler.

## Setting where Jobs are Dispatched

In many cases, you may wish to submit individual jobs in a flow to different machines or with different job submission options (e.g. different compute resources).

The easiest way to achieve this goal is by defining a unique `fworker` for each job in your flow and setting the `job.config.manager_config["_fworker"]` to be the name of the `fworker` you wish to use for that job.

Let's consider an example. Imagine you have a `flow` with two jobs, `job1` and `job2`, and you wish to submit `job1` and `job2` with different walltimes.

### Setting up the FireWorks Configurations

First, you would define two separate FireWorks configuration directories:

```text
/path/to/fw_config1
├── FW_config1.yaml
├── my_fworker1.yaml
├── my_launchpad1.yaml
└── my_qadapter1.yaml
```

```text
/path/to/fw_config2
├── FW_config2.yaml
├── my_fworker2.yaml
├── my_launchpad2.yaml
└── my_qadapter2.yaml
```

The `my_fworker1.yaml` and `my_fworker2.yaml` files should have different `name` attributes, such as `fworker1` and `fworker2`, so they can be distinguished from one another.

Additionally, the `my_qadapter1.yaml` and `my_qadapter2.yaml` files should have different job submission settings tailored for the two job types you plan to run (e.g. different `nodes` or `walltime` attributes).

While optional, it is convenient to define a shortcut to these directories in your `~/.bashrc`, as will become clear below.

```bash
export FW_CONFIG1=/path/to/fw_config1
export FW_CONFIG2=/path/to/fw_config2
```

### Setting the Manager Configs

With the unique job submission information prepared for your two job types, you now need to constrain each job in your flow to a specific `fworker`.

To achieve this, set the name of each `fworker` (i.e. the `name` variable in `my_fworker1.yaml` and `my_fworker2.yaml`) as the `_fworker` attribute in the `manager_config` of each job in your flow.

```python
for job, _ in flow.iterflow():
    if job.name == "job1":
        job.update_config({"manager_config": {"_fworker": "fworker1"}})
    else:
        job.update_config({"manager_config": {"_fworker": "fworker2"}})
```

To make the process a bit easier, the `{obj}update_config()` function can also be applied directly to a flow in conjunction with a filter. The logic above can then be simplified to:

```python
flow.update_config({"manager_config": {"_fworker": "fworker1"}}, name_filter="job1")
```

### Launching the Jobs

As described above, convert the flow to a workflow via {obj}`flow_to_workflow` and add it to your launch pad.

Finally, use the `-c` ("config") command-line option when launching your fireworks to specify the FireWorks configuration directory you wish to use.

To launch all fireworks tied to `fw_config1`, run `qlaunch rapidfire -c $FW_CONFIG1 --nlaunches <N>`. To launch all fireworks tied to `fw_config2`, run `qlaunch rapidfire -c $FW_CONFIG2 --nlaunches <N>`. As always, the jobs won't actually run until any prior jobs they depend on are completed.

## Learn More

For additional FireWorks-related options in Jobflow, see the {obj}`jobflow.managers.fireworks` section of the [Jobflow API](https://materialsproject.github.io/jobflow/jobflow.managers.html#module-jobflow.managers.fireworks).

For documentation on how to submit jobs to the queue that are in your launchpad, refer to the "Queue Tutorial" in the [FireWorks Documentation](https://materialsproject.github.io/fireworks/queue_tutorial.html#submit-a-job).
