Change log
==========

v0.1.9
------

New features

- Delayed updates to config and metadata for dynamic flows. See docstring of
  `Job.update_metadata` for more details (@gpetretto, #198)
- Additional stores are now generated on the fly as memory stores if they are not
  specified  in jobflow settings (@davidwaroquiers, #183)

Bug fixes:

- Optimised calls to update_kwargs (@jmmshn, #177)
- "job_uuid" and "job_index" are now indexed fields in additional stores (@jmmshn, #165)
- Fix additional store storing `None` (@mjwen, #160)

Enhancements:

- Docs refactored.
- Added code of conduct.

v0.1.8
------

New features:

- New `update_metadata` function for updating the metadata of jobs and flows.
- New `update_config` function for updating the config (included manager_config) of
  jobs and flows.
- New `DIRECTORY_FORMAT` option in JobflowSettings for controlling the date time format
  used to create new directories.
- New functions for adding and removing jobs from a flow. The `Job.jobs` list is no
  longer mutable (@gpetretto).
- New `Job.hosts` attribute that stores a list of all host Flows. This captures the
  nested nature of flows with the outer flow always first in the list (@gpetretto).

Bug fixes:

- OutputReferences are no longer iterable.
- Docstring clarifications (@utf, @mjwen).

v0.1.7
------

New features:

- Validate subschemas of nested models (@gpetretto, #118).
- `downstream_manager_config` for controlling config of dynamic jobs (@arosen93, #121).
- S3Store yaml parsing (@jmmshn, #124).

Bug fixes:

- Fix home path for loading settings (@gpetretto, #119).
- Docs updates (@arosen93, #111).

v0.1.6
------

Bug fixes:

- Docs fixes (@arosen).
- Compatibility with maggma>=0.38.1 (#68)
- Fixed missing PyYAML requirement (#67)

v0.1.5
------

Bug fixes:

- Remove `JobConfig.pass_metadata` option and instead pass metadata automatically.
- Fix serialization compatibility with the FireWorks workflow manager.

v0.1.4
------

New features:

- Add `append_name` option to `Job` and `Flow` that allow easy modification of all
  job names in a flow.
- Add `JobConfig.pass_metadata` (defaults to True) that can be used to pass job metadata
  on to dynamically added jobs.

Bug fixes:

- Fireworks manager now adds metadata to FireWork spec. Fixes #21.

v0.1.3
------

Jobflow now uses pydantic to handle settings. Currently, there is only a single setting
`JOB_STORE` which controls the default store used by `run_locally` and the fireworks
manager. You can update the default store by writing a `~/.jobflow.yaml` settings
file. See the API documentation for more details.

v0.1.2
------

New features:

- `ensure_success` option added to `run_locally`.
- Better graph visualisation.
- Updating the name of a job from a maker now propagates the name change to the maker.
- `Job.update_maker_kwargs` with `nested=True` now applies the updates to makers
  in the kwargs or args of the job.

v0.1.1
------

Docs updates.

v0.1.0
------

Major changes:

- `Schema` class removed. Any pydantic model can now be an output schema.

Enhancements:

- `JobStore.get_output` now resolves references in the output of other jobs.
- `JobStore.get_output`: `which` now supports specifying a specific job index.
- Better support for circular and missing references in `JobStore.get_output` and
  `OutputReference.resolve`.
- Update dependencies to use latest jsanitize features.

Bug fixes:

- Fixed issue with references in flow of flows (@davidwaroquiers, #18).
- Makes now allows non-default parameters (fixes: #13).
- Fix reference cache with multiple indexes.

v0.0.2
------

Testing automated releases.

v0.0.1
------

Initial release containing:

- `Job`, `Flow`, `Maker`, and `JobStore` API.
- Tools for running Flows locally.
- Fireworks integration.
