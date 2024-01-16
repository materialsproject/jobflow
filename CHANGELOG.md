# Change log

## v0.1.16

### Bug Fixes 🐛
* Explicit warning stacklevels by @janosh in https://github.com/materialsproject/jobflow/pull/503
* Fix handling of function from standard library by @gpetretto in https://github.com/materialsproject/jobflow/pull/516

### Enhancements 🛠
* Optionally avoid deserialization when resolving references by @gpetretto in https://github.com/materialsproject/jobflow/pull/512

### Documentation 📖
* Add documentation for how to deal with stores beyond `MemoryStore` by @Andrew-S-Rosen in https://github.com/materialsproject/jobflow/pull/506
* Fix tutorial bug by @mcgalcode in https://github.com/materialsproject/jobflow/pull/505
* Fix tutorial link formatting in docs by @ml-evs in https://github.com/materialsproject/jobflow/pull/504
* Add reference for jobflow-remote by @ml-evs in https://github.com/materialsproject/jobflow/pull/513

### House-Keeping 🧹
* Build docs on every push by @utf in https://github.com/materialsproject/jobflow/pull/507
* Update deploy.yml by @utf in https://github.com/materialsproject/jobflow/pull/508

### Other Changes
* Auto-update pre-commit hooks by @github-actions in https://github.com/materialsproject/jobflow/pull/511

### New Contributors
* @ml-evs made their first contribution in https://github.com/materialsproject/jobflow/pull/504

**Full Changelog**: https://github.com/materialsproject/jobflow/compare/v0.1.15...v0.1.16

## v0.1.15

### Bug Fixes 🐛
* Fix JobStoreDocument validator by @utf in https://github.com/materialsproject/jobflow/pull/478
* fix Flow deserialization for hosts by @gpetretto in https://github.com/materialsproject/jobflow/pull/499
### Enhancements 🛠
* Formalizing the JobStore document format as a pydantic model by @hrushikesh-s in https://github.com/materialsproject/jobflow/pull/424
* Simplify code by using `Flow` methods `__len__` and `__getitem__` by @janosh in https://github.com/materialsproject/jobflow/pull/467
* `run_locally()` add `root_dir: str | Path | None` keyword by @janosh in https://github.com/materialsproject/jobflow/pull/486
* Allow SSHTunnel in job stores  by @mjwen in https://github.com/materialsproject/jobflow/pull/477
### Documentation 📖
* JOSS paper by @utf in https://github.com/materialsproject/jobflow/pull/446
* JOSS Paper: Add missing DOIs by @Andrew-S-Rosen in https://github.com/materialsproject/jobflow/pull/452
* JOSS Paper: Reduce line-spacing in code blocks by @Andrew-S-Rosen in https://github.com/materialsproject/jobflow/pull/454
* JOSS Paper update: Add missing year to `refs.bib` by @Andrew-S-Rosen in https://github.com/materialsproject/jobflow/pull/458
* JOSS Paper: Remove stray tick mark in "[@montydb]`" by @Andrew-S-Rosen in https://github.com/materialsproject/jobflow/pull/459
* JOSS Paper: Add another missing year to `refs.bib` by @Andrew-S-Rosen in https://github.com/materialsproject/jobflow/pull/461
* Update refs.bib by @xuanxu in https://github.com/materialsproject/jobflow/pull/462
* JOSS: Add missing `,` to paper by @Andrew-S-Rosen in https://github.com/materialsproject/jobflow/pull/479
* JOSS: Add more complete Zenodo reference info by @Andrew-S-Rosen in https://github.com/materialsproject/jobflow/pull/481
* JOSS: Fix references to include all coauthors and remove "others" by @Andrew-S-Rosen in https://github.com/materialsproject/jobflow/pull/480
* Add fibonacci example to tutorial by @JaGeo in https://github.com/materialsproject/jobflow/pull/494
### House-Keeping 🧹
* Remove `__all__` from all modules by @janosh in https://github.com/materialsproject/jobflow/pull/442
* Bump minimum Python version to 3.9 by @janosh in https://github.com/materialsproject/jobflow/pull/455
* Replace deprecated `pkg_resources` with `importlib.metadata.version` by @janosh in https://github.com/materialsproject/jobflow/pull/460
* Match `atomate2` `ruff` config by @janosh in https://github.com/materialsproject/jobflow/pull/464
* Test error messages by @janosh in https://github.com/materialsproject/jobflow/pull/465

## New Contributors
* @hrushikesh-s made their first contribution in https://github.com/materialsproject/jobflow/pull/424
* @xuanxu made their first contribution in https://github.com/materialsproject/jobflow/pull/462

**Full Changelog**: https://github.com/materialsproject/jobflow/compare/v0.1.14...v0.1.15

## v0.1.14

### Bug Fixes 🐛
* Add warning when jobflow.yaml is blank or badly formatted by @mcgalcode in https://github.com/materialsproject/jobflow/pull/415
* Fix issue with data store by @JaGeo in https://github.com/materialsproject/jobflow/pull/409
* Make ensure_success apply to replace/detour/addition jobs by @mcgalcode in https://github.com/materialsproject/jobflow/pull/426
### Enhancements 🛠
* Add tests for Python 3.11 by @Andrew-S-Rosen in https://github.com/materialsproject/jobflow/pull/413
* Add support for Pydantic 2 by @Andrew-S-Rosen in https://github.com/materialsproject/jobflow/pull/440
### Documentation 📖
* Link to @jageo's tutorials and fix a broken link by @Andrew-S-Rosen in https://github.com/materialsproject/jobflow/pull/412
* Update @arosen93 to @Andrew-S-Rosen by @Andrew-S-Rosen in https://github.com/materialsproject/jobflow/pull/427

**Full Changelog**: https://github.com/materialsproject/jobflow/compare/v0.1.13...v0.1.14

## v0.1.13

Bug Fixes:

- Delete `Flow.__deepcopy__` by @janosh in https://github.com/materialsproject/jobflow/pull/399

**Full Changelog**: https://github.com/materialsproject/jobflow/compare/v0.1.12...v0.1.13

## v0.1.12

New features:

- Add `to_mermaid` function to convert flow to mermaid syntax by @utf in https://github.com/materialsproject/jobflow/pull/311
- Allow external references by @gpetretto in https://github.com/materialsproject/jobflow/pull/392
- Add `to_mermaid` for all in graph utils by @JaGeo in https://github.com/materialsproject/jobflow/pull/351
- Propagate tags from `fw_spec` to metadata by @sivonxay in https://github.com/materialsproject/jobflow/pull/345
- Allow bson objects as job inputs, e.g. datetime.datetimes by @mcgalcode in https://github.com/materialsproject/jobflow/pull/375
- Also `allow_bson` in job serialization by @mcgalcode in https://github.com/materialsproject/jobflow/pull/376
- `Flow` + `Job` magic methods by @janosh in https://github.com/materialsproject/jobflow/pull/369

Bug fixes:

- Fix ValueError: mutable default for field `add_maker` is not allowed: use `default_factory` by @janosh in https://github.com/materialsproject/jobflow/pull/387
- Fixed nested data storage bug by @jmmshn in https://github.com/materialsproject/jobflow/pull/293
- Test imports need to be in function by @jmmshn in https://github.com/materialsproject/jobflow/pull/310
- Fix `TypeError` when passing name to `flow_to_workflow(` by @janosh in https://github.com/materialsproject/jobflow/pull/396
- Fix mermaid with one job (in a flow) by @JaGeo in https://github.com/materialsproject/jobflow/pull/350
- Fixed `JobStore.from_dict_spec` so that the original `dict_spec` is not modified by @davidwaroquiers in https://github.com/materialsproject/jobflow/pull/331

Enhancements:

- Fix typo in data store docs by @xperrylinn in https://github.com/materialsproject/jobflow/pull/316
- Fix typo in fireworks docs by @Andrew-S-Rosen in https://github.com/materialsproject/jobflow/pull/343
- Fix typo in docs by @Andrew-S-Rosen in https://github.com/materialsproject/jobflow/pull/359
- Fix broken link to FireWorks tutorial by @janosh in https://github.com/materialsproject/jobflow/pull/319
- Add documentation: Flows, FireWorks, Dynamic Flows, Makers by @Andrew-S-Rosen in https://github.com/materialsproject/jobflow/pull/338
- Update forum link by @mkhorton in https://github.com/materialsproject/jobflow/pull/373
- Add copy button to code blocks in docs by @Andrew-S-Rosen in https://github.com/materialsproject/jobflow/pull/344

## v0.1.11

- Enable serialisation of bson.

## v0.1.10

- Move project configuration to `pyproject.toml`.
- Add tutorial on generalized makers (@jmmshn, #268)

## v0.1.9

New features

- Delayed updates to config and metadata for dynamic flows. See docstring of
  `Job.update_metadata` for more details (@gpetretto, #198)
- Additional stores are now generated on the fly as memory stores if they are not
  specified in jobflow settings (@davidwaroquiers, #183)

Bug fixes:

- Optimised calls to update_kwargs (@jmmshn, #177)
- "job_uuid" and "job_index" are now indexed fields in additional stores (@jmmshn, #165)
- Fix additional store storing `None` (@mjwen, #160)

Enhancements:

- Docs refactored.
- Added code of conduct.

## v0.1.8

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

## v0.1.7

New features:

- Validate subschemas of nested models (@gpetretto, #118).
- `downstream_manager_config` for controlling config of dynamic jobs (@Andrew-S-Rosen, #121).
- S3Store yaml parsing (@jmmshn, #124).

Bug fixes:

- Fix home path for loading settings (@gpetretto, #119).
- Docs updates (@Andrew-S-Rosen, #111).

## v0.1.6

Bug fixes:

- Docs fixes (@arosen).
- Compatibility with maggma>=0.38.1 (#68)
- Fixed missing PyYAML requirement (#67)

## v0.1.5

Bug fixes:

- Remove `JobConfig.pass_metadata` option and instead pass metadata automatically.
- Fix serialization compatibility with the FireWorks workflow manager.

## v0.1.4

New features:

- Add `append_name` option to `Job` and `Flow` that allow easy modification of all
  job names in a flow.
- Add `JobConfig.pass_metadata` (defaults to True) that can be used to pass job metadata
  on to dynamically added jobs.

Bug fixes:

- Fireworks manager now adds metadata to FireWork spec. Fixes #21.

## v0.1.3

Jobflow now uses pydantic to handle settings. Currently, there is only a single setting
`JOB_STORE` which controls the default store used by `run_locally` and the fireworks
manager. You can update the default store by writing a `~/.jobflow.yaml` settings
file. See the API documentation for more details.

## v0.1.2

New features:

- `ensure_success` option added to `run_locally`.
- Better graph visualisation.
- Updating the name of a job from a maker now propagates the name change to the maker.
- `Job.update_maker_kwargs` with `nested=True` now applies the updates to makers
  in the kwargs or args of the job.

## v0.1.1

Docs updates.

## v0.1.0

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

## v0.0.2

Testing automated releases.

## v0.0.1

Initial release containing:

- `Job`, `Flow`, `Maker`, and `JobStore` API.
- Tools for running Flows locally.
- Fireworks integration.
