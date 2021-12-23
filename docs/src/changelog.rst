Change log
==========

v0.1.6
------

Bug fixes:

- Docs fixes (@arosen).
- Compatability with maggma>=0.38.1 (#68)
- Fixed missing PyYAML requirement (#67)

v0.1.5
------

Bug fixes:

- Remove ``JobConfig.pass_metadata`` option and instead pass metadata automatically.
- Fix serialization compatability with the FireWorks workflow manager.


v0.1.4
------

New features:

- Add ``append_name`` option to ``Job`` and ``Flow`` that allow easy modification of all
  job names in a flow.
- Add ``JobConfig.pass_metadata`` (defaults to True) that can be used to pass job metadata
  on to dynamically added jobs.

Bug fixes:

- Fireworks manager now adds metadata to FireWork spec. Fixes #21.

v0.1.3
------

Jobflow now uses pydantic to handle settings. Currently, there is only a single setting
``JOB_STORE`` which controls the default store used by ``run_locally`` and the fireworks
manager. You can update the default store by writing a ``~/.jobflow.yaml`` settings
file. See the API documentation for more details.

v0.1.2
------

New features:

- ``ensure_success`` option added to ``run_locally``.
- Better graph visualisation.
- Updating the name of a job from a maker now propogates the name change to the maker.
- ``Job.update_maker_kwargs`` with ``nested=True`` now applies the updates to makers
  in the kwargs or args of the job.

v0.1.1
------

Docs updates.

v0.1.0
------

Major changes:

- ``Schema`` class removed. Any pydantic model can now be an output schema.

Enhancements:

- ``JobStore.get_output`` now resolves references in the output of other jobs.
- ``JobStore.get_output``: ``which`` now supports specifying a specific job index.
- Better support for circular and missing references in ``JobStore.get_output`` and
  ``OutputReference.resolve``.
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

- ``Job``, ``Flow``, ``Maker``, and ``JobStore`` API.
- Tools for running Flows locally.
- Fireworks integration.
