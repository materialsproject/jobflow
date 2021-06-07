Change log
==========

[Unreleased]
------------

Schema related fixes:

- Better schema support for ``typing.Any``.
- ``Schema`` no longer subclasses ``MSONable``. This improves the schema description
  and fixes a number of bugs.

v0.0.2
------

Testing automated releases.

v0.0.1
------

Initial release containing:

- ``Job``, ``Flow``, ``Maker``, and ``JobStore`` API.
- Tools for running Flows locally.
- Fireworks integration.
