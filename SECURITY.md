# Security policy

## Supported versions

Security fixes are released for the latest v1 release, as a new patch or minor version. Earlier
releases are not patched; upgrade to the latest v1 release, which keeps the compatibility promise
in [ADR-0020](docs/adr/0020-v1-compatibility-promise.md).

## Reporting a vulnerability

Report a vulnerability privately through GitHub:
[report a vulnerability](https://github.com/aetomala/worklease/security/advisories/new).
Do not open a public issue, pull request or discussion for it.

Include the affected version, the backend and database driver, and a reproduction or a
description of the impact. A report that concerns a dependency is passed to that project when
worklease's use of it is not affected.
