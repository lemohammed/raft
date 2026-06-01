# Security Policy

## Supported Versions

Security fixes target the `main` branch until tagged release lines exist.

## Reporting a Vulnerability

Please report suspected vulnerabilities privately to the repository owner rather
than opening a public issue with exploit details. Include:

- the affected command or protocol record;
- a minimal reproduction or proof of impact;
- whether the issue requires local same-user access, shared bus access, or a
  malicious agent identity.

`raft` is currently a same-host, same-user coordination bus. Reports that show
an agent can impersonate another claimed name, forge signed records, bypass a
capability token, escape a task sandbox boundary, or make another agent lose an
open obligation are in scope.
