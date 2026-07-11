# Security policy

## Supported versions

Security fixes are provided for the latest published release.

## Reporting a vulnerability

Please do not open a public issue for a suspected vulnerability. Use GitHub's
private vulnerability reporting for this repository:

<https://github.com/aupadhyay/rrjj/security/advisories/new>

Include the affected version, reproduction steps, impact, and any suggested
mitigation. You should receive an acknowledgement within seven days.

## Deployment notes

rrjj records filesystem contents and should be treated as having access to
everything under its watched root. Its HTTP server has no authentication and is
intended to bind to loopback or sit behind an authenticated proxy. Use
least-privilege object-store credentials scoped to the recording prefix, and
apply retention and access policies appropriate for the captured data.

The Unix control socket is also unauthenticated at the protocol layer. rrjj
creates it with mode `0600`, so only the daemon's operating-system user should
be able to issue snapshot, mark, flush, pause, or resume commands. Do not relax
its permissions or place it in a shared writable directory without an
additional access-control boundary.
