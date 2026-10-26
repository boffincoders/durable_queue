# Security policy

## Reporting a vulnerability

Please do not report security problems in public issues or discussions.

Report them privately through GitHub: open the repository's [Security tab](https://github.com/boffincoders/durable_queue/security) and choose **Report a vulnerability** ([direct link](https://github.com/boffincoders/durable_queue/security/advisories/new)). Only the maintainers can see the report.

Please include:

* the package and version, and the Dart or Flutter version;
* what an attacker can do, and what they need first (for example, a copy of the database file, or code running in the app);
* steps or a small program that shows it.

We acknowledge reports within 3 working days and keep you informed until the problem is fixed or we explain why we do not consider it a vulnerability. Fixes are released as new versions of the affected packages and announced in a GitHub security advisory. We credit reporters in the advisory unless they prefer otherwise.

## Supported versions

Fixes go into the latest published version of each package. The packages are before 1.0, so update to the latest version to receive them.

| Package | Supported |
| --- | --- |
| `durable_queue` and every `durable_queue_*` package | Latest published version |

## What the packages store

Tasks are stored by the storage you choose (`durable_queue_sqlite`, `MemoryQueueStorage`, or your own adapter) as plain data, including their payloads and the text of the last error. `durable_queue_encryption` encrypts payloads, error messages, deduplication keys, and groups before they are written; ids, task types, status, times, and idempotency keys stay readable. See its README for what it does and does not protect.

Some things are your app's responsibility:

* Keep secrets such as passwords and access tokens out of task payloads and exception messages. Get current credentials when the handler runs.
* Keep encryption keys in the platform's secure storage, not next to the database.
* Use HTTPS for requests and transfers. The demo app and its server use plain HTTP on a local network and are not meant for production.
* Send the task's idempotency key to your server, so a repeated attempt cannot apply an action twice.
