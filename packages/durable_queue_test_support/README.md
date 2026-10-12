# durable_queue_test_support

Shared test helpers for the packages in this repository. Not published to pub.dev.

* `queueStorageContractTests` runs every behavior a `QueueStorage` adapter must provide. See [STORAGE.md](../durable_queue/STORAGE.md).
* `storedTask` builds a `StoredTask` with sensible defaults for tests.

Packages in this workspace use it as a dev dependency:

```yaml
dev_dependencies:
  durable_queue_test_support:
    path: ../durable_queue_test_support
```

```dart
import 'package:durable_queue_test_support/durable_queue_test_support.dart';

void main() => queueStorageContractTests(MyStorage.new);
```

Adapter packages outside this repository can copy `lib/src/storage_contract.dart` and `lib/src/stored_task_builder.dart`.
