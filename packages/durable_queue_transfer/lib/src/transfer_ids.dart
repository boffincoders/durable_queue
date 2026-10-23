/// Transfer ids link a background_downloader task to a queue task and
/// attempt: `dq-<attempt>-<queue task id>`.
abstract final class TransferIds {
  static const prefix = 'dq-';

  static String build(String queueTaskId, int attempt) =>
      '$prefix$attempt-$queueTaskId';

  /// The attempt in [transferId], or null if it is not one of ours.
  static int? attempt(String transferId) {
    if (queueTaskId(transferId) == null) return null;
    final rest = transferId.substring(prefix.length);
    return int.parse(rest.substring(0, rest.indexOf('-')));
  }

  /// The queue task id in [transferId], or null if it is not one of ours.
  static String? queueTaskId(String transferId) {
    if (!transferId.startsWith(prefix)) return null;
    final rest = transferId.substring(prefix.length);
    final dash = rest.indexOf('-');
    if (dash <= 0 || dash == rest.length - 1) return null;
    if (int.tryParse(rest.substring(0, dash)) == null) return null;
    return rest.substring(dash + 1);
  }
}
