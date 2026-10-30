/// Transfer ids link a background_downloader task to a queue task and
/// attempt: `dq-<attempt>-<queue task id>`, or
/// `dq-<attempt>.<n>-<queue task id>` for the n-th restart of an attempt
/// that was stopped by a hold.
abstract final class TransferIds {
  static const prefix = 'dq-';

  static final _head = RegExp(r'^(\d+)(?:\.(\d+))?$');

  static String build(String queueTaskId, int attempt, [int restart = 0]) =>
      restart == 0
      ? '$prefix$attempt-$queueTaskId'
      : '$prefix$attempt.$restart-$queueTaskId';

  /// The attempt in [transferId], or null if it is not one of ours.
  static int? attempt(String transferId) {
    final head = _headOf(transferId);
    return head == null ? null : int.parse(head.group(1)!);
  }

  /// The queue task id in [transferId], or null if it is not one of ours.
  static String? queueTaskId(String transferId) {
    if (_headOf(transferId) == null) return null;
    final rest = transferId.substring(prefix.length);
    return rest.substring(rest.indexOf('-') + 1);
  }

  static RegExpMatch? _headOf(String transferId) {
    if (!transferId.startsWith(prefix)) return null;
    final rest = transferId.substring(prefix.length);
    final dash = rest.indexOf('-');
    if (dash <= 0 || dash == rest.length - 1) return null;
    return _head.firstMatch(rest.substring(0, dash));
  }
}
