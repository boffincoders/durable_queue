import 'package:durable_queue/durable_queue.dart';

/// Uploads one photo file from the app's support directory.
final class PhotoUpload extends DurableTask {
  PhotoUpload(this.fileName);

  /// The file name inside the `photos` directory.
  final String fileName;

  static const taskType = 'photo_upload';

  @override
  String get type => taskType;

  @override
  Map<String, dynamic> toJson() => {'fileName': fileName};

  static PhotoUpload fromJson(Map<String, dynamic> json) =>
      PhotoUpload(json['fileName'] as String);
}
