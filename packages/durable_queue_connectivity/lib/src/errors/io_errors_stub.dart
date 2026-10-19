/// Whether [error] is a `dart:io` socket-level failure. Always false where
/// `dart:io` is unavailable.
bool isIoConnectionError(Object error) => false;
