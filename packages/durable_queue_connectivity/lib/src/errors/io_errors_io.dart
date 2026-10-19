import 'dart:io';

/// Whether [error] is a `dart:io` socket-level failure.
bool isIoConnectionError(Object error) =>
    error is SocketException || error is HttpException;
