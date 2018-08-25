import 'dart:async';
import 'dart:io';
import 'binary_reader.dart';
import 'frame.dart';

class CassandraConnection {
  final host;
  final int port;
  final String username;
  final String password;
  final String keyspaceName;
  final bool useSsl;

  Socket _socket;

  CassandraConnection(this.host, this.port,
      {this.username, this.password, this.keyspaceName, this.useSsl: false});

  Future open() async {
    if (_socket != null) {
      throw new StateError('The connection is already open.');
    } else {
      if (useSsl == true) {
        _socket = await SecureSocket.connect(host, port);
      } else {
        _socket = await Socket.connect(host, port);
      }
    }
  }

  Future close() async {
    await _socket.close();
  }
}
