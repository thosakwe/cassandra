import 'dart:async';
import 'dart:io';
import 'dart:typed_data';
import 'binary_reader.dart';
import 'frame.dart';

class CassandraConnection {
  final host;
  final int port;
  final String username;
  final String password;
  final String keyspaceName;
  final bool useSsl;

  BinaryReader _reader;
  Socket _socket;

  CassandraConnection(this.host, this.port,
      {this.username,
      this.password,
      this.keyspaceName,
      this.useSsl: false});

  Future open() async {
    if (_socket != null) {
      throw new StateError('The connection is already open.');
    } else {
      if (useSsl == true) {
        _socket = await SecureSocket.connect(host, port);
      } else {
        _socket = await Socket.connect(host, port);
      }

      _reader = new BinaryReader();
      _socket
          //.map((buf) {
          //  print('Incoming: ${new String.fromCharCodes(buf)}');
          //  return buf;
          // })
          .pipe(_reader)
          .then((_) {
        print('Done');
      });

      await sendStartupRequest();
    }
  }

  Future sendStartupRequest() async {
    var buf = new Uint8List(9);
    new CqlFrameHeader(new ByteData.view(buf.buffer))
      ..version = CqlFrameHeaderVersion.request
      ..opcode = CqlFrameOpcode.startup
      ..streamId = 0
      ..flags = CqlFrameHeaderFlags(0);
    _socket.add(buf);

    // Read the next header...
    var resBuf = await _reader.read(9);
    var resHeader = new CqlFrameHeader(new ByteData.view(resBuf.buffer));
    throw resHeader.opcode;
  }

  Future close() async {
    await _socket.close();
  }
}
