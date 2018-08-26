import 'dart:async';
import 'dart:io';
import 'dart:typed_data';
import 'package:dart2_constant/convert.dart';
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

      _reader = new BinaryReader();
      _socket.pipe(_reader);

      await sendOptionsRequest();
      await sendStartupRequest();
    }
  }

  static int encodedSizeOfString(String s) {
    // Short + UTF-8 string length (not a c-string)
    return 2 + utf8.encode(s).length;
  }

  static int encodedSizeOfStringMap(Map<String, String> map) {
    if (map.isEmpty) return 2;

    // Length as a short = 2
    // + size of key + value for each key
    return 2 +
        map.keys
            .map((k) => encodedSizeOfString(k) + encodedSizeOfString(map[k]))
            .reduce((a, b) => a + b);
  }

  static void encodeString(ByteData byteData, String string, int byteOffset) {
    var bytes = utf8.encode(string);

    // Write strlen as short
    byteData.setUint16(byteOffset, bytes.length, Endian.big);

    // Write the actual string
    for (int i = 0; i < bytes.length; i++) {
      byteData.setUint8(byteOffset + 2 + i, bytes[i]);
    }
  }

  static void encodeStringMap(
      ByteData byteData, Map<String, String> map, int byteOffset) {
    // Write the length as a short.
    byteData.setUint16(byteOffset, map.length, Endian.big);

    // Then, for each key, write the key and value.
    var index = byteOffset + 2;

    for (var k in map.keys) {
      var v = map[k];
      encodeString(byteData, k, index);
      index += encodedSizeOfString(k);
      encodeString(byteData, v, index);
      index += encodedSizeOfString(v);
    }
  }

  Future sendOptionsRequest() async {
    var buf = new Uint8List(9);
    var byteData = new ByteData.view(buf.buffer);

    // Write the header.
    new CqlFrameHeader(byteData)
      ..version = CqlFrameHeaderVersion.requestV5
      ..flags = CqlFrameHeaderFlags(0)
      ..streamId = 0
      ..opcode = CqlFrameOpcode.options
      ..bodyLength = 0;

    // Send the data...
    _socket.add(buf);
    await _socket.flush();

    // Read the next header...
    var resBuf = await _reader.read(9);
    var resHeader = new CqlFrameHeader(new ByteData.view(resBuf.buffer));
    var data = await _reader.read(resHeader.bodyLength);
    byteData = new ByteData.view(data.buffer);

    // Get the error code
    var errorCode = byteData.getInt32(0, Endian.big);

    // Read the length of the message.
    var msgLen = byteData.getUint16(4, Endian.big);

    // Next, read the message.
    var msgBuf = new Uint8List.view(data.buffer, 6, msgLen);
    var msg = utf8.decode(msgBuf);

    throw new StateError('Error $errorCode: $msg');
  }

  Future sendStartupRequest() async {
    var options = {'CQL_VERSION': '3.0.0'};
    var optionsSize = encodedSizeOfStringMap(options);

    var buf = new Uint8List(9 + optionsSize);
    var byteData = new ByteData.view(buf.buffer);

    // Write the header.
    new CqlFrameHeader(byteData)
      ..version = CqlFrameHeaderVersion.requestV5
      ..flags = CqlFrameHeaderFlags(0)
      ..streamId = 0
      ..opcode = CqlFrameOpcode.startup
      ..bodyLength = optionsSize;

    // Encode the options.
    encodeStringMap(byteData, options, 9);

    // Send the data...
    _socket.add(buf);
    await _socket.flush();

    // Read the next header...
    var resBuf = await _reader.read(9);
    var resHeader = new CqlFrameHeader(new ByteData.view(resBuf.buffer));
    throw resHeader.opcode;
  }

  Future close() async {
    await _socket.close();
  }
}
