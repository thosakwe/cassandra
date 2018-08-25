import 'dart:typed_data';
import 'package:cassandra/cassandra.dart';
import 'package:test/test.dart';

void main() {
  CqlFrameHeader requestHeader, responseHeader;

  setUp(() {
    // Make a 9-byte header
    requestHeader =
        new CqlFrameHeader(new ByteData.view(new Uint8List(9).buffer));
    responseHeader =
        new CqlFrameHeader(new ByteData.view(new Uint8List(9).buffer));

    // Set direction
    requestHeader.byteData.setUint8(0, 0x04);
    responseHeader.byteData.setUint8(0, 0x84);
  });

  test('parses version', () {
    expect(requestHeader.version.isRequest, true);
    expect(requestHeader.version.isResponse, false);
    expect(responseHeader.version.isResponse, true);
    expect(responseHeader.version.isRequest, false);
  });
}
