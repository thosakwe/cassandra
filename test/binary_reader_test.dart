import 'dart:async';
import 'package:cassandra/src/binary_reader.dart';
import 'package:charcode/ascii.dart';
import 'package:test/test.dart';

void main() {
  BinaryReader reader;

  setUp(() {
    var stream = new Stream<List<int>>.fromIterable([
      [$H, $e, $l, $l, $o, $comma, $space],
      [$w, $o, $r, $l, $d, $exclamation],
    ]);

    reader = new BinaryReader();
    stream.pipe(reader);
  });

  test('can read buffer of smaller size', () async {
    expect(await reader.read(3), [$H, $e, $l]);
  });

  test('can read buffer of exact size', () async {
    expect(await reader.read(7), [$H, $e, $l, $l, $o, $comma, $space]);
  });

  test('can read buffer of greater size', () async {
    expect(await reader.read(10),
        [$H, $e, $l, $l, $o, $comma, $space, $w, $o, $r]);
  });

  test('fails on reads of too large a size', () async {
    expect(() => reader.read(1000), throwsStateError);
  });
}
