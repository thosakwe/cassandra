import 'package:cassandra/cassandra.dart';
import 'package:test/test.dart';

void main() {
  CassandraConnection connection;

  setUp(() async {
    connection = new CassandraConnection(
      'localhost',
      7199,
      automaticallySendStartupMessage: false,
    );
    await connection.open();
  });

  tearDown(() async {
    await connection.close();
  });

  test('startup', () async {
    await connection.sendStartupRequest();
  });
}
