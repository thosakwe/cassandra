import 'package:cassandra/cassandra.dart';
import 'package:test/test.dart';

void main() {
  CassandraConnection connection;

  setUp(() async {
    connection = new CassandraConnection('localhost', 9042);
  });

  tearDown(() async {
    await connection.close();
  });

  test('startup', () async {
    await connection.open();
  });
}
