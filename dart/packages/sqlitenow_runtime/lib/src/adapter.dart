typedef SqliteNowEncode<Domain, Sql> = Sql Function(Domain value);
typedef SqliteNowDecode<Domain, Sql> = Domain Function(Sql value);

final class SqliteNowAdapter<Domain, Sql> {
  const SqliteNowAdapter({required this.encode, required this.decode});

  final SqliteNowEncode<Domain, Sql> encode;
  final SqliteNowDecode<Domain, Sql> decode;

  Sql encodeValue(Domain value) => encode(value);

  Domain decodeValue(Sql value) => decode(value);
}

final class NullableSqliteNowAdapter<
  Domain extends Object,
  Sql extends Object
> {
  const NullableSqliteNowAdapter(this.adapter);

  final SqliteNowAdapter<Domain, Sql> adapter;

  Sql? encodeValue(Domain? value) {
    if (value == null) return null;
    return adapter.encodeValue(value);
  }

  Domain? decodeValue(Sql? value) {
    if (value == null) return null;
    return adapter.decodeValue(value);
  }
}
