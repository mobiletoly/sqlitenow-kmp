# SQLiteNow Swift Sync Sample

This sample is a native SwiftUI iOS app backed by a generated SQLiteNow Swift
package and SQLiteNow's sync support/runtime products. The app imports only:

```swift
import NowSampleSyncDatabaseSQLiteNow
```

The Swift sync guide is available at:
`https://mobiletoly.github.io/sqlitenow-kmp/swift/sync/`.

Generate the local Swift package first:

```shell
./gradlew sampleSyncSwiftGenerate
```

The task invokes the SwiftPM `sqlitenow-generate` command plugin from this
directory. The Swift-facing config lives in `SQLiteNow.json`, and authored SQL
lives under:

```text
SQLiteNow/databases/NowSampleSyncDatabase
```

Build the generated package directly:

```shell
swift build --package-path swift/samples/sync/SQLiteNowGenerated/NowSampleSyncDatabaseSQLiteNow
```

The generated package includes manifest version 3 metadata under
`.sqlitenow/package-manifest.json`.

This repository sample uses `runtimeXcframeworkDirectory` so it can build
against locally produced runtime artifacts. Released app projects normally omit
that field and let the SQLiteNow SwiftPM package provide runtime/support
products.

The local support guard is:

```shell
./gradlew sampleSyncSwiftSupportGate
```

Build the iOS app:

```shell
xcodebuild -project swift/samples/sync/iosApp/iosApp.xcodeproj -scheme iosApp -destination 'generic/platform=iOS Simulator' build
```

For live sync, start the compatible `nethttp-server-example` from the sibling
`go-oversync` checkout and keep it available at the app's default base URL:

```text
http://127.0.0.1:8080
```

From `/Users/pochkin/Projects/my/go-oversync`, start Postgres in one terminal:

```shell
docker run --rm -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=samplesync -p 5432:5432 postgres:16
```

Then start the sample server in another terminal:

```shell
DATABASE_URL="postgres://postgres:postgres@localhost:5432/samplesync?sslmode=disable" JWT_SECRET="dev-secret" go run ./examples/nethttp_server
```

If a server is already running, confirm that it is the sample-sync server:

```shell
curl -sS http://127.0.0.1:8080/syncx/status
```

The response should include `app_name: nethttp-server-example` and the
`business.person`, `business.person_address`, and `business.comment` tables.

The sign-in sheet posts to `/dummy-signin` with the current local source id and
uses schema `business` for sync client creation.

`SQLiteNowGenerated/` is disposable generated output. Swift app code depends on
the generated Swift package, not on `build/` or a Gradle database module.
