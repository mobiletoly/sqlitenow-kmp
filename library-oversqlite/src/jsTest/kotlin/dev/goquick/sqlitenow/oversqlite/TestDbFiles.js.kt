package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.SqliteConnectionConfig
import dev.goquick.sqlitenow.core.persistence.IndexedDbSqlitePersistence
import kotlin.js.Promise
import kotlinx.coroutines.await

internal actual fun supportsDurableTestDatabaseRestart(): Boolean = !isNode()

internal actual fun createTempSqliteNowTestDbPath(prefix: String): String =
    if (isNode()) nodeJoinPath(nodeTempDir(), "$prefix-${randomSuffix()}.db")
    else "$prefix-${randomSuffix()}.db"

private val browserTestPersistence = IndexedDbSqlitePersistence(
    storageName = "SqliteNowOversqliteTest", storeName = "sqlite-tests",
)

internal actual fun createSqliteNowTestConnectionConfig(path: String): SqliteConnectionConfig =
    if (isNode()) SqliteConnectionConfig()
    else SqliteConnectionConfig(persistence = browserTestPersistence)

internal actual suspend fun deleteTempSqliteNowTestDbArtifacts(path: String) {
    if (isNode()) nodeDeleteIfExists(path) else browserTestPersistence.clear(path)
}

internal actual suspend fun cleanupDirectWorkerTestDatabase(dbName: String) {
    cleanupDirectWorkerTestDatabaseJs(dbName).await()
}

@Suppress("UNUSED_PARAMETER")
private fun cleanupDirectWorkerTestDatabaseJs(dbName: String): Promise<dynamic> =
    js(
        """
        (() => {
          if (typeof window === 'undefined') return Promise.resolve();
          const bytes = new TextEncoder().encode(dbName);
          return crypto.subtle.digest('SHA-256', bytes).then((digest) => {
            const hash = Array.from(new Uint8Array(digest))
              .map((byte) => byte.toString(16).padStart(2, '0')).join('');
            const target = "sqlitenow-worker-v1-" + hash + ".sqlite3";
            const ignoreMissing = (promise) => promise.catch((error) => {
              if (!error || error.name !== 'NotFoundError') throw error;
            });
            return navigator.storage.getDirectory().then((root) =>
              Promise.all(
                [target, target + ".health.json", target + ".migration.json"].map(
                  (name) => ignoreMissing(root.removeEntry(name))
                )
              )
            );
          });
        })()
        """,
    ).unsafeCast<Promise<dynamic>>()

private fun randomSuffix(): String =
    js("Math.random().toString(16).slice(2)").unsafeCast<String>()

private fun isNode(): Boolean =
    js("typeof process !== 'undefined' && process.versions != null && process.versions.node != null").unsafeCast<Boolean>()

// Only executed in Node. Indirect lookup keeps Node built-ins out of browser bundles.
private fun nodeModule(name: String): dynamic = js("eval('require')(name)")

private fun nodeTempDir(): String = nodeModule("node:os").tmpdir().unsafeCast<String>()

private fun nodeJoinPath(parent: String, child: String): String =
    nodeModule("node:path").join(parent, child).unsafeCast<String>()

private fun nodeDeleteIfExists(path: String) {
    val fs = nodeModule("node:fs")
    if (fs.existsSync(path).unsafeCast<Boolean>()) fs.rmSync(path, js("({force:true})"))
}
