package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.SqliteConnectionConfig
import dev.goquick.sqlitenow.core.SqlitePersistence

internal actual fun createTempSqliteNowTestDbPath(prefix: String): String =
    nodeJoinPath(nodeTempDir(), "$prefix-${randomSuffix()}.db")

internal actual fun createSqliteNowTestConnectionConfig(path: String): SqliteConnectionConfig =
    SqliteConnectionConfig(persistence = NodeFileSqlitePersistence())

internal actual suspend fun deleteTempSqliteNowTestDbArtifacts(path: String) {
    nodeDeleteIfExists(path)
}

private class NodeFileSqlitePersistence : SqlitePersistence {
    override suspend fun load(dbName: String): ByteArray? {
        if (!nodeExists(dbName)) return null
        return nodeReadBytes(dbName)
    }

    override suspend fun persist(dbName: String, bytes: ByteArray) {
        nodeWriteBytes(dbName, bytes)
    }

    override suspend fun clear(dbName: String) {
        nodeDeleteIfExists(dbName)
    }
}

private fun randomSuffix(): String =
    js("Math.random().toString(16).slice(2)").unsafeCast<String>()

private fun nodeTempDir(): String =
    js("require('node:os').tmpdir()").unsafeCast<String>()

private fun nodeJoinPath(parent: String, child: String): String =
    js("require('node:path').join(parent, child)").unsafeCast<String>()

private fun nodeExists(path: String): Boolean =
    js("require('node:fs').existsSync(path)").unsafeCast<Boolean>()

private fun nodeDeleteIfExists(path: String) {
    js(
        """
        (() => {
          const fs = require('node:fs');
          if (fs.existsSync(path)) {
            fs.rmSync(path, { force: true });
          }
        })()
        """,
    )
}

private fun nodeReadBytes(path: String): ByteArray {
    val values = js("Array.from(require('node:fs').readFileSync(path))").unsafeCast<Array<Int>>()
    val result = ByteArray(values.size)
    for (i in result.indices) {
        result[i] = values[i].toByte()
    }
    return result
}

private fun nodeWriteBytes(path: String, bytes: ByteArray) {
    val values = bytes.map { (it.toInt() and 0xFF).toDouble() }.toTypedArray()
    js("require('node:fs').writeFileSync(path, Uint8Array.from(values))")
}
