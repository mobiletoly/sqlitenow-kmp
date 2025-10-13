package dev.goquick.sqlitenow.core

import dev.goquick.sqlitenow.core.sqlite.SqliteConnection

internal actual fun exportConnectionBytes(connection: SqliteConnection): ByteArray? {
    return connection.exportToByteArray()
}
