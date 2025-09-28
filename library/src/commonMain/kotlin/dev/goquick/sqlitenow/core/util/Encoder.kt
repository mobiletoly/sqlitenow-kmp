package dev.goquick.sqlitenow.core.util

import kotlinx.serialization.json.Json

/**
 * Encodes a collection to a JSON string for storage in SQLite.
 * This is useful for storing collections in TEXT columns.
 */
inline fun <reified T> Collection<T>.jsonEncodeToSqlite(): String {
    return Json.encodeToString(this)
}

/**
 * Decodes a JSON string from SQLite back to a list.
 * This is the counterpart to jsonEncodeToSqlite().
 */
inline fun <reified T> String.jsonDecodeListFromSqlite(): List<T> {
    return Json.decodeFromString(this)
}
