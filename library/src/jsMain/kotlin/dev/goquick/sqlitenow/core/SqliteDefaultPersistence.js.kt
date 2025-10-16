/*
 * Copyright 2025 Anatoliy Pochkin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.goquick.sqlitenow.core

import dev.goquick.sqlitenow.core.persistence.IndexedDbSqlitePersistence

internal actual fun sqliteDefaultPersistence(dbName: String): SqlitePersistence? {
    if (dbName.isBlank() || dbName.isInMemoryPath()) return null
    val storageName = "SqliteNow"
    val storeName = "sqlite-databases"
    return IndexedDbSqlitePersistence(storageName = storageName, storeName = storeName)
}

private fun String.isInMemoryPath(): Boolean {
    return this == ":memory:" || startsWith(":memory:") || startsWith(":temp:")
}
