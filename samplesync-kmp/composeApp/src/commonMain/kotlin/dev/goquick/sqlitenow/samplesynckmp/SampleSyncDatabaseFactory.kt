/*
 * Copyright 2025 Toly Pochkin
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
package dev.goquick.sqlitenow.samplesynckmp

import dev.goquick.sqlitenow.samplesynckmp.db.AddressType
import dev.goquick.sqlitenow.samplesynckmp.db.NowSampleSyncDatabase
import dev.goquick.sqlitenow.samplesynckmp.db.VersionBasedDatabaseMigrations
import dev.goquick.sqlitenow.core.util.jsonDecodeListFromSqlite
import dev.goquick.sqlitenow.core.util.jsonEncodeToSqlite
import dev.goquick.sqlitenow.core.util.toRfc3339String
import dev.goquick.sqlitenow.core.util.toSqliteDate
import dev.goquick.sqlitenow.core.util.fromRfc3339String
import dev.goquick.sqlitenow.core.util.fromSqliteDate
import kotlin.time.Instant
import kotlinx.datetime.LocalDate

internal fun newSampleSyncDatabase(dbName: String): NowSampleSyncDatabase = NowSampleSyncDatabase(
    dbName,
    personAdapters = NowSampleSyncDatabase.PersonAdapters(
        birthDateToSqlValue = { it?.toSqliteDate() },
        sqlValueToBirthDate = { it?.let(LocalDate::fromSqliteDate) },
        sqlValueToUpdatedAt = { Instant.fromRfc3339String(it) },
    ),
    commentAdapters = NowSampleSyncDatabase.CommentAdapters(
        createdAtToSqlValue = { it.toRfc3339String() },
        tagsToSqlValue = { it?.jsonEncodeToSqlite() },
        sqlValueToCreatedAt = { Instant.fromRfc3339String(it) },
        sqlValueToTags = { it?.jsonDecodeListFromSqlite() ?: emptyList() },
    ),
    personAddressAdapters = NowSampleSyncDatabase.PersonAddressAdapters(
        addressTypeToSqlValue = { it.value },
        sqlValueToAddressType = { AddressType.from(it) },
    ),
    migration = VersionBasedDatabaseMigrations(),
)
