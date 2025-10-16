package dev.goquick.sqlitenow.core.test

import dev.goquick.sqlitenow.core.test.db.AddressType
import dev.goquick.sqlitenow.core.test.db.LibraryTestDatabase
import dev.goquick.sqlitenow.core.test.db.PersonAggregateResult
import dev.goquick.sqlitenow.core.test.db.PersonAggregateSummary
import dev.goquick.sqlitenow.core.test.db.PersonSelectOneResult
import dev.goquick.sqlitenow.core.test.db.PersonSummary
import dev.goquick.sqlitenow.core.test.db.PersonSummaryResult
import dev.goquick.sqlitenow.core.test.db.SinglePersonSummary
import dev.goquick.sqlitenow.core.test.db.VersionBasedDatabaseMigrations
import dev.goquick.sqlitenow.core.util.fromSqliteDate
import dev.goquick.sqlitenow.core.util.fromSqliteTimestamp
import dev.goquick.sqlitenow.core.util.toSqliteDate
import dev.goquick.sqlitenow.core.util.toSqliteTimestamp
import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.TimeZone
import kotlinx.datetime.toLocalDateTime
import kotlinx.serialization.json.Json
import kotlin.time.ExperimentalTime
import kotlin.time.Instant

/**
 * Shared helpers for constructing generated databases with the correct adapters configured.
 */
@OptIn(ExperimentalTime::class)
fun createLibraryTestDatabase(
    dbName: String = ":memory:",
    debug: Boolean = true,
): LibraryTestDatabase {
    return LibraryTestDatabase(
        dbName = dbName,
        migration = VersionBasedDatabaseMigrations(),
        debug = debug,
        categoryAdapters = LibraryTestDatabase.CategoryAdapters(
            sqlValueToCreatedAt = { LocalDateTime.fromSqliteTimestamp(it) }
        ),
        personAdapters = LibraryTestDatabase.PersonAdapters(
            birthDateToSqlValue = { it?.toSqliteDate() },
            sqlValueToBirthDate = { it?.let { value -> LocalDate.fromSqliteDate(value) } },
            personSummaryResultMapper = { raw: PersonSummaryResult ->
                PersonSummary(
                    id = raw.id,
                    fullName = "${raw.myFirstName} ${raw.myLastName}".trim(),
                )
            },
            personSelectOneResultMapper = { raw: PersonSelectOneResult ->
                SinglePersonSummary(
                    id = raw.id,
                    fullName = "${raw.myFirstName} ${raw.myLastName}".trim(),
                    age = 33,
                )
            },
            personAggregateResultMapper = { raw: PersonAggregateResult ->
                PersonAggregateSummary(
                    totalCount = raw.totalCount,
                    averageFirstNameLength = raw.avgFirstNameLength ?: 0.0,
                )
            },
        ),
        commentAdapters = LibraryTestDatabase.CommentAdapters(
            createdAtToSqlValue = { it.toSqliteTimestamp() },
            tagsToSqlValue = { value -> value?.let { Json.encodeToString(it) } },
            sqlValueToTags = { value -> value?.let { Json.decodeFromString<List<String>>(it) } },
        ),
        personCategoryAdapters = LibraryTestDatabase.PersonCategoryAdapters(
            sqlValueToAssignedAt = { LocalDateTime.fromSqliteTimestamp(it) }
        ),
        personAddressAdapters = LibraryTestDatabase.PersonAddressAdapters(
            addressTypeToSqlValue = { it.value },
            sqlValueToAddressType = { AddressType.from(it) },
            sqlValueToConstantTimestamp = { epochSeconds ->
                epochSeconds?.let { Instant.fromEpochSeconds(it).toLocalDateTime(TimeZone.UTC) }
            },
        ),
    )
}
