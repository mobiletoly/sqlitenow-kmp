package dev.goquick.sqlitenow.core.test

import dev.goquick.sqlitenow.core.util.fromSqliteDate
import dev.goquick.sqlitenow.core.util.fromSqliteTimestamp
import dev.goquick.sqlitenow.core.util.toSqliteDate
import dev.goquick.sqlitenow.core.util.toSqliteTimestamp
import dev.goquick.sqlitenow.core.test.db.AddressType
import dev.goquick.sqlitenow.core.test.db.LibraryTestDatabase
import dev.goquick.sqlitenow.core.test.db.PersonSelectOneResult
import dev.goquick.sqlitenow.core.test.db.PersonAggregateResult
import dev.goquick.sqlitenow.core.test.db.PersonAggregateSummary
import dev.goquick.sqlitenow.core.test.db.SinglePersonSummary
import dev.goquick.sqlitenow.core.test.db.PersonSummary
import dev.goquick.sqlitenow.core.test.db.PersonSummaryResult
import dev.goquick.sqlitenow.core.test.db.VersionBasedDatabaseMigrations
import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import kotlinx.serialization.json.Json

/**
 * Helper object to create LibraryTestDatabase instances with all required adapters.
 * This ensures consistent database setup across all test files.
 */
object TestDatabaseHelper {
    
    /**
     * Creates a LibraryTestDatabase instance with all required adapters configured.
     * Uses the working configuration from BasicCollectionTest as the reference.
     */
    fun createDatabase(dbName: String = ":memory:", debug: Boolean = true): LibraryTestDatabase {
        return LibraryTestDatabase(
            dbName = dbName,
            migration = VersionBasedDatabaseMigrations(),
            debug = debug,
            categoryAdapters = LibraryTestDatabase.CategoryAdapters(
                sqlValueToBirthDate = { it?.let { LocalDate.fromSqliteDate(it) } }
            ),
            personAdapters = LibraryTestDatabase.PersonAdapters(
                birthDateToSqlValue = { it?.toSqliteDate() },
                personSummaryResultMapper = { raw: PersonSummaryResult ->
                    PersonSummary(
                        id = raw.id,
                        fullName = "${raw.myFirstName} ${raw.myLastName}".trim()
                    )
                },
                personSelectOneResultMapper = { raw: PersonSelectOneResult ->
                    SinglePersonSummary(
                        id = raw.id,
                        fullName = "${raw.myFirstName} ${raw.myLastName}".trim(),
                        age = 33
                    )
                },
                sqlValueToTags = { it?.let { Json.decodeFromString(it) } },
                personAggregateResultMapper = { raw: PersonAggregateResult ->
                    PersonAggregateSummary(
                        totalCount = raw.totalCount,
                        averageFirstNameLength = raw.avgFirstNameLength ?: 0.0,
                    )
                },
            ),
            commentAdapters = LibraryTestDatabase.CommentAdapters(
                createdAtToSqlValue = { it.toSqliteTimestamp() },
                tagsToSqlValue = { it?.let { Json.encodeToString(it) } },
            ),
            personCategoryAdapters = LibraryTestDatabase.PersonCategoryAdapters(
                sqlValueToAssignedAt = { LocalDateTime.fromSqliteTimestamp(it) }
            ),
            personAddressAdapters = LibraryTestDatabase.PersonAddressAdapters(
                addressTypeToSqlValue = { it.value },
                sqlValueToAddressType = { AddressType.from(it) },
                sqlValueToCreatedAt = { LocalDateTime.fromSqliteTimestamp(it) },
            )
        )
    }
}
