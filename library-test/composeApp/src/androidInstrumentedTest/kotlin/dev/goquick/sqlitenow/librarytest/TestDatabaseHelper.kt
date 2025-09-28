package dev.goquick.sqlitenow.librarytest

import dev.goquick.sqlitenow.core.util.fromSqliteDate
import dev.goquick.sqlitenow.core.util.fromSqliteTimestamp
import dev.goquick.sqlitenow.core.util.toSqliteDate
import dev.goquick.sqlitenow.core.util.toSqliteTimestamp
import dev.goquick.sqlitenow.librarytest.db.AddressType
import dev.goquick.sqlitenow.librarytest.db.LibraryTestDatabase
import dev.goquick.sqlitenow.librarytest.db.VersionBasedDatabaseMigrations
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
                sqlValueToTags = { it?.let { Json.decodeFromString(it) } },
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
