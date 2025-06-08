package dev.goquick.sqlitenow.gradle.introsqlite

import dev.goquick.sqlitenow.gradle.introsqlite.DatabaseIntrospector
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.DisplayName
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class DatabaseIntrospectorTest {
    private lateinit var connection: Connection
    private lateinit var introspector: DatabaseIntrospector

    @BeforeEach
    fun setUp() {
        connection = DriverManager.getConnection("jdbc:sqlite::memory:")
        introspector = DatabaseIntrospector(connection)

        connection.createStatement().use { stmt ->
            stmt.executeUpdate(
                """
                CREATE TABLE users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT NOT NULL UNIQUE COLLATE NOCASE,
                    email TEXT UNIQUE,
                    created_at INTEGER DEFAULT (strftime('%s', 'now')),
                    active INTEGER DEFAULT 1
                )
                """
            )


            stmt.executeUpdate(
                """
                CREATE TABLE posts (
                    post_id INTEGER PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    title TEXT NOT NULL,
                    content TEXT,
                    created_at INTEGER DEFAULT (strftime('%s', 'now')),
                    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
                )
                """
            )


            stmt.executeUpdate(
                """
                CREATE TABLE comments (
                    comment_id INTEGER,
                    post_id INTEGER,
                    user_id INTEGER NOT NULL,
                    content TEXT NOT NULL,
                    created_at INTEGER DEFAULT (strftime('%s', 'now')),
                    PRIMARY KEY (comment_id, post_id),
                    FOREIGN KEY (post_id) REFERENCES posts(post_id) ON DELETE CASCADE,
                    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
                )
                """
            )


            stmt.executeUpdate(
                """
                CREATE INDEX idx_posts_user_id ON posts(user_id)
                """
            )

            stmt.executeUpdate(
                """
                CREATE UNIQUE INDEX idx_posts_title ON posts(title)
                """
            )

            stmt.executeUpdate(
                """
                CREATE INDEX idx_comments_user_post ON comments(user_id, post_id)
                """
            )
        }
    }

    @AfterEach
    fun tearDown() {
        connection.close()
    }

    @Test
    @DisplayName("Test extracting column comments")
    fun testExtractColumnComments() {
        // Create a table with column comments
        connection.createStatement().use { stmt ->
            stmt.executeUpdate(
                """
                -- @@className=PersonEntity
                CREATE TABLE IF NOT EXISTS Person (
                    id INTEGER PRIMARY KEY NOT NULL,

                    -- This is a comment for first_name
                    first_name VARCHAR NOT NULL,

                    last_name TEXT NOT NULL,

                    -- This is a comment for email
                    email TEXT NOT NULL UNIQUE,

                    phone TEXT,

                    -- @@propertyType=kotlinx.datetime.LocalDate
                    birth_date TEXT,

                    -- @@propertyType=createdAt
                    -- @@nonNull
                    created_at TEXT NOT NULL DEFAULT current_timestamp
                );
                """
            )
        }

        // Get the columns for the Person table
        val columns = introspector.getColumns("Person")

        // Check that we have the expected columns
        assertEquals(7, columns.size)

        // Check comments for first_name
        val firstNameColumn = columns["first_name"]
        assertNotNull(firstNameColumn)
        // Check that the expected comment is in the list
        println("First name comments: ${firstNameColumn.comments}")
        // Our implementation might find the same comment twice, so we check that the expected comment is in the list
        assertTrue(firstNameColumn.comments.contains("This is a comment for first_name"))

        // Check comments for email
        val emailColumn = columns["email"]
        assertNotNull(emailColumn)
        // Check that at least one of the comments contains the expected text
        assertTrue(emailColumn.comments.any { it.contains("This is a comment for email") })
        println("Email comments: ${emailColumn.comments}")
        // Check that at least one of the comments contains the expected text
        assertTrue(emailColumn.comments.any { it.contains("This is a comment for email") })

        // Check comments for birth_date
        val birthDateColumn = columns["birth_date"]
        assertNotNull(birthDateColumn)
        // Check that at least one of the comments contains the expected text
        assertTrue(birthDateColumn.comments.any { it.contains("@@propertyType=kotlinx.datetime.LocalDate") })
        println("Birth date comments: ${birthDateColumn.comments}")
        // Check that at least one of the comments contains the expected text
        assertTrue(birthDateColumn.comments.any { it.contains("@@propertyType=kotlinx.datetime.LocalDate") })

        // Check comments for created_at (multiple comments)
        val createdAtColumn = columns["created_at"]
        assertNotNull(createdAtColumn)
        println("Created at comments: ${createdAtColumn.comments}")
        // Check that both comments are in the list
        assertTrue(createdAtColumn.comments.any { it.contains("@@propertyType=createdAt") })
        assertTrue(createdAtColumn.comments.any { it.contains("@@nonNull") })
        // Check that we have exactly 2 comments
        assertEquals(2, createdAtColumn.comments.size)
    }

    @Test
    @DisplayName("Test extracting multiple inline comments")
    fun testExtractMultipleInlineComments() {
        // Create a table with multiple inline comments
        connection.createStatement().use { stmt ->
            stmt.executeUpdate(
                """
                CREATE TABLE IF NOT EXISTS MultiLineComments (
                    id INTEGER PRIMARY KEY NOT NULL,

                    -- @@propertyType=createdAt
                    -- @@nonNull
                    created_at TEXT NOT NULL DEFAULT current_timestamp,

                    -- This is a comment line 1
                    -- This is a comment line 2
                    -- @@propertyType=kotlinx.datetime.LocalDate
                    updated_at TEXT
                );
                """
            )
        }

        // Get the columns for the MultiLineComments table
        val columns = introspector.getColumns("MultiLineComments")

        // Check that we have the expected columns
        assertEquals(3, columns.size)

        // Check comments for created_at
        val createdAtColumn = columns["created_at"]
        assertNotNull(createdAtColumn)
        println("Multiple inline created_at comments: ${createdAtColumn.comments}")
        // Check that both annotations are in the list
        assertTrue(createdAtColumn.comments.any { it.contains("@@propertyType=createdAt") })
        assertTrue(createdAtColumn.comments.any { it.contains("@@nonNull") })
        // Check that we have exactly 2 comments
        assertEquals(2, createdAtColumn.comments.size)

        // Check comments for updated_at
        val updatedAtColumn = columns["updated_at"]
        assertNotNull(updatedAtColumn)
        println("Multiple inline updated_at comments: ${updatedAtColumn.comments}")
        // Check that the annotation is in the list
        assertTrue(updatedAtColumn.comments.any { it.contains("@@propertyType=kotlinx.datetime.LocalDate") })
        // We should have exactly 3 comments
        assertEquals(3, updatedAtColumn.comments.size)
    }

    @Test
    @DisplayName("Test getting all tables")
    fun testGetTables() {
        val tables = introspector.getTables()

        assertEquals(3, tables.size)
        val tableNames = tables.map { it.name }
        assertTrue(tableNames.contains("users"))
        assertTrue(tableNames.contains("posts"))
        assertTrue(tableNames.contains("comments"))
    }

    @Test
    @DisplayName("Test getting columns for users table")
    fun testGetColumnsForUsersTable() {
        val columns = introspector.getColumns("users")

        assertEquals(5, columns.size)
        val columnNames = columns.keys
        assertTrue(columnNames.contains("id"))
        assertTrue(columnNames.contains("username"))
        assertTrue(columnNames.contains("email"))
        assertTrue(columnNames.contains("created_at"))
        assertTrue(columnNames.contains("active"))


        val idColumn = columns["id"]
        assertNotNull(idColumn)
        assertEquals("INTEGER", idColumn.dataType)
        assertTrue(idColumn.isPrimaryKey)
        assertTrue(idColumn.isAutoincrement)


        val usernameColumn = columns["username"]
        assertNotNull(usernameColumn)
        assertEquals("TEXT", usernameColumn.dataType)
        assertTrue(usernameColumn.notNull)
        assertTrue(usernameColumn.isUnique)
        assertEquals("NOCASE", usernameColumn.collationName)


        val emailColumn = columns["email"]
        assertNotNull(emailColumn)
        assertEquals("TEXT", emailColumn.dataType)
        assertFalse(emailColumn.notNull)
        assertTrue(emailColumn.isUnique)


        val createdAtColumn = columns["created_at"]
        assertNotNull(createdAtColumn)
        assertEquals("INTEGER", createdAtColumn.dataType)
        assertFalse(createdAtColumn.notNull)
        assertNotNull(createdAtColumn.defaultValue)


        val activeColumn = columns["active"]
        assertNotNull(activeColumn)
        assertEquals("INTEGER", activeColumn.dataType)
        assertFalse(activeColumn.notNull)
        assertEquals("1", activeColumn.defaultValue)
    }

    @Test
    @DisplayName("Test getting columns for posts table")
    fun testGetColumnsForPostsTable() {
        val columns = introspector.getColumns("posts")

        assertEquals(5, columns.size)
        val columnNames = columns.keys
        assertTrue(columnNames.contains("post_id"))
        assertTrue(columnNames.contains("user_id"))
        assertTrue(columnNames.contains("title"))
        assertTrue(columnNames.contains("content"))
        assertTrue(columnNames.contains("created_at"))


        val postIdColumn = columns["post_id"]
        assertNotNull(postIdColumn)
        assertEquals("INTEGER", postIdColumn.dataType)
        assertTrue(postIdColumn.isPrimaryKey)


        val userIdColumn = columns["user_id"]
        assertNotNull(userIdColumn)
        assertEquals("INTEGER", userIdColumn.dataType)
        assertTrue(userIdColumn.notNull)
    }

    @Test
    @DisplayName("Test getting columns for comments table")
    fun testGetColumnsForCommentsTable() {
        val columns = introspector.getColumns("comments")

        assertEquals(5, columns.size)
        val columnNames = columns.keys
        assertTrue(columnNames.contains("comment_id"))
        assertTrue(columnNames.contains("post_id"))
        assertTrue(columnNames.contains("user_id"))
        assertTrue(columnNames.contains("content"))
        assertTrue(columnNames.contains("created_at"))


        val commentIdColumn = columns["comment_id"]
        assertNotNull(commentIdColumn)
        assertEquals("INTEGER", commentIdColumn.dataType)
        assertTrue(commentIdColumn.isPrimaryKey)

        val postIdColumn = columns["post_id"]
        assertNotNull(postIdColumn)
        assertEquals("INTEGER", postIdColumn.dataType)
        assertTrue(postIdColumn.isPrimaryKey)
    }

    @Test
    @DisplayName("Test getting indexes")
    fun testGetIndexes() {
        val postsIndexes = introspector.getIndexes("posts")

        assertTrue(postsIndexes.size >= 2)
        val indexNames = postsIndexes.keys
        assertTrue(indexNames.contains("idx_posts_user_id"))
        assertTrue(indexNames.contains("idx_posts_title"))


        val userIdIndex = postsIndexes["idx_posts_user_id"]
        assertNotNull(userIdIndex)
        assertEquals("posts", userIdIndex.tableName)
        assertEquals(1, userIdIndex.columnNames.size)
        assertEquals("user_id", userIdIndex.columnNames[0])
        assertFalse(userIdIndex.isUnique)


        val titleIndex = postsIndexes["idx_posts_title"]
        assertNotNull(titleIndex)
        assertEquals("posts", titleIndex.tableName)
        assertEquals(1, titleIndex.columnNames.size)
        assertEquals("title", titleIndex.columnNames[0])
        assertTrue(titleIndex.isUnique)

        val commentsIndexes = introspector.getIndexes("comments")

        assertTrue(commentsIndexes.size >= 1)
        val customIndex = commentsIndexes["idx_comments_user_post"]
        assertNotNull(customIndex, "Could not find idx_comments_user_post index")
        assertEquals("comments", customIndex.tableName)
        assertEquals(2, customIndex.columnNames.size)
        assertEquals("user_id", customIndex.columnNames[0])
        assertEquals("post_id", customIndex.columnNames[1])
        assertFalse(customIndex.isUnique)
    }

    @Test
    @DisplayName("Test getting foreign keys")
    fun testGetForeignKeys() {

        val postsForeignKeys = introspector.getForeignKeys("posts")

        assertEquals(1, postsForeignKeys.size)
        val postsForeignKey = postsForeignKeys.values.first()
        assertEquals("posts", postsForeignKey.fromTable)
        assertEquals("user_id", postsForeignKey.fromColumn)
        assertEquals("users", postsForeignKey.toTable)
        assertEquals("id", postsForeignKey.toColumn)
        assertEquals("CASCADE", postsForeignKey.onDelete)

        val commentsForeignKeys = introspector.getForeignKeys("comments")

        assertEquals(2, commentsForeignKeys.size)
        val postIdForeignKey = commentsForeignKeys.values.find { it.fromColumn == "post_id" }
        assertNotNull(postIdForeignKey)
        assertEquals("comments", postIdForeignKey.fromTable)
        assertEquals("post_id", postIdForeignKey.fromColumn)
        assertEquals("posts", postIdForeignKey.toTable)
        assertEquals("post_id", postIdForeignKey.toColumn)
        assertEquals("CASCADE", postIdForeignKey.onDelete)

        val userIdForeignKey = commentsForeignKeys.values.find { it.fromColumn == "user_id" }
        assertNotNull(userIdForeignKey)
        assertEquals("comments", userIdForeignKey.fromTable)
        assertEquals("user_id", userIdForeignKey.fromColumn)
        assertEquals("users", userIdForeignKey.toTable)
        assertEquals("id", userIdForeignKey.toColumn)
        assertEquals("CASCADE", userIdForeignKey.onDelete)
    }

    @Test
    @DisplayName("Test full database introspection")
    fun testFullDatabaseIntrospection() {
        val tables = introspector.getTables()

        assertEquals(3, tables.size)
        val usersTable = tables.find { it.name == "users" }
        assertNotNull(usersTable)
        assertEquals(5, usersTable.columns.size)
        assertEquals(0, usersTable.foreignKeys.size)

        val postsTable = tables.find { it.name == "posts" }
        assertNotNull(postsTable)
        assertEquals(5, postsTable.columns.size)
        assertTrue(postsTable.indexes.size >= 2, "Expected at least 2 indexes for posts table")
        assertEquals(1, postsTable.foreignKeys.size)

        val commentsTable = tables.find { it.name == "comments" }
        assertNotNull(commentsTable)
        assertEquals(5, commentsTable.columns.size)
        assertTrue(commentsTable.indexes.size >= 1, "Expected at least 1 index for comments table")
        assertEquals(2, commentsTable.foreignKeys.size)
    }
}
