package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.driver.bundled.BundledSQLiteDriver
import androidx.test.ext.junit.runners.AndroidJUnit4
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import kotlinx.coroutines.delay
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import java.util.UUID
import kotlin.random.Random

@RunWith(AndroidJUnit4::class)
class BlobSyncTest {

    @Before
    fun setUp() {
        if (skipAllTest) {
            throw UnsupportedOperationException("(TEMPORARY setUp) Not implemented yet")
        }
    }

    private suspend fun createDb(): SafeSQLiteConnection {
        val db = SafeSQLiteConnection(BundledSQLiteDriver().open(":memory:"))
        // Create a table with a BLOB column
        db.execSQL("""
            CREATE TABLE files (
              id BLOB PRIMARY KEY NOT NULL DEFAULT (randomblob(16)),
              name TEXT NOT NULL,
              data BLOB
            )
            """.trimIndent()
        )
        db.execSQL("""
            CREATE TABLE file_reviews (
              id BLOB PRIMARY KEY NOT NULL DEFAULT (randomblob(16)),
              file_id BLOB NOT NULL,
              review TEXT NOT NULL,
              FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE CASCADE
            )
        """.trimIndent())
        return db
    }

    private suspend fun createClient(db: SafeSQLiteConnection, user: String, device: String): DefaultOversqliteClient {
        val client = createSyncTestClient(
            db = db,
            userSub = user,
            deviceId = device,
            tables = listOf("files", "file_reviews")
        )
        assertTrue(client.bootstrap(user, device).isSuccess)
        return client
    }

    @Test
    fun blob_column_roundtrip_sync() = runBlockingTest {
        val user = "user-blob-" + UUID.randomUUID().toString().substring(0, 8)
        val dbA = createDb()
        val clientA = createClient(dbA, user, "dev-A")

        // Insert binary content on A (let SQLite generate BLOB UUID with randomblob(16))
        val name = "file.bin"
        val bytes = Random.nextBytes(1024)

        dbA.prepare("INSERT INTO files(name,data) VALUES(?,?)").use { st ->
            st.bindText(1, name)
            st.bindBlob(2, bytes)
            st.step()
        }

        // Get the generated BLOB ID
        var generatedId: ByteArray? = null
        dbA.prepare("SELECT id FROM files WHERE name=?").use { st ->
            st.bindText(1, name)
            if (st.step()) {
                generatedId = st.getBlob(0)
            }
        }

        // Build upload payload locally and verify base64 encoding for BLOB column
        val nextChangeId = scalarLong(dbA, "SELECT next_change_id FROM _sync_client_info LIMIT 1")
        val uploader = SyncUploader(
            http = createAuthenticatedHttpClient(user, "dev-A"),
            config = OversqliteConfig(schema = "business", syncTables = listOf(SyncTable("files"), SyncTable("file_reviews"))),
            resolver = ServerWinsResolver,
            upsertBusinessFromPayload = { _,_,_,_ -> },
            updateRowMeta = { _,_,_,_,_ -> },
            ioDispatcher = kotlinx.coroutines.Dispatchers.IO
        )
        val prepared = uploader.prepareUpload(dbA, nextChangeId)
        assertTrue(prepared.changes.isNotEmpty())
        val change = prepared.changes.first { it.table == "files" }
        val payload = change.payload as kotlinx.serialization.json.JsonObject
        val encoded = payload["data"]!!.toString().trim('"')
        val decoded = kotlin.io.encoding.Base64.decode(encoded)
        assertArrayEquals(bytes, decoded)

        // Update data and verify next payload
        val bytes2 = Random.nextBytes(2048)
        dbA.prepare("UPDATE files SET data=? WHERE id=?").use { st ->
            st.bindBlob(1, bytes2)
            st.bindBlob(2, generatedId!!)
            st.step()
        }
        val prepared2 = uploader.prepareUpload(dbA, nextChangeId + 1)
        val change2 = prepared2.changes.first { it.table == "files" }
        val payload2 = change2.payload as kotlinx.serialization.json.JsonObject
        val encoded2 = payload2["data"]!!.toString().trim('"')
        val decoded2 = kotlin.io.encoding.Base64.decode(encoded2)
        assertArrayEquals(bytes2, decoded2)

        // Upload coalesced pending change(s) (INSERT after UPDATE stays INSERT due to coalescing)
        assertTrue(clientA.uploadOnce().isSuccess)

        // Use a fresh database for the same user but a different device to download materialized state

        val dbC = createDb()
        val clientC = createClient(dbC, user, "dev-C")
        // Hydrate snapshot for this user on device C (should materialize files row if the server registers 'files')
        assertTrue(clientC.hydrate(includeSelf = false, limit = 500, windowed = true).isSuccess)

        // Verify row exists and matches the latest blob content exactly after hydration
        val countC = scalarLong(dbC, "SELECT COUNT(*) FROM files WHERE name='$name'")
        assertEquals(1, countC.toInt())
        val fetchedC = dbC.prepare("SELECT name, data FROM files WHERE id=?").use { st ->
            st.bindBlob(1, generatedId!!)
            if (st.step()) Pair(st.getText(0), st.getBlob(1)) else null
        }
        assertTrue("Hydrated row not found", fetchedC != null)
        assertEquals(name, fetchedC!!.first)
        assertArrayEquals(bytes2, fetchedC.second)
    }

    @Test
    fun blob_foreign_key_roundtrip_sync() = runBlockingTest {
        val user = "user-blob-fk-${UUID.randomUUID()}"

        val dbA = createDb()
        val clientA = createClient(dbA, user, "dev-A")

        // Insert file with BLOB primary key on device A
        val fileName = "document.pdf"
        val fileData = Random.nextBytes(512)

        dbA.prepare("INSERT INTO files(name,data) VALUES(?,?)").use { st ->
            st.bindText(1, fileName)
            st.bindBlob(2, fileData)
            st.step()
        }

        // Get the generated BLOB file ID
        var fileId: ByteArray? = null
        dbA.prepare("SELECT id FROM files WHERE name=?").use { st ->
            st.bindText(1, fileName)
            if (st.step()) {
                fileId = st.getBlob(0)
            }
        }

        // Insert file review with BLOB foreign key
        val reviewText = "Great document, very useful!"
        dbA.prepare("INSERT INTO file_reviews(file_id,review) VALUES(?,?)").use { st ->
            st.bindBlob(1, fileId!!)
            st.bindText(2, reviewText)
            st.step()
        }

        // Get the generated review ID
        var reviewId: ByteArray? = null
        dbA.prepare("SELECT id FROM file_reviews WHERE review=?").use { st ->
            st.bindText(1, reviewText)
            if (st.step()) {
                reviewId = st.getBlob(0)
            }
        }

        // Upload changes from device A
        val uploadResult = clientA.uploadOnce()
        println("DEBUG: Upload result: ${uploadResult.isSuccess}")
        if (uploadResult.isFailure) {
            println("DEBUG: Upload error: ${uploadResult.exceptionOrNull()}")
        }
        assertTrue("Upload failed", uploadResult.isSuccess)

        // Create device B and download changes
        val dbB = createDb()
        val clientB = createClient(dbB, user, "dev-B")
        val hydrateResult = clientB.hydrate(includeSelf = false, limit = 500, windowed = true)
        println("DEBUG: Hydrate result: ${hydrateResult.isSuccess}")
        if (hydrateResult.isFailure) {
            println("DEBUG: Hydrate error: ${hydrateResult.exceptionOrNull()}")
        }
        assertTrue("Hydrate failed", hydrateResult.isSuccess)

        // Verify file exists on device B
        val fileCountB = scalarLong(dbB, "SELECT COUNT(*) FROM files WHERE name='$fileName'")
        println("DEBUG: File count on device B: $fileCountB")
        assertEquals("File not found on device B", 1, fileCountB.toInt())

        // Verify review exists on device B with correct foreign key relationship
        val reviewCountB = scalarLong(dbB, "SELECT COUNT(*) FROM file_reviews WHERE review='$reviewText'")
        println("DEBUG: Review count on device B: $reviewCountB")
        assertEquals("Review not found on device B", 1, reviewCountB.toInt())

        // Verify foreign key relationship works by joining tables
        val joinResult = dbB.prepare("""
            SELECT f.name, fr.review
            FROM files f
            JOIN file_reviews fr ON f.id = fr.file_id
            WHERE f.name = ?
        """).use { st ->
            st.bindText(1, fileName)
            if (st.step()) Pair(st.getText(0), st.getText(1)) else null
        }

        assertTrue("Join result not found", joinResult != null)
        assertEquals(fileName, joinResult!!.first)
        assertEquals(reviewText, joinResult.second)

        // Upload changes from device B
        assertTrue(clientB.uploadOnce().isSuccess)
        delay(2000)

        // Update review on device B and sync back
        val updatedReview = "Updated: Even better after second read!"

        // Get the review ID from device B (it might be different after sync)
        var reviewIdB: ByteArray? = null
        dbB.prepare("SELECT id FROM file_reviews WHERE review=?").use { st ->
            st.bindText(1, reviewText)
            if (st.step()) {
                reviewIdB = st.getBlob(0)
            }
        }
        println("DEBUG: Review ID on device B: ${reviewIdB?.let { it.joinToString("") { "%02x".format(it) } }}")

        // Check the server version in _sync_row_meta for this review on device B
        val reviewIdHex = reviewIdB?.joinToString("") { "%02x".format(it) }?.lowercase()
        val serverVersionB = if (reviewIdHex != null) {
            dbB.prepare("SELECT server_version FROM _sync_row_meta WHERE table_name='file_reviews' AND pk_uuid=?").use { st ->
                st.bindText(1, reviewIdHex)
                if (st.step()) st.getLong(0) else null
            }
        } else null
        println("DEBUG: Server version for review on device B: $serverVersionB (looking for $reviewIdHex)")

        dbB.prepare("UPDATE file_reviews SET review=? WHERE id=?").use { st ->
            st.bindText(1, updatedReview)
            st.bindBlob(2, reviewIdB!!)
            st.step()
        }

        // Upload changes from device B
        val uploadResultB = clientB.uploadOnce()
        println("DEBUG: Upload from device B result: ${uploadResultB.isSuccess}")
        if (uploadResultB.isFailure) {
            println("DEBUG: Upload from device B error: ${uploadResultB.exceptionOrNull()}")
        } else {
            val summary = uploadResultB.getOrNull()
            println("DEBUG: Upload summary: $summary")
            println("DEBUG: Applied: ${summary?.applied}, Conflicts: ${summary?.conflict}, Invalid: ${summary?.invalid}")
            if (summary?.conflict ?: 0 > 0) {
                println("DEBUG: There were conflicts in the upload!")
            }
        }
        assertTrue("Upload from device B failed", uploadResultB.isSuccess)

        // Download updates on device A
        val downloadResultA = clientA.downloadOnce()
        println("DEBUG: Download to device A result: ${downloadResultA.isSuccess}")
        if (downloadResultA.isFailure) {
            println("DEBUG: Download to device A error: ${downloadResultA.exceptionOrNull()}")
        }
        assertTrue("Download to device A failed", downloadResultA.isSuccess)

        // Verify updated review on device A (use the original review ID from device A)
        val finalReview = dbA.prepare("SELECT review FROM file_reviews WHERE review LIKE 'Updated:%' OR review LIKE 'Great document%'").use { st ->
            if (st.step()) st.getText(0) else null
        }

        assertEquals(updatedReview, finalReview)
    }

    @Test
    fun blob_foreign_key_simple_sync() = runBlockingTest {
        val user = "user-blob-fk-simple-${UUID.randomUUID()}"

        val dbA = createDb()
        val clientA = createClient(dbA, user, "dev-A")

        // Insert file with BLOB primary key on device A
        val fileName = "simple.txt"
        val fileData = Random.nextBytes(256)

        dbA.prepare("INSERT INTO files(name,data) VALUES(?,?)").use { st ->
            st.bindText(1, fileName)
            st.bindBlob(2, fileData)
            st.step()
        }

        // Get the generated BLOB file ID
        var fileId: ByteArray? = null
        dbA.prepare("SELECT id FROM files WHERE name=?").use { st ->
            st.bindText(1, fileName)
            if (st.step()) {
                fileId = st.getBlob(0)
            }
        }

        // Insert file review with BLOB foreign key
        val reviewText = "Simple test review"
        dbA.prepare("INSERT INTO file_reviews(file_id,review) VALUES(?,?)").use { st ->
            st.bindBlob(1, fileId!!)
            st.bindText(2, reviewText)
            st.step()
        }

        // Upload changes from device A
        val uploadResult = clientA.uploadOnce()
        println("DEBUG: Upload result: ${uploadResult.isSuccess}")
        if (uploadResult.isSuccess) {
            val summary = uploadResult.getOrNull()
            println("DEBUG: Upload summary: $summary")
        }
        assertTrue("Upload failed", uploadResult.isSuccess)

        // Create device B and download changes
        val dbB = createDb()
        val clientB = createClient(dbB, user, "dev-B")
        val hydrateResult = clientB.hydrate(includeSelf = false, limit = 500, windowed = true)
        assertTrue("Hydrate failed", hydrateResult.isSuccess)

        // Verify both file and review exist on device B
        val fileCountB = scalarLong(dbB, "SELECT COUNT(*) FROM files WHERE name='$fileName'")
        assertEquals("File not synced", 1, fileCountB.toInt())

        val reviewCountB = scalarLong(dbB, "SELECT COUNT(*) FROM file_reviews WHERE review='$reviewText'")
        assertEquals("Review not synced", 1, reviewCountB.toInt())

        // Verify foreign key relationship works by joining tables
        val joinResult = dbB.prepare("""
            SELECT f.name, fr.review
            FROM files f
            JOIN file_reviews fr ON f.id = fr.file_id
            WHERE f.name = ?
        """).use { st ->
            st.bindText(1, fileName)
            if (st.step()) Pair(st.getText(0), st.getText(1)) else null
        }

        assertTrue("Join failed - foreign key relationship broken", joinResult != null)
        assertEquals("File name mismatch", fileName, joinResult!!.first)
        assertEquals("Review text mismatch", reviewText, joinResult.second)

        println("✅ BLOB foreign key sync test passed!")
    }
}
