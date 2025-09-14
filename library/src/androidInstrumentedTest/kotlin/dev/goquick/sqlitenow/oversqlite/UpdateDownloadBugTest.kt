package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.driver.bundled.BundledSQLiteDriver
import androidx.test.ext.junit.runners.AndroidJUnit4
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Test
import org.junit.runner.RunWith
import java.util.UUID

/**
 * Test to reproduce the bug where:
 * 1. Device creates a record and uploads it successfully
 * 2. Device modifies the same record locally and uploads the changes
 * 3. When device downloads, it gets the original record instead of the updated one
 */
@RunWith(AndroidJUnit4::class)
class UpdateDownloadBugTest {

    @Test
    fun reproduce_update_download_bug() = runBlockingTest {
        println("\n🐛 REPRODUCING UPDATE DOWNLOAD BUG")
        println("Issue: Device gets original record instead of updated record after download")

        // Setup single device (the bug can manifest even with one device)
        val userId = "user-bug-${UUID.randomUUID().toString().substring(0, 8)}"
        val deviceId = "device-bug-test"

        val db = SafeSQLiteConnection(BundledSQLiteDriver().open(":memory:"))
        createBusinessTables(db)

        val client = createSyncTestClient(
            db = db,
            userSub = userId,
            deviceId = deviceId,
            tables = listOf("users", "posts"),
            verboseLogs = true  // Enable verbose logging to test the new feature
        )

        // Bootstrap the device
        println("\n📋 STEP 1: Bootstrap device")
        assert(client.bootstrap(userId, deviceId).isSuccess) { "Bootstrap failed" }
        assert(client.hydrate(includeSelf = false, limit = 1000, windowed = true).isSuccess) { "Hydrate failed" }

        // Create initial record
        println("\n📋 STEP 2: Create initial record")
        val recordId = UUID.randomUUID().toString()
        val originalName = "John Doe"
        val originalEmail = "john.doe@example.com"
        
        db.prepare("INSERT INTO users (id, name, email) VALUES (?, ?, ?)").use { st ->
            st.bindText(1, recordId)
            st.bindText(2, originalName)
            st.bindText(3, originalEmail)
            st.step()
        }
        
        println("✅ Created user: $originalName ($originalEmail)")
        
        // Verify record exists locally
        val initialUser = getUserFromDb(db, recordId)
        assertNotNull("Initial user should exist in local DB", initialUser)
        assertEquals("Initial name should match", originalName, initialUser!!.second)
        assertEquals("Initial email should match", originalEmail, initialUser.third)

        // Upload initial record
        println("\n📋 STEP 3: Upload initial record")
        val initialUploadResult = client.uploadOnce()
        assertUploadSuccess(initialUploadResult, expectedApplied = 1)
        println("✅ Initial record uploaded successfully")

        // Modify the record locally
        println("\n📋 STEP 4: Modify record locally")
        val updatedName = "Jane Smith"
        val updatedEmail = "jane.smith@example.com"
        
        db.prepare("UPDATE users SET name = ?, email = ? WHERE id = ?").use { st ->
            st.bindText(1, updatedName)
            st.bindText(2, updatedEmail)
            st.bindText(3, recordId)
            st.step()
        }
        
        println("✅ Updated user: $updatedName ($updatedEmail)")
        
        // Verify record is updated locally
        val updatedUser = getUserFromDb(db, recordId)
        assertNotNull("Updated user should exist in local DB", updatedUser)
        assertEquals("Updated name should match", updatedName, updatedUser!!.second)
        assertEquals("Updated email should match", updatedEmail, updatedUser.third)

        // Upload the update
        println("\n📋 STEP 5: Upload the update")
        val updateUploadResult = client.uploadOnce()
        assertUploadSuccess(updateUploadResult, expectedApplied = 1)
        println("✅ Update uploaded successfully")

        // Now download to see if we get the updated record
        println("\n📋 STEP 6: Download to verify updated record")
        val downloadResult = client.downloadOnce(limit = 1000, includeSelf = false)
        assertDownloadSuccess(downloadResult)
        println("✅ Download completed")

        // Check what we have in the database after download
        val finalUser = getUserFromDb(db, recordId)
        assertNotNull("Final user should exist in local DB", finalUser)

        println("\n🔍 VERIFICATION:")
        println("Original record: $originalName ($originalEmail)")
        println("Expected record: $updatedName ($updatedEmail)")
        println("Actual record:   ${finalUser!!.second} (${finalUser.third})")

        // This is where the bug manifests - we expect the updated record but get the original
        if (finalUser.second == originalName && finalUser.third == originalEmail) {
            println("🐛 BUG REPRODUCED: Got original record instead of updated record!")
            println("❌ Expected: $updatedName ($updatedEmail)")
            println("❌ Got:      ${finalUser.second} (${finalUser.third})")
        } else if (finalUser.second == updatedName && finalUser.third == updatedEmail) {
            println("✅ No bug detected: Got expected updated record")
        } else {
            println("❓ Unexpected result: Got neither original nor expected record")
        }

        // For now, let's assert what we expect (this might fail if the bug exists)
        assertEquals("Should have updated name after download", updatedName, finalUser.second)
        assertEquals("Should have updated email after download", updatedEmail, finalUser.third)
        
        println("✅ TEST PASSED: Update download works correctly")
    }

    private suspend fun getUserFromDb(db: SafeSQLiteConnection, id: String): Triple<String, String, String>? {
        return db.prepare("SELECT id, name, email FROM users WHERE id = ?").use { st ->
            st.bindText(1, id)
            if (st.step()) {
                Triple(st.getText(0), st.getText(1), st.getText(2))
            } else {
                null
            }
        }
    }
}
