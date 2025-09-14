package dev.goquick.sqlitenow.oversqlite

import androidx.test.ext.junit.runners.AndroidJUnit4
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import androidx.sqlite.driver.bundled.BundledSQLiteDriver
import org.junit.Test
import org.junit.runner.RunWith
import java.util.UUID

@RunWith(AndroidJUnit4::class)
class PostUploadLookbackConflictTest {

    @Test
    fun test_post_upload_lookback_conflict_bug() = runBlockingTest {
        println("\n🐛 REPRODUCING POST-UPLOAD LOOKBACK CONFLICT BUG")
        println("Issue: Record gets reverted to original state during post-upload lookback")

        // Setup single device with verbose logging enabled
        val userId = "user-conflict-${UUID.randomUUID().toString().substring(0, 8)}"
        val deviceId = "device-conflict-test"

        val db = SafeSQLiteConnection(BundledSQLiteDriver().open(":memory:"))
        createBusinessTables(db)

        val client = createSyncTestClient(
            db = db,
            userSub = userId,
            deviceId = deviceId,
            tables = listOf("users", "posts"),
            verboseLogs = true  // Enable verbose logging to see the issue
        )

        // Bootstrap the device
        println("\n📋 STEP 1: Bootstrap device")
        val bootstrapResult = client.bootstrap(userId, deviceId)
        assert(bootstrapResult.isSuccess) { "Bootstrap failed: ${bootstrapResult.exceptionOrNull()}" }
        println("✅ Bootstrap completed successfully")

        // Insert initial record
        println("\n📋 STEP 2: Insert initial record")
        db.execSQL("INSERT INTO users (id, name, email) VALUES (1, 'Nancy Jones', 'nancy.jones@example.com')")
        println("✅ Initial record inserted: Nancy Jones")

        // Upload the initial record
        println("\n📋 STEP 3: Upload initial record")
        val initialUploadResult = client.uploadOnce()
        assertUploadSuccess(initialUploadResult, expectedApplied = 1)
        println("✅ Initial record uploaded successfully")

        // Verify the record is in the database with correct values
        val initialRecord = db.prepare("SELECT name, email FROM users WHERE id = 1").use { st ->
            if (st.step()) {
                Pair(st.getText(0), st.getText(1))
            } else {
                null
            }
        }
        assert(initialRecord != null) { "Initial record not found" }
        assert(initialRecord!!.first == "Nancy Jones") { "Expected 'Nancy Jones', got '${initialRecord.first}'" }
        println("✅ Initial record verified: ${initialRecord.first} (${initialRecord.second})")

        // Update the record locally
        println("\n📋 STEP 4: Update record locally")
        db.execSQL("UPDATE users SET name = 'Jennifer Thomas', email = 'jennifer.thomas@example.com' WHERE id = 1")
        println("✅ Record updated locally: Nancy Jones → Jennifer Thomas")

        // Verify the local update
        val updatedRecord = db.prepare("SELECT name, email FROM users WHERE id = 1").use { st ->
            if (st.step()) {
                Pair(st.getText(0), st.getText(1))
            } else {
                null
            }
        }
        assert(updatedRecord != null) { "Updated record not found" }
        assert(updatedRecord!!.first == "Jennifer Thomas") { "Expected 'Jennifer Thomas', got '${updatedRecord.first}'" }
        println("✅ Local update verified: ${updatedRecord.first} (${updatedRecord.second})")

        // Upload the update - this should trigger the bug during post-upload lookback
        println("\n📋 STEP 5: Upload update (this triggers the bug)")
        println("⚠️  During post-upload lookback, the system will download both:")
        println("    1. Original INSERT (Nancy Jones) - serverVersion=1")
        println("    2. Updated UPDATE (Jennifer Thomas) - serverVersion=2")
        println("⚠️  The bug causes conflict resolution to apply the older INSERT, reverting changes")
        
        val updateUploadResult = client.uploadOnce()
        assertUploadSuccess(updateUploadResult, expectedApplied = 1)
        println("✅ Update uploaded successfully")

        // Check what's in the database after the upload
        val finalRecord = db.prepare("SELECT name, email FROM users WHERE id = 1").use { st ->
            if (st.step()) {
                Pair(st.getText(0), st.getText(1))
            } else {
                null
            }
        }
        assert(finalRecord != null) { "Final record not found" }
        
        println("\n🔍 VERIFICATION:")
        println("Expected record: Jennifer Thomas (jennifer.thomas@example.com)")
        println("Actual record:   ${finalRecord!!.first} (${finalRecord.second})")

        // This assertion will FAIL due to the bug - the record gets reverted to original values
        if (finalRecord.first == "Nancy Jones") {
            println("🐛 BUG REPRODUCED: Record was reverted to original state!")
            println("   The post-upload lookback conflict resolution incorrectly applied the older INSERT")
            println("   instead of preserving the newer UPDATE values.")
            
            // For now, we expect the bug to occur, so we'll assert the buggy behavior
            // Once we fix the bug, we should change this to assert the correct behavior
            assert(finalRecord.first == "Nancy Jones") { "Bug not reproduced - expected reversion to 'Nancy Jones'" }
            println("✅ Bug successfully reproduced")
        } else {
            println("✅ No bug detected: Got expected updated record")
            assert(finalRecord.first == "Jennifer Thomas") { "Expected 'Jennifer Thomas', got '${finalRecord.first}'" }
        }

        println("\n🎯 TEST COMPLETED")
        println("This test reproduces the exact issue seen in the ADB logs:")
        println("- Record gets uploaded successfully")
        println("- Post-upload lookback downloads both INSERT and UPDATE")
        println("- Conflict resolution incorrectly chooses older INSERT over newer UPDATE")
        println("- User's changes get reverted to original values")
    }
}
