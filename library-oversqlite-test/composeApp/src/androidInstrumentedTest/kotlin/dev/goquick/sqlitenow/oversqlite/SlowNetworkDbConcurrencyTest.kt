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
package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.driver.bundled.BundledSQLiteDriver
import androidx.test.ext.junit.runners.AndroidJUnit4
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import io.ktor.client.HttpClient
import io.ktor.client.plugins.auth.Auth
import io.ktor.client.plugins.auth.providers.BearerTokens
import io.ktor.client.plugins.auth.providers.bearer
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.plugins.defaultRequest
import io.ktor.serialization.kotlinx.json.json
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.delay
import kotlinx.serialization.json.Json
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import java.util.UUID

@RunWith(AndroidJUnit4::class)
class SlowNetworkDbConcurrencyTest {

    @Before
    fun setUp() {
        if (skipAllOversqliteTest) {
            throw UnsupportedOperationException("(TEMPORARY setUp) Not implemented yet")
        }
    }

    private suspend fun createDb(): SafeSQLiteConnection {
        val db = SafeSQLiteConnection(BundledSQLiteDriver().open(":memory:"))
        createBusinessTables(db)
        return db
    }

    private fun delayedHttpClient(userSub: String, deviceId: String, delayMs: Long, started: CompletableDeferred<Unit>): HttpClient {
        return HttpClient {
            install(ContentNegotiation) { json(Json { ignoreUnknownKeys = true }) }
            install(Auth) {
                bearer {
                    loadTokens {
                        started.complete(Unit)
                        delay(delayMs)
                        BearerTokens(generateJwt(userSub, deviceId, "your-secret-key-change-in-production"), null)
                    }
                    refreshTokens {
                        delay(delayMs)
                        BearerTokens(generateJwt(userSub, deviceId, "your-secret-key-change-in-production"), null)
                    }
                }
            }
            defaultRequest { url("http://10.0.2.2:8080") }
        }
    }

    @Test
    fun slow_network_does_not_block_db_operations() = runBlockingTest {
        val db = createDb()

        val userId = "user-${UUID.randomUUID().toString().substring(0, 8)}"
        val deviceId = "device-slownet"

        // Create client with artificial network delay
        val networkStarted = CompletableDeferred<Unit>()
        val http = delayedHttpClient(userId, deviceId, delayMs = 1200, started = networkStarted)
        val client = DefaultOversqliteClient(
            db = db,
            config = OversqliteConfig(schema = "business", syncTables = listOf(SyncTable("users"), SyncTable("posts"))),
            http = http,
            resolver = ClientWinsResolver,
            tablesUpdateListener = { }
        )

        // Bootstrap and hydrate
        assertTrue(client.bootstrap(userId, deviceId).isSuccess)
        assertTrue(client.hydrate(includeSelf = false, limit = 500, windowed = true).isSuccess)

        // Seed a change to force an upload call
        val u1 = UUID.randomUUID().toString()
        db.execSQL("INSERT INTO users(id,name,email) VALUES('$u1','N','$u1@example.com')")

        // Start uploadOnce (will hit delayed network)
        val uploadResultDeferred = CompletableDeferred<Result<UploadSummary>>()
        coroutineScope {
            launch { uploadResultDeferred.complete(client.uploadOnce()) }
        }

        // Wait until network has started and is sleeping
        networkStarted.await()

        // While the network is sleeping, run a burst of DB writes and reads
        // Run a burst of DB operations while network is sleeping
        repeat(60) { i ->
            val id = UUID.randomUUID().toString()
            db.execSQL("INSERT INTO users(id,name,email) VALUES('$id','User$i','$id@example.com')")
            scalarLong(db, "SELECT COUNT(*) FROM users")
        }

        // Ensure upload completes eventually
        val uploadResult = uploadResultDeferred.await()
        assertTrue("Upload failed: ${uploadResult.exceptionOrNull()?.message}", uploadResult.isSuccess)

        // If we reached here, DB ops completed during network delay

        // Now test download path under slow network
        val otherDb = createDb()
        val otherClient = createSyncTestClient(otherDb, userId, "device-fast", tables = listOf("users", "posts"))
        assertTrue(otherClient.bootstrap(userId, "device-fast").isSuccess)
        assertTrue(otherClient.hydrate(includeSelf = false, limit = 500, windowed = true).isSuccess)

        // Create a server-visible change via other client
        val uid2 = UUID.randomUUID().toString()
        otherDb.execSQL("INSERT INTO users(id,name,email) VALUES('$uid2','M','$uid2@example.com')")
        assertUploadSuccess(otherClient.uploadOnce(), expectedApplied = 1)

        val networkStarted2 = CompletableDeferred<Unit>()
        val http2 = delayedHttpClient(userId, deviceId, delayMs = 1200, started = networkStarted2)
        val slowNetClient = DefaultOversqliteClient(
            db = db,
            config = OversqliteConfig(schema = "business", syncTables = listOf(SyncTable("users"), SyncTable("posts"))),
            http = http2,
            resolver = ClientWinsResolver,
            tablesUpdateListener = { }
        )

        val downloadResultDeferred = CompletableDeferred<Result<Pair<Int, Long>>>()
        coroutineScope {
            launch { downloadResultDeferred.complete(slowNetClient.downloadOnce(limit = 200, includeSelf = false)) }
        }
        networkStarted2.await()

        // DB ops during slow download
        repeat(40) { j ->
            val newId = UUID.randomUUID().toString()
            db.execSQL("INSERT INTO users(id,name,email) VALUES('$newId','L$j','$newId@example.com')")
            db.execSQL("UPDATE users SET name='L${j}x' WHERE id='$newId'")
        }

        val downloadResult = downloadResultDeferred.await()
        assertTrue("Download failed: ${downloadResult.exceptionOrNull()?.message}", downloadResult.isSuccess)
        // If we reached here, DB ops completed during slow download
    }
}
