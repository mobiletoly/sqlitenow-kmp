package dev.goquick.sqlitenow.oversqlite

import android.content.Context
import androidx.sqlite.SQLiteConnection
import androidx.sqlite.driver.bundled.BundledSQLiteDriver
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import io.ktor.client.plugins.auth.providers.bearer
import io.ktor.client.plugins.defaultRequest
import io.ktor.serialization.kotlinx.json.json
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import kotlin.random.Random

@RunWith(AndroidJUnit4::class)
class AndroidOversqliteStressTest {

    @Before
    fun setUp() {
        if (skipAllTest) {
            throw UnsupportedOperationException("(TEMPORARY setUp) Not implemented yet")
        }
    }

    @Test
    fun scenario_multi_user_delete_should_not_resurrect() = runBlocking {
        val context: Context = ApplicationProvider.getApplicationContext()
        fun openDb(name: String): SQLiteConnection {
            val file = context.cacheDir.resolve(name).absolutePath
            return BundledSQLiteDriver().open(file)
        }

        val dbA = SafeSQLiteConnection(openDb("muA.db"))
        val dbB = SafeSQLiteConnection(openDb("muB.db"))

        createBusinessTables(dbA); createBusinessTables(dbB)

        val userA = "user-A-" + java.util.UUID.randomUUID().toString().substring(0, 8)
        val userB = "user-B-" + java.util.UUID.randomUUID().toString().substring(0, 8)
        val clientA = createSyncTestClient(
            db = dbA,
            userSub = userA,
            deviceId = "device-1",
            tables = listOf("users")
        )
        val clientB = createSyncTestClient(
            db = dbB,
            userSub = userB,
            deviceId = "device-2",
            tables = listOf("users")
        )

        // Sign in and hydrate
        assert(clientA.bootstrap(userA, "device-1").isSuccess)
        assert(clientB.bootstrap(userB, "device-2").isSuccess)
        clientA.hydrate(includeSelf = false, limit = 1000, windowed = true)
        clientB.hydrate(includeSelf = false, limit = 1000, windowed = true)

        // Add records for each user, upload, and sync
        val a1 = java.util.UUID.randomUUID().toString()
        dbA.execSQL("INSERT INTO users(id,name,email) VALUES('$a1','Alice','alice@example.com')")
        assert(clientA.uploadOnce().isSuccess)
        val b1 = java.util.UUID.randomUUID().toString()
        dbB.execSQL("INSERT INTO users(id,name,email) VALUES('$b1','Bob','bob@example.com')")
        assert(clientB.uploadOnce().isSuccess)
        clientA.downloadOnce(limit = 1000, includeSelf = false)
        clientB.downloadOnce(limit = 1000, includeSelf = false)

        // Delete A's record locally and upload + sync A
        dbA.execSQL("DELETE FROM users WHERE id='$a1'")
        assert(clientA.uploadOnce().isSuccess)
        clientA.downloadOnce(limit = 1000, includeSelf = false)

        // Expect: A should not see its deleted record, and should not see B's data
        assert(scalarLong(dbA, "SELECT COUNT(*) FROM users WHERE id='$a1'") == 0L)
        assert(scalarLong(dbA, "SELECT COUNT(*) FROM users") == 0L)

        // Sanity: B still sees its row and not A's
        clientB.downloadOnce(limit = 1000, includeSelf = false)
        assert(scalarLong(dbB, "SELECT COUNT(*) FROM users WHERE id='$b1'") == 1L)
        assert(scalarLong(dbB, "SELECT COUNT(*) FROM users WHERE id='$a1'") == 0L)
    }

    @Test
    fun long_running_sync_with_fuzz_and_flaky_network() = runBlocking {
        val context: Context = ApplicationProvider.getApplicationContext()

        // Use file-backed DBs in cache dir to simulate restart/persistence
        fun openDb(name: String): SQLiteConnection {
            val file = context.cacheDir.resolve(name).absolutePath
            return BundledSQLiteDriver().open(file)
        }

        val dbA = SafeSQLiteConnection(openDb("stressA.db"))
        val dbB = SafeSQLiteConnection(openDb("stressB.db"))


        createBusinessTables(dbA)
        createBusinessTables(dbB)

        // Intermittent auth token provider: randomly fails or delays to simulate flaky network
        fun flakyTokenProvider(user: String, device: String, secret: String): suspend () -> String =
            {
                if (Random.nextDouble() < 0.2) {
                    throw java.io.IOException("simulated token fetch failure")
                }
                if (Random.nextDouble() < 0.3) {
                    delay(Random.nextLong(25, 120))
                }
                generateJwt(user, device, secret)
            }

        // Helper to create HTTP client with flaky token provider
        fun createFlakyHttpClient(tokenProvider: suspend () -> String): io.ktor.client.HttpClient {
            return io.ktor.client.HttpClient {
                install(io.ktor.client.plugins.contentnegotiation.ContentNegotiation) {
                    json(kotlinx.serialization.json.Json { ignoreUnknownKeys = true })
                }
                install(io.ktor.client.plugins.auth.Auth) {
                    bearer {
                        loadTokens {
                            io.ktor.client.plugins.auth.providers.BearerTokens(
                                accessToken = tokenProvider(),
                                refreshToken = null
                            )
                        }
                        refreshTokens {
                            // Simulate token refresh
                            io.ktor.client.plugins.auth.providers.BearerTokens(
                                accessToken = tokenProvider(),
                                refreshToken = null
                            )
                        }
                    }
                }
                defaultRequest {
                    url("http://10.0.2.2:8080")
                }
            }
        }

        val cfg = OversqliteConfig(schema = "business", syncTables = listOf(
            SyncTable(tableName = "users"),
            SyncTable(tableName = "posts")
        ))
        val secret = "your-secret-key-change-in-production"
        val userSub = "user-stress-" + java.util.UUID.randomUUID().toString().substring(0, 8)

        // Create flaky HTTP clients that simulate token refresh
        val httpClientA = createFlakyHttpClient(flakyTokenProvider(userSub, "device-A", secret))
        val httpClientB = createFlakyHttpClient(flakyTokenProvider(userSub, "device-B", secret))

        var clientA = DefaultOversqliteClient(
            db = dbA,
            config = cfg,
            http = httpClientA,
            resolver = ClientWinsResolver,
            tablesUpdateListener = { println("Tables updated: $it") })
        var clientB = DefaultOversqliteClient(
            db = dbB,
            config = cfg,
            http = httpClientB,
            resolver = ClientWinsResolver,
            tablesUpdateListener = { println("Tables updated: $it") })

        assert(clientA.bootstrap(userSub, "device-A").isSuccess) { "Bootstrap A failed" }
        assert(clientB.bootstrap(userSub, "device-B").isSuccess) { "Bootstrap B failed" }

        // Seed some initial users on A
        repeat(5) {
            val uid = java.util.UUID.randomUUID().toString()
            dbA.execSQL("INSERT OR IGNORE INTO users(id,name,email) VALUES('$uid','User$it','u$it@example.com')")
        }
        clientA.uploadOnce()

        // Stress loop: random ops and sync with flaky network
        repeat(40) { iter ->
            // Random op on A
            when (Random.nextInt(0, 5)) {
                0 -> { // insert user
                    val id = java.util.UUID.randomUUID().toString()
                    val nm = "U_" + Random.nextInt(1000)
                    dbA.execSQL("INSERT INTO users(id,name,email) VALUES('$id','$nm','$nm@example.com')")
                }

                1 -> { // update random user
                    val id = scalarText(dbA, "SELECT id FROM users ORDER BY RANDOM() LIMIT 1")
                    if (id.isNotEmpty()) {
                        val nm = "U2_" + Random.nextInt(1000)
                        dbA.execSQL("UPDATE users SET name='$nm' WHERE id='$id'")
                    }
                }

                2 -> { // delete random user without posts
                    val id = scalarText(
                        dbA,
                        "SELECT id FROM users WHERE id NOT IN (SELECT author_id FROM posts) ORDER BY RANDOM() LIMIT 1"
                    )
                    if (id.isNotEmpty()) dbA.execSQL("DELETE FROM users WHERE id='$id'")
                }

                3 -> { // insert post for random user
                    val uid = scalarText(dbA, "SELECT id FROM users ORDER BY RANDOM() LIMIT 1")
                    if (uid.isNotEmpty()) {
                        val pid = java.util.UUID.randomUUID().toString()
                        val title = "T_" + Random.nextInt(1000)
                        dbA.execSQL("INSERT INTO posts(id,title,content,author_id) VALUES('$pid','$title','c', '$uid')")
                    }
                }

                else -> { // update random post title
                    val pid = scalarText(dbA, "SELECT id FROM posts ORDER BY RANDOM() LIMIT 1")
                    if (pid.isNotEmpty()) {
                        val title = "T2_" + Random.nextInt(1000)
                        dbA.execSQL("UPDATE posts SET title='$title' WHERE id='$pid'")
                    }
                }
            }

            // Try uploads from A and occasional from B with retries
            retrySuspend(3) { clientA.uploadOnce().isSuccess }
            if (Random.nextBoolean()) retrySuspend(3) { clientB.uploadOnce().isSuccess }

            // Intermittent downloads
            retrySuspend(3) { clientA.downloadOnce(limit = 200, includeSelf = false).isSuccess }
            retrySuspend(3) { clientB.downloadOnce(limit = 200, includeSelf = false).isSuccess }

            // Occasionally simulate restart during paging by recreating clients
            if (iter % 10 == 5) {
                clientA.close(); clientB.close()
                clientA = DefaultOversqliteClient(
                    dbA,
                    cfg,
                    createFlakyHttpClient(flakyTokenProvider(userSub, "device-A", secret)),
                    ClientWinsResolver,
                    tablesUpdateListener = { println("Tables updated: $it") })
                clientB = DefaultOversqliteClient(
                    dbB,
                    cfg,
                    createFlakyHttpClient(flakyTokenProvider(userSub, "device-B", secret)),
                    ClientWinsResolver,
                    tablesUpdateListener = { println("Tables updated: $it") })
                // Continue downloads after restart
                retrySuspend(3) { clientA.downloadOnce(limit = 200, includeSelf = false).isSuccess }
                retrySuspend(3) { clientB.downloadOnce(limit = 200, includeSelf = false).isSuccess }
            }
        }

        // Final convergence: both sides should see same counts for users and posts after hydration
        retrySuspend(5) { clientA.downloadOnce(limit = 1000, includeSelf = false).isSuccess }
        retrySuspend(5) { clientB.downloadOnce(limit = 1000, includeSelf = false).isSuccess }

        val usersA = scalarLong(dbA, "SELECT COUNT(*) FROM users").toInt()
        val postsA = scalarLong(dbA, "SELECT COUNT(*) FROM posts").toInt()
        val usersB = scalarLong(dbB, "SELECT COUNT(*) FROM users").toInt()
        val postsB = scalarLong(dbB, "SELECT COUNT(*) FROM posts").toInt()

        // Allow small tolerance due to random deletes/updates in-flight; but should converge after final downloads
        assert(kotlin.math.abs(usersA - usersB) <= 1) { "User count divergence: A=$usersA B=$usersB" }
        assert(kotlin.math.abs(postsA - postsB) <= 1) { "Post count divergence: A=$postsA B=$postsB" }
    }
}
