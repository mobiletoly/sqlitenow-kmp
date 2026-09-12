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

import android.os.Bundle
import android.os.Process
import android.util.Log
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.compose.runtime.Composable
import androidx.compose.ui.tooling.preview.Preview
import androidx.lifecycle.lifecycleScope
import dev.goquick.sqlitenow.common.resolveDatabasePath
import dev.goquick.sqlitenow.common.setupAndroidAppContext
import dev.goquick.sqlitenow.core.sqlite.use
import dev.goquick.sqlitenow.oversqlite.MergeResult
import dev.goquick.sqlitenow.oversqlite.Resolver
import dev.goquick.sqlitenow.oversqlite.openForSync
import io.ktor.client.HttpClient
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.respond
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.plugins.defaultRequest
import io.ktor.http.HttpHeaders
import io.ktor.http.headersOf
import io.ktor.serialization.kotlinx.json.json
import java.io.File
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.launch

class MainActivity : ComponentActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        setupAndroidAppContext(this.applicationContext)

        if (intent.getBooleanExtra("lifecycle_probe", false)) {
            runLifecycleProbe()
            return
        }

        setContent {
            App()
        }
    }

    private fun runLifecycleProbe() {
        val role = intent.getStringExtra("lifecycle_probe_role") ?: "unknown"
        val token = intent.getStringExtra("lifecycle_probe_token") ?: "missing"
        lifecycleScope.launch {
            val path = resolveDatabasePath(
                dbName = "lifecycle-probe.sqlite3",
                appName = "SampleSync",
            )
            if (role == "owner") {
                listOf(path, "$path-wal", "$path-shm").forEach { File(it).delete() }
            }
            val lifecycleDatabase = newSampleSyncDatabase(path)
            val lifecycleHttp = HttpClient(MockEngine { request ->
                check(request.url.encodedPath == "/sync/connect") {
                    "unexpected lifecycle request ${request.method.value} ${request.url.encodedPath}"
                }
                respond(
                    """{"resolution":"initialize_empty"}""",
                    headers = headersOf(HttpHeaders.ContentType, "application/json"),
                )
            }) {
                install(ContentNegotiation) { json() }
                defaultRequest { url("https://lifecycle-probe.invalid/") }
            }
            val opening = lifecycleDatabase.openForSync(
                config = lifecycleDatabase.buildOversqliteConfig(
                    generationId = 1L,
                    databaseId = "lifecycle-probe",
                    serviceId = "lifecycle-probe",
                    verboseLogs = true,
                ),
                httpClient = lifecycleHttp,
                resolver = Resolver { MergeResult.AcceptServer },
            )
            opening.advance()
            if (role == "owner") {
                lifecycleDatabase.connection().execSQL(
                    "INSERT OR REPLACE INTO person(id,first_name,last_name,email) " +
                        "VALUES(x'00112233445566778899AABBCCDDEEFF','SQLITENOW','Owner','lifecycle-probe@example.test')",
                )
            }
            val durable = lifecycleDatabase.connection().prepare(
                "SELECT o.phase,o.revision,o.outcome,o.writes_available," +
                    "hex(p.id),p.first_name,p.last_name,p.email " +
                    "FROM _sync_opening_state o JOIN person p ON p.id=x'00112233445566778899AABBCCDDEEFF' " +
                    "WHERE o.singleton_key=1",
            ).use { statement ->
                check(statement.step()) { "missing durable opening state or business witness" }
                listOf(
                    statement.getText(0),
                    statement.getLong(1).toString(),
                    statement.getText(2),
                    statement.getLong(3).toString(),
                    statement.getText(4),
                    statement.getText(5),
                    statement.getText(6),
                    statement.getText(7),
                ).joinToString("|")
            }
            Log.i(
                "SQLiteNowLifecycle",
                "SQLITENOW_ANDROID_DURABLE_CUT token=$token role=$role pid=${Process.myPid()} state=$durable",
            )
            awaitCancellation()
        }
    }
}

@Preview
@Composable
fun AppAndroidPreview() {
    App()
}
