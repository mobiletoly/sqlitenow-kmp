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

import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.request.get
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.serialization.kotlinx.json.json
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.withContext
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json

private const val expectedSampleSyncServerAppName = "samplesync-server"

@Serializable
private data class SigninRequest(val user: String, val password: String, val device: String)

@Serializable
private data class SigninResponse(
    val token: String,
    @SerialName("expires_in") val expiresIn: Long,
    val user: String,
)

@Serializable
private data class ServerStatusResponse(
    @SerialName("app_name")
    val appName: String,
)

suspend fun ensureSampleSyncServer(baseUrl: String) {
    val http = HttpClient {
        install(ContentNegotiation) { json(Json { ignoreUnknownKeys = true }) }
    }
    try {
        val response = http.get("$baseUrl/status")
        check(response.status == HttpStatusCode.OK) {
            "SampleSync server check failed: HTTP ${response.status}"
        }
        val status: ServerStatusResponse = response.body()
        check(status.appName == expectedSampleSyncServerAppName) {
            "Expected SampleSync server '$expectedSampleSyncServerAppName' at $baseUrl, " +
                "but got '${status.appName}'. Start examples/samplesync_server."
        }
    } finally {
        http.close()
    }
}

private suspend fun requestJwtToken(
    baseUrl: String,
    user: String,
    sourceId: String,
    password: String,
): String {
    val http = HttpClient {
        install(ContentNegotiation) { json(Json { ignoreUnknownKeys = true }) }
    }
    return try {
        val resp: SigninResponse = http.post("$baseUrl/dummy-signin") {
            contentType(ContentType.Application.Json)
            setBody(SigninRequest(user, password, sourceId))
        }.body()
        resp.token
    } finally {
        http.close()
    }
}

// Fetches JWT from the example server's /dummy-signin endpoint.
suspend fun fetchJwt(baseUrl: String, user: String, sourceId: String, password: String = "demo"): String {
    ensureSampleSyncServer(baseUrl)
    return requestJwtToken(
        baseUrl = baseUrl,
        user = user,
        sourceId = sourceId,
        password = password,
    )
}

// Refresh needs to outlive the failed request scope; otherwise nested network calls can inherit
// a completed parent job from the original 401 response path.
suspend fun refreshJwt(baseUrl: String, user: String, sourceId: String, password: String = "demo"): String {
    return withContext(NonCancellable) {
        requestJwtToken(
            baseUrl = baseUrl,
            user = user,
            sourceId = sourceId,
            password = password,
        )
    }
}
