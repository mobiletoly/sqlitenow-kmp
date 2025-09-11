package dev.goquick.sqlitenow.samplesynckmp

import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.http.ContentType
import io.ktor.http.contentType
import io.ktor.serialization.kotlinx.json.json
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json

@Serializable
private data class SigninRequest(val user: String, val password: String, val device: String)

@Serializable
private data class SigninResponse(val token: String, val expires_in: Long, val user: String, val device: String)

// Fetches JWT from the example server's /dummy-signin endpoint.
suspend fun fetchJwt(baseUrl: String, user: String, device: String, password: String = "demo"): String {
    val http = HttpClient {
        install(ContentNegotiation) { json(Json { ignoreUnknownKeys = true }) }
    }
    val resp: SigninResponse = http.post("$baseUrl/dummy-signin") {
        contentType(ContentType.Application.Json)
        setBody(SigninRequest(user, password, device))
    }.body()
    http.close()
    return resp.token
}
