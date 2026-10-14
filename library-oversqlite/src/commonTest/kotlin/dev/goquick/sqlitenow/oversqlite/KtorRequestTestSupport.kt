package dev.goquick.sqlitenow.oversqlite

import io.ktor.client.request.HttpRequestData
import io.ktor.http.content.OutgoingContent
import io.ktor.http.content.TextContent
import io.ktor.utils.io.ByteChannel
import io.ktor.utils.io.readRemaining
import io.ktor.utils.io.core.readText
import io.ktor.utils.io.core.readBytes

internal suspend fun HttpRequestData.bodyText(): String {
    return when (val content = body) {
        is TextContent -> content.text
        is OutgoingContent.ByteArrayContent -> decodedBodyText(content.bytes())
        is OutgoingContent.ReadChannelContent -> decodedBodyText(content.readFrom().readRemaining().readBytes())
        is OutgoingContent.WriteChannelContent -> {
            val channel = ByteChannel(autoFlush = true)
            content.writeTo(channel)
            channel.close()
            decodedBodyText(channel.readRemaining().readBytes())
        }
        is OutgoingContent.NoContent -> ""
        else -> error("unsupported request body type ${content::class.simpleName}")
    }
}

private suspend fun HttpRequestData.decodedBodyText(bytes: ByteArray): String =
    if (headers["Content-Encoding"] == "gzip") decodeRequestGzipForTest(bytes, 4 * 1024 * 1024).decodeToString()
    else bytes.decodeToString()

internal expect suspend fun decodeRequestGzipForTest(encoded: ByteArray, decodedLimit: Int): ByteArray
