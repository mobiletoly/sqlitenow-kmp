package dev.goquick.sqlitenow.oversqlite

import io.ktor.client.request.HttpRequestData
import io.ktor.http.content.OutgoingContent
import io.ktor.http.content.TextContent
import io.ktor.utils.io.ByteChannel
import io.ktor.utils.io.readRemaining
import io.ktor.utils.io.core.readText

internal suspend fun HttpRequestData.bodyText(): String {
    return when (val content = body) {
        is TextContent -> content.text
        is OutgoingContent.ByteArrayContent -> content.bytes().decodeToString()
        is OutgoingContent.ReadChannelContent -> content.readFrom().readRemaining().readText()
        is OutgoingContent.WriteChannelContent -> {
            val channel = ByteChannel(autoFlush = true)
            content.writeTo(channel)
            channel.close()
            channel.readRemaining().readText()
        }
        is OutgoingContent.NoContent -> ""
        else -> error("unsupported request body type ${content::class.simpleName}")
    }
}
