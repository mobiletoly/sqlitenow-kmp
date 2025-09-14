package dev.goquick.sqlitenow.oversqlite

import io.ktor.http.HttpStatusCode

class UploadHttpException(
    val status: HttpStatusCode,
    val rawBody: String,
    cause: Throwable? = null,
) : RuntimeException("Upload failed: HTTP $status - $rawBody", cause)

class DownloadHttpException(
    val status: HttpStatusCode,
    val rawBody: String,
    cause: Throwable? = null,
) : RuntimeException("Download failed: HTTP $status - $rawBody", cause)
