/*
 * Copyright 2026 Toly Pochkin
 * SPDX-License-Identifier: Apache-2.0
 */
package dev.goquick.sqlitenow.oversqlite

internal actual val oversqliteFetchManagesResponseCompression: Boolean = true

internal actual fun decodeOversqliteSingleMemberGzip(encoded: ByteArray, decodedLimit: Long): ByteArray =
    error("Fetch-managed transports expose decoded response bytes")
