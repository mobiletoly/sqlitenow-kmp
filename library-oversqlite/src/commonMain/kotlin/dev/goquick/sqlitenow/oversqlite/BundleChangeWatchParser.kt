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
package dev.goquick.sqlitenow.oversqlite

import kotlinx.serialization.SerializationException
import kotlinx.serialization.json.Json

internal class BundleChangeWatchSseParser(
    private val json: Json,
) {
    private var eventName: String = ""
    private val dataLines = mutableListOf<String>()

    fun accept(line: String): List<BundleChangeEvent> {
        if (line.isEmpty()) {
            return dispatch()
        }
        if (line.startsWith(":")) {
            return emptyList()
        }

        val separator = line.indexOf(':')
        val field = if (separator < 0) line else line.substring(0, separator)
        var value = if (separator < 0) "" else line.substring(separator + 1)
        if (value.startsWith(" ")) {
            value = value.substring(1)
        }

        when (field) {
            "event" -> eventName = value
            "data" -> dataLines += value
        }
        return emptyList()
    }

    fun finish() {
        eventName = ""
        dataLines.clear()
    }

    private fun dispatch(): List<BundleChangeEvent> {
        val shouldEmit = eventName == "bundle"
        val data = dataLines.joinToString("\n")
        finish()
        if (!shouldEmit) {
            return emptyList()
        }
        require(data.isNotEmpty()) { "bundle change event is missing data" }
        val event = try {
            json.decodeFromString(BundleChangeEvent.serializer(), data)
        } catch (error: SerializationException) {
            throw IllegalArgumentException("bundle change event data is malformed", error)
        } catch (error: IllegalArgumentException) {
            throw IllegalArgumentException("bundle change event data is malformed", error)
        }
        require(event.bundleSeq > 0) {
            "bundle change event bundle_seq ${event.bundleSeq} must be positive"
        }
        return listOf(event)
    }
}

internal fun parseBundleChangeEventLines(
    lines: Iterable<String>,
    json: Json,
): List<BundleChangeEvent> {
    val parser = BundleChangeWatchSseParser(json)
    val events = mutableListOf<BundleChangeEvent>()
    for (line in lines) {
        events += parser.accept(line)
    }
    parser.finish()
    return events
}
