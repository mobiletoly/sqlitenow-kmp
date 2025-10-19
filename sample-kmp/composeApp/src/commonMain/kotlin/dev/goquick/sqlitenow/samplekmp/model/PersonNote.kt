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
package dev.goquick.sqlitenow.samplekmp.model

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.Serializable
import kotlinx.serialization.cbor.Cbor
import kotlinx.serialization.decodeFromByteArray
import kotlinx.serialization.encodeToByteArray

@Serializable
data class PersonNote(
    val title: String,
    val content: String
) {
    companion object {
        @OptIn(ExperimentalSerializationApi::class)
        fun deserialize(bytes: ByteArray): PersonNote {
            return Cbor.decodeFromByteArray<PersonNote>(bytes)
        }

        @OptIn(ExperimentalSerializationApi::class)
        fun serialize(personNote: PersonNote): ByteArray {
            return Cbor.encodeToByteArray<PersonNote>(personNote)
        }
    }
}
