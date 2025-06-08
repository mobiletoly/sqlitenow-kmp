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
