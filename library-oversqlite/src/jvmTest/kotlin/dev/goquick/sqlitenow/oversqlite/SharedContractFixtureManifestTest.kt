package dev.goquick.sqlitenow.oversqlite

import java.nio.file.Files
import java.nio.file.Path
import kotlin.test.AfterTest
import java.security.MessageDigest
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlin.io.path.createDirectories
import kotlin.io.path.exists
import kotlin.io.path.isRegularFile
import kotlin.io.path.readText
import kotlin.io.path.writeText
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertTrue

class SharedContractFixtureManifestTest {
    private val ownedCorpora = mutableListOf<Path>()

    @AfterTest
    fun removeOwnedCorpora() {
        for (root in ownedCorpora) {
            Files.walk(root).use { paths ->
                paths.sorted(Comparator.reverseOrder()).forEach { Files.delete(it) }
            }
            check(!root.exists()) { "owned corpus cleanup failed" }
            println("CORPUS_CLEANUP $root removed")
        }
    }

    @Test
    fun sharedCorpusRequiresItsCompleteManifestBeforeFixtureReads() {
        assertTrue(oversqliteContractFixture("README.md").isRegularFile())
    }

    @Test
    fun everyCurrentClassificationHasARegisteredSemanticConsumer() {
        val classifications = Json.parseToJsonElement(
            oversqliteContractFixture("CLASSIFICATIONS.json").readText(),
        ).jsonObject
        val currentEntries = classifications.getValue("currentProtocolFixtures").jsonArray
            .map { it.jsonPrimitive.content }
        val current = currentEntries.toSet()
        val registered = mapOf(
            "local-apply/unique-final-state.json" to "SharedFinalStateUniqueFixtureTest",
            "local-apply/a1.json" to "SharedLocalApplyFixtureTest",
            "protocol-handshake/connect.json" to "SharedProtocolHandshakeFixtureTest",
            "protocol-http/requests.json" to "SharedProtocolHttpRequestFixtureTest",
            "watch/basic.json" to "SharedBundleChangeWatchFixtureTest",
            "wire/compact-rows.json" to "SharedWireContractFixtureTest",
 "wire/compression.json" to "SharedWireContractFixtureTest",
 "wire/limits.json" to "SharedWireContractFixtureTest",
 "recovery/protocol.json" to "SharedRecoveryProtocolFixtureTest",
        )
        assertEquals(currentEntries.size, current.size, "current fixture classifications must not contain duplicates")
        assertEquals(registered.keys, current)
        registered.forEach { (path, consumer) ->
            assertTrue(oversqliteContractFixture(path).exists(), "$consumer claims missing active path $path")
        }
    }

    @Test
    fun manifestValidationFailsClosed() {
        val hash = { body: String ->
            MessageDigest.getInstance("SHA-256").digest(body.encodeToByteArray())
                .joinToString("") { "%02x".format(it) }
        }
        val files = mapOf("a.json" to "a", "b.json" to "b")
        val valid = listOf("${hash("a")}  a.json", "${hash("b")}  b.json")
        validateManifestPinnedCorpus(writeCorpus(valid, files))

        mapOf(
            "unsorted" to listOf(valid[1], valid[0]),
            "duplicate" to listOf(valid[0], valid[0]),
            "traversal" to listOf("${hash("a")}  ../a.json", valid[1]),
            "uppercase" to listOf("${hash("a").uppercase()}  a.json", valid[1]),
            "stale" to listOf("${"0".repeat(64)}  a.json", valid[1]),
            "missing" to listOf(valid[0]),
        ).forEach { (_, records) ->
            assertFails { validateManifestPinnedCorpus(writeCorpus(records, files)) }
        }
        assertFails {
            validateManifestPinnedCorpus(writeCorpus(valid, files + ("c.json" to "c")))
        }
    }

    private fun writeCorpus(records: List<String>, files: Map<String, String>) =
        Files.createTempDirectory("oversqlite-contract-corpus").also { root ->
            ownedCorpora.add(root)
            files.forEach { (name, body) ->
                val path = root.resolve(name)
                path.parent.createDirectories()
                path.writeText(body)
            }
            root.resolve("MANIFEST.sha256").writeText(records.joinToString("\n", postfix = "\n"))
        }
}
