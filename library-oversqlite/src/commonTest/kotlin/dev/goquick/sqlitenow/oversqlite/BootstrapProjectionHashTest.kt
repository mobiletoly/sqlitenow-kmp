package dev.goquick.sqlitenow.oversqlite

import kotlin.test.*

internal class BootstrapProjectionHashTest {
    @Test fun independentWirePageFramesAndOwnership() {
        val id = BootstrapProjectionIdentity("snapshot", "1", "a".repeat(64), 9, "b".repeat(64))
        fun frame(key: String, version: Int): ByteArray {
            val body = "[\"i\",\"0\",[\"$key\"],\"$version\",{\"id\":\"$key\"}]".encodeToByteArray()
            return byteArrayOf(0, 0, 0, 0, 0, 0, 0, body.size.toByte()) + body
        }
        val first = frame("x", 7); val second = frame("y", 8)
        assertEquals(38, first.size); assertEquals(38, second.size)
        val owner = Any()
        val writer = BootstrapWirePageHashWriter(owner, id, 2L..3L)
        assertFails { writer.add(Any(), 2, first, 26) }
        writer.add(owner, 2, first, 26); writer.add(owner, 3, second, 26)
        // Independent Python SHA256(F(header)||frame1||frame2||F(footer)).
        assertEquals("0c43f386f22333534dc26535d0388c8c249cc639a0ce8fc45f817a613bf3e24d", writer.finish(owner, 2, 52))
        assertFails { writer.finish(owner, 2, 52) }
        val failed = BootstrapWirePageHashWriter(owner, id, 2L..3L)
        assertFails { failed.add(owner, 2, first.copyOf().also { it[7] = 0 }, 26) }
        assertFails { failed.add(owner, 2, first, 26) }
        assertFails { failed.finish(owner, 0, 0) }
    }
    private val identity = BootstrapProjectionIdentity("snap-Ž", "7", "a".repeat(64),
        Long.MAX_VALUE, "b".repeat(64))
    private val key = "{\"id\":\"κ🧩\"}"
    private fun values() = listOf(TypedValue.Null, TypedValue.Integer(Long.MIN_VALUE.toString()),
        TypedValue.Integer(Long.MAX_VALUE.toString()), TypedValue.Real("5e-324"), TypedValue.Real("-0.0"),
        TypedValue.Text("Ž🧩\\\"\n"), TypedValue.Blob(byteArrayOf()), TypedValue.Blob(byteArrayOf(0, -1, 17)))

    @Test fun independentEmptyGlobalAndPageVectors() {
        // Fixed bytes computed independently with Python hashlib, struct and UTF-8 JSON.
        for (factory in listOf(::newSnapshotDigest, ::PortableSnapshotDigest)) {
            val owner = Any()
            val empty = BootstrapProjectionHashWriter(owner, identity, factory = factory)
            assertEquals("da206eeadaabf971c4b3db944c7ffcfc8f17fefeeca36abceae8392733f5e5c6", empty.finish(owner, 0, 0))
            assertEquals(123L, bootstrapProjectionRowBytes(key, values()))
            val global = BootstrapProjectionHashWriter(owner, identity, factory = factory)
            global.add(owner, 1, 3, Long.MAX_VALUE, key, values(), 123)
            assertEquals("476a300af9d0bf0b0850edb8346cf3a1e20f488dd96be190e966281e719393f6", global.finish(owner, 1, 123))
            val page = BootstrapProjectionHashWriter(owner, identity, 8L..8L, factory)
            page.add(owner, 8, 3, Long.MAX_VALUE, key, values(), 123)
            assertEquals("6827ec98385005fc53b9ac69b7215ca852678770b778a2762298a2318ee5c626", page.finish(owner, 1, 123))
        }
    }

    @Test fun independentPageProofAndChainVectors() {
        val proof = bootstrapPageProof(identity, 2, 8, 8, 1, 123, "c".repeat(64),
            "6827ec98385005fc53b9ac69b7215ca852678770b778a2762298a2318ee5c626")
        assertEquals("6234fd23d3ae00ffb6fae64a4680a1e15b6cd93dee5c0c6347cf7bdf74836ec9", proof)
        val seed = bootstrapPageChainSeed(identity)
        assertEquals("9be2767c753e9a9568ff664d7ec57bb4cac550dcb93149844e1d190397f13fb0", seed)
        assertEquals("cd39706165c824812afbb6cbc5f1abda19a7fd4f5785ba80edeeb31ae67228d3", bootstrapPageChainExtend(seed, proof))
        assertNotEquals(bootstrapPageChainExtend(seed, proof), bootstrapPageChainExtend(proof, seed))
        assertFails { bootstrapPageChainExtend("0", proof) }
        assertFails { bootstrapPageProof(identity, 0, 8, 8, 1, 123, seed, proof) }
        assertFails { bootstrapPageProof(identity, 2, 8, 9, 1, 123, seed, proof) }
    }

    @Test fun zeroNormalizationAndFieldBinding() {
        fun hash(number: String, table: Int = 0, version: Long = 1, localKey: String = key,
            selected: BootstrapProjectionIdentity = identity): String {
            val owner = Any()
            val writer = BootstrapProjectionHashWriter(owner, selected)
            writer.add(owner, 1, table, version, localKey, listOf(TypedValue.Real(number)), 8)
            return writer.finish(owner, 1, 8)
        }
        val base = hash("0")
        assertEquals(base, hash("-0.0"))
        assertNotEquals(base, hash("5e-324"))
        assertNotEquals(base, hash("0", table = 1))
        assertNotEquals(base, hash("0", version = 2))
        assertNotEquals(base, hash("0", localKey = "{}"))
        assertNotEquals(base, hash("0", selected = identity.copy(descriptorSignature = "d".repeat(64))))
        assertNotEquals(base, hash("0", selected = identity.copy(snapshotBundleSeq = Long.MAX_VALUE - 1)))
    }

    @Test fun contiguousOrdinalsTotalsAndSingleFinalization() {
        val owner = Any()
        val writer = BootstrapProjectionHashWriter(owner, identity)
        assertFailsWith<IllegalStateException> { writer.requireOwner(Any()) }
        writer.add(owner, 1, 0, 1, key, emptyList(), 0)
        writer.add(owner, 2, 0, 2, key, emptyList(), 1)
        writer.finish(owner, 2, 1)
        assertFailsWith<IllegalStateException> { writer.finish(owner, 2, 1) }
        assertFailsWith<IllegalStateException> { writer.add(owner, 3, 0, 1, key, emptyList(), 1) }
        val closed = BootstrapProjectionHashWriter(owner, identity)
        closed.close(); closed.close()
        assertFailsWith<IllegalStateException> { closed.finish(owner, 0, 0) }
        for (ordinal in listOf(0L, 2L, Long.MAX_VALUE)) {
            val invalid = BootstrapProjectionHashWriter(owner, identity)
            assertFails { invalid.add(owner, ordinal, 0, 1, key, emptyList(), 0) }
            assertFailsWith<IllegalStateException> { invalid.finish(owner, 0, 0) }
        }
        val totals = BootstrapProjectionHashWriter(owner, identity)
        assertFails { totals.finish(owner, 1, 0) }
        assertFailsWith<IllegalStateException> { totals.finish(owner, 0, 0) }
        val page = BootstrapProjectionHashWriter(owner, identity, 5L..6L)
        page.add(owner, 5, 0, 1, key, emptyList(), 0)
        assertFails { page.finish(owner, 1, 0) }
    }

    @Test fun invalidScalarsOverflowAndProviderFailureAreTerminal() {
        val owner = Any()
        for (invalid in listOf(TypedValue.Real("NaN"), TypedValue.Real("Infinity"),
            TypedValue.Integer("9223372036854775808"))) {
            val writer = BootstrapProjectionHashWriter(owner, identity)
            assertFails { writer.add(owner, 1, 0, 1, key, listOf(invalid), 0) }
            assertFailsWith<IllegalStateException> { writer.finish(owner, 0, 0) }
        }
        assertEquals(Long.MAX_VALUE, bootstrapCheckedAdd(Long.MAX_VALUE - 1, 1))
        assertFails { bootstrapCheckedAdd(Long.MAX_VALUE, 1) }
        assertFails { bootstrapCheckedAdd(-1, 0) }
        val overflow = BootstrapProjectionHashWriter(owner, identity)
        overflow.add(owner, 1, 0, 1, key, emptyList(), Long.MAX_VALUE)
        assertFails { overflow.add(owner, 2, 0, 1, key, emptyList(), 1) }
        assertFailsWith<IllegalStateException> { overflow.finish(owner, 1, Long.MAX_VALUE) }
        val failure = IllegalStateException("digest update failed")
        var fail = false
        val provider = object : SnapshotDigest() {
            override fun updateBytes(bytes: ByteArray) { if (fail) throw failure }
            override fun updateAscii(value: Char) { if (fail) throw failure }
            override fun finishDigestHex(): String = throw failure
        }
        val writer = BootstrapProjectionHashWriter(owner, identity, factory = { provider })
        fail = true
        assertSame(failure, assertFails { writer.add(owner, 1, 0, 1, key, emptyList(), 0) })
        assertFailsWith<IllegalStateException> { writer.finish(owner, 0, 0) }
    }

    @Test fun streamingRowsDoNotRetainCallerValues() {
        val owner = Any()
        val bytes = byteArrayOf(1, 2, 3)
        val streamed = BootstrapProjectionHashWriter(owner, identity)
        val reference = BootstrapProjectionHashWriter(owner, identity)
        for (ordinal in 1L..256L) {
            bytes[0] = ordinal.toByte()
            streamed.add(owner, ordinal, 0, 1, key, listOf(TypedValue.Blob(bytes)), 3)
            reference.add(owner, ordinal, 0, 1, key, listOf(TypedValue.Blob(bytes.copyOf())), 3)
        }
        bytes.fill(0)
        assertEquals(reference.finish(owner, 256, 768), streamed.finish(owner, 256, 768))
    }
}
