package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.async.step
import kotlin.test.Test
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * Typed bootstrap verifies each foreign key's installed relationships against the sealed stage in both
 * directions. The reverse direction (no installed relationship without an expected edge) must look up
 * the one staged child row that could produce a matching edge, by its primary key, instead of running
 * the expected join again for every relationship: that re-run makes the check grow with
 * relationships times parents.
 */
internal class BootstrapRelationshipCheckPlanTest : SnapshotBootstrapTestSupport() {
    private data class PlanNode(val id: Long, val parent: Long, val detail: String)

    @Test
    fun reverseRelationshipCheckLooksUpTheStagedChildByPrimaryKey() = runOwnedWebDatabaseTest {
        withBootstrapFixture { fixture ->
            val transfer = prepareBootstrapTransfer(fixture)
            val verification = transfer.admission.descriptor.tables.single { it.table == "bootstrap_children" }
                .foreignSql.single().verification
            val plan = mutableListOf<PlanNode>()
            fixture.db.withPreparedStatement("EXPLAIN QUERY PLAN $verification") { st ->
                while (st.step()) plan += PlanNode(st.getLong(0), st.getLong(1), st.getText(3))
            }
            val rendered = plan.joinToString("\n") { "${it.id}<-${it.parent}: ${it.detail}" }
            fun descendants(id: Long): List<PlanNode> = plan.filter { it.parent == id }.flatMap { listOf(it) + descendants(it.id) }

            // The reverse anti-join: a loop over the installed relationships with a correlated
            // existence check beside it.
            val reverse = plan.filter { Regex("""^(SCAN|SEARCH) r\b""").containsMatchIn(it.detail) }.flatMap { loop ->
                plan.filter { it.parent == loop.parent && it.id != loop.id && it.detail.startsWith("CORRELATED") }
            }
            if (reverse.isEmpty()) fail("plan has no reverse relationship anti-join:\n$rendered")
            for (check in reverse) {
                val reads = descendants(check.id).map { it.detail }
                assertTrue(reads.any { it.startsWith("SEARCH s ") && "row_ordinal=?" in it },
                    "the reverse check must read the staged child row by primary key:\n$rendered")
                assertTrue(reads.none { it.startsWith("SCAN") || Regex(""">\?""").containsMatchIn(it) },
                    "the reverse check must not scan or range-read per relationship:\n$rendered")
            }
        }
    }
}
