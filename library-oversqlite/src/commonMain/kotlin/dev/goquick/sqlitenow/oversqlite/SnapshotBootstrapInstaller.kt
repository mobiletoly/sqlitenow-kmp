package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.async.step
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive

/** Runs only inside the existing final apply writer; publication belongs to its caller. */
internal class SnapshotBootstrapInstaller(
    private val db: SafeSQLiteConnection,
    private val config: OversqliteConfig,
    private val operations: OversqliteClientOperationController,
) {
    suspend fun install(
        stage: TypedSnapshotStage, admission: BootstrapSchemaAdmission.Eligible, metadata: ValidatedConfig,
        targetContext: Long, cache: StatementCache,
    ): Set<String> {
        cache.requireConnectionOwner(db)
        require(db.inTransaction()) { "bootstrap installation requires the final writer" }
        admission.pin.requireCurrent(db, metadata)
        val descriptor = stage.descriptor
        require(admission.descriptor.signature == descriptor.signature && targetContext > 0)
        require(db.scalarLong("""SELECT count(*) FROM _sync_apply_state WHERE singleton_key=1 AND apply_mode=1
            AND baseline_mode='row' AND bootstrap_operation='' AND bootstrap_target_context=0
            AND bootstrap_revision_low=0 AND bootstrap_revision_high=0""") == 1L) { "bootstrap apply owner is unavailable" }
        requireCleanSnapshotWork(db)
        require(bootstrapManagedStateEmpty(db, metadata, cache)) { "bootstrap final state is no longer empty" }
        require(db.withPreparedStatement("""SELECT count(*) FROM _sync_provenance_contexts c
            CROSS JOIN _sync_contract_identity i CROSS JOIN _sync_attachment_state a CROSS JOIN _sync_operation_state o
            WHERE c.context_id=? AND i.singleton_key=1 AND a.singleton_key=1 AND o.singleton_key=1
            AND c.database_id=i.database_id AND c.service_id=i.service_id AND c.generation_id=i.generation_id
            AND c.generation_fingerprint=i.fingerprint AND c.admission='admitted' AND c.local_version=i.local_version
            AND c.physical_shape=i.physical_shape AND c.account_binding='attached' AND c.account_id!=''
            AND c.account_id=CASE WHEN o.target_user_id!='' THEN o.target_user_id ELSE a.attached_user_id END
            AND c.origin_source_id=?""", cache) {
            it.bindLong(1, targetContext); it.bindText(2, stage.source); require(it.step()); it.getLong(0)
        } == 1L) { "bootstrap target context differs from admitted operation" }
        val operation = OversqliteOperationStateStore(db).loadState()
        val audit = BootstrapStageAudit.read(db, descriptor, operation, cache)
        val measured = audit.measured
        val workspace = OversqliteWorkspace(db, config)
        val baseRevision = db.scalarLong("SELECT baseline_revision FROM _sync_conflict_state WHERE singleton_key=1")
        require(baseRevision >= 0 && measured.rows < Long.MAX_VALUE - baseRevision) { "bootstrap baseline revision range overflow" }
        val work = bootstrapInstallationWork(descriptor, measured)
        return operations.admitSnapshotFinalTransition {
            operations.reserveSyncWork("bootstrap-final", work, readyAtomic = true)
            val manifest = stage.verifyForApply(cache, audit)
            // Preflight every table's largest admitted SQL page before any business effect.
            val pageRows = descriptor.tables.map { binding ->
                val rowBytes = bootstrapCheckedAdd(4096 + 1024L * binding.columns.size,
                    workspace.scale("bootstrap-install", measured.maxBody, 24))
                var rows = minOf(config.snapshotApplyBatchRows, maxOf(1L, manifest.tables[binding.slot].rows).coerceAtMost(Int.MAX_VALUE.toLong()).toInt())
                val byBytes = if (measured.maxBody == 0L) rows else (config.snapshotApplyBatchBytes / measured.maxBody).coerceAtMost(Int.MAX_VALUE.toLong()).toInt()
                require(byBytes > 0 || manifest.tables[binding.slot].rows == 0L) { "bootstrap SQL row exceeds admitted page target" }
                rows = minOf(rows, maxOf(1, byBytes))
                val base = workspace.readDurableBase("bootstrap-install", cache)
                bootstrapWork("snapshot.bootstrap.quota_reads")
                while (true) {
                    try {
                        workspace.checkCapturedReadBase("bootstrap-install", base, WorkspaceCharge(16 + rows.toLong(), workspace.scale("bootstrap-install", rowBytes, rows.toLong())))
                        break
                    } catch (capacity: SyncWorkspaceLimitException) {
                        if (capacity.suppressedExceptions.isNotEmpty() || rows == 1) throw capacity
                        rows = maxOf(1, rows / 2)
                    }
                }
                rows
            }
            syncPhase("snapshot.bootstrap.foreign_keys") {
            for (binding in descriptor.tables) for (foreign in binding.foreignSql) {
                currentCoroutineContext().ensureActive()
                require(db.withPreparedStatement(foreign.membership, cache) {
                    it.bindText(1, stage.session.snapshotId); require(it.step()); it.getLong(0)
                } == 0L) { "bootstrap stage has a missing foreign-key principal" }
                bootstrapWork("snapshot.bootstrap.foreign_key_checks")
            }
            }
            if (measured.rows > 0) {
                db.withPreparedStatement("""UPDATE _sync_apply_state SET baseline_mode='bootstrap',bootstrap_operation=?,
                    bootstrap_target_context=?,bootstrap_revision_low=?,bootstrap_revision_high=?
                    WHERE singleton_key=1 AND apply_mode=1 AND baseline_mode='row'""", cache) {
                    it.bindText(1, stage.session.snapshotId); it.bindLong(2, targetContext)
                    it.bindLong(3, baseRevision + 1); it.bindLong(4, baseRevision + measured.rows); it.step()
                }
                require(db.scalarLong("SELECT changes()") == 1L)
                db.withPreparedStatement("UPDATE _sync_conflict_state SET baseline_revision=? WHERE singleton_key=1 AND baseline_revision=?", cache) {
                    it.bindLong(1, baseRevision + measured.rows); it.bindLong(2, baseRevision); it.step()
                }
                require(db.scalarLong("SELECT changes()") == 1L) { "bootstrap baseline revision owner changed" }
            }
            syncPhase("row_installation", measured.rows, measured.logicalBytes) {
                for (binding in descriptor.tables) {
                    var after = 0L; var installed = 0L
                    while (installed < manifest.tables[binding.slot].rows) {
                        currentCoroutineContext().ensureActive()
                        val page = db.withPreparedStatement(binding.installPageSql, cache) {
                            it.bindText(1, stage.session.snapshotId); it.bindLong(2, after); it.bindLong(3, pageRows[binding.slot].toLong())
                            require(it.step()); it.getLong(0) to it.getLong(1)
                        }
                        require(page.first > 0 && page.second > after)
                        syncPhase("snapshot.bootstrap.business_installation", page.first) {
                        db.withPreparedStatement(binding.businessSql, cache) {
                            it.bindText(1, stage.session.snapshotId); it.bindLong(2, after); it.bindLong(3, page.second); it.step()
                        }
                        require(db.scalarLong("SELECT changes()") == page.first) { "bootstrap business row count differs" }
                        }
                        syncPhase("snapshot.bootstrap.baseline_installation", page.first) {
                        db.withPreparedStatement(binding.baselineSql, cache) {
                            it.bindLong(1, stage.session.snapshotBundleSeq); it.bindLong(2, targetContext); it.bindLong(3, baseRevision)
                            it.bindText(4, stage.session.snapshotId); it.bindLong(5, after); it.bindLong(6, page.second); it.step()
                        }
                        require(db.scalarLong("SELECT changes()") == page.first) { "bootstrap baseline row count differs" }
                        }
                        syncPhase("snapshot.bootstrap.relationship_installation", page.first) {
                        for (foreign in binding.foreignSql) db.withPreparedStatement(foreign.insert, cache) {
                            currentCoroutineContext().ensureActive()
                            it.bindLong(1, baseRevision); it.bindText(2, stage.session.snapshotId)
                            it.bindLong(3, after); it.bindLong(4, page.second); it.step()
                        }
                        }
                        installed = bootstrapCheckedAdd(installed, page.first); after = page.second
                        bootstrapWork("snapshot.bootstrap.installation_rows", page.first)
                        bootstrapWork("snapshot.bootstrap.install_pages")
                    }
                    require(installed == manifest.tables[binding.slot].rows)
                }
            }
            syncPhase("final_validation", measured.rows, measured.logicalBytes) {
                syncPhase("snapshot.bootstrap.final_verification", measured.rows, measured.logicalBytes) {
                verifyInstalled(stage, baseRevision, targetContext, measured.rows, cache)
                }
            }
            db.execSQL("""UPDATE _sync_apply_state SET baseline_mode='row',bootstrap_operation='',bootstrap_target_context=0,
                bootstrap_revision_low=0,bootstrap_revision_high=0 WHERE singleton_key=1""")
            descriptor.tables.filter { manifest.tables[it.slot].rows > 0 }.mapTo(linkedSetOf()) { it.table }
        }
    }

    internal suspend fun verifyInstalled(stage: TypedSnapshotStage, baseRevision: Long, targetContext: Long, rows: Long, cache: StatementCache) {
        require(db.scalarLong("SELECT count(*) FROM _sync_row_state") == rows) { "bootstrap authoritative scope differs" }
        var expectedEdges = 0L
        for (binding in stage.descriptor.tables) {
            currentCoroutineContext().ensureActive()
            require(db.withPreparedStatement(binding.businessVerificationSql, cache) {
                it.bindText(1, stage.session.snapshotId); it.bindText(2, stage.session.snapshotId); require(it.step()); it.getLong(0)
            } == 0L) { "bootstrap business values differ from sealed stage" }
            require(db.withPreparedStatement(binding.authoritativeVerificationSql, cache) {
                it.bindText(1, stage.session.snapshotId); it.bindLong(2, baseRevision); it.bindLong(3, targetContext)
                it.bindLong(4, stage.session.snapshotBundleSeq); it.bindLong(5, targetContext); require(it.step()); it.getLong(0)
            } == 0L) { "bootstrap authoritative values differ from sealed stage" }
            bootstrapWork("snapshot.bootstrap.table_checks")
            for (foreign in binding.foreignSql) {
                require(db.withPreparedStatement(foreign.verification, cache) {
                    it.bindLong(1, baseRevision); it.bindText(2, stage.session.snapshotId); require(it.step())
                    expectedEdges = bootstrapCheckedAdd(expectedEdges, it.getLong(1)); it.getLong(0)
                } == 0L) { "bootstrap relationship projection differs" }
                bootstrapWork("snapshot.bootstrap.foreign_key_checks")
            }
        }
        require(db.scalarLong("SELECT EXISTS(SELECT 1 FROM _sync_relationships WHERE view!='server')") == 0L)
        require(db.scalarLong("SELECT count(*) FROM _sync_relationships") == expectedEdges) { "bootstrap relationship scope differs" }
        require(db.scalarLong("SELECT count(*) FROM pragma_foreign_key_check") == 0L) { "bootstrap foreign-key check failed" }
    }
}

/** Section 8 minimum, with conservative indexed work for all admitted business indexes. */
internal fun bootstrapInstallationWork(descriptor: SnapshotDescriptor, measured: BootstrapStageMeasure): Long {
    val n = measured.rows
    val foreign = descriptor.tables.sumOf { it.metadata.foreignKeys.size.toLong() }
    val unique = descriptor.tables.sumOf { it.metadata.uniqueKeys.sumOf { key -> key.columns.size.toLong() } }
    var v = 0L; var r = 0L
    for (binding in descriptor.tables) {
        v = snapshotWorkAdd(v, bootstrapWorkScale(measured.tables[binding.slot], binding.columns.size.toLong()))
        r = snapshotWorkAdd(r, bootstrapWorkScale(measured.tables[binding.slot], binding.metadata.foreignKeys.size.toLong()))
    }
    var bits = 0L; var value = n
    while (value > 0) { bits++; value = value ushr 1 }
    val j = descriptor.tables.maxOfOrNull { it.metadata.foreignKeys.size.toLong() +
        maxOf(it.metadata.indexWorkComponents, it.metadata.uniqueKeys.sumOf { key -> key.columns.size.toLong() }) } ?: 0
    var work = snapshotWorkAdd(512, bootstrapWorkScale(descriptor.tables.size.toLong() + foreign + unique, 64))
    for (component in listOf(bootstrapWorkScale(n, 256), bootstrapWorkScale(v, 64), bootstrapWorkScale(r, 32),
        bootstrapWorkScale(measured.frames, 2), bootstrapWorkScale(measured.bodies, 8),
        bootstrapWorkScale(bootstrapWorkScale(bootstrapWorkScale(n, 32), 1 + bits), 1 + j))) work = snapshotWorkAdd(work, component)
    return work
}

private fun bootstrapWorkScale(value: Long, weight: Long): Long {
    require(value >= 0 && weight >= 0)
    return if (weight == 0L) 0 else snapshotWorkScale(value, weight)
}
