package dev.goquick.sqlitenow.core.test

import kotlin.js.Promise
import kotlinx.coroutines.await

internal actual suspend fun cleanupGeneratedWorkerMigrationState(dbName: String) {
    cleanupGeneratedWorkerMigrationStateJs(dbName).await()
}

internal actual fun generatedWorkerLegacyMigrationBrowserAvailable(): Boolean =
    js("typeof window !== 'undefined'").unsafeCast<Boolean>()

@Suppress("UNUSED_PARAMETER")
private fun cleanupGeneratedWorkerMigrationStateJs(dbName: String): Promise<dynamic> =
    js(
        """
        (() => {
          if (typeof window === 'undefined') return Promise.resolve();
          const bytes = new TextEncoder().encode(dbName);
          return crypto.subtle.digest('SHA-256', bytes).then((digest) => {
            const hash = Array.from(new Uint8Array(digest))
              .map((byte) => byte.toString(16).padStart(2, '0')).join('');
            const target = "sqlitenow-worker-v1-" + hash + ".sqlite3";
            return navigator.storage.getDirectory().then((root) =>
              Promise.all(
                [target, target + ".health.json", target + ".migration.json"].map(
                  (name) => root.removeEntry(name).catch((error) => {
                    if (!error || error.name !== 'NotFoundError') throw error;
                  })
                )
              )
            );
          });
        })()
        """,
    ).unsafeCast<Promise<dynamic>>()
