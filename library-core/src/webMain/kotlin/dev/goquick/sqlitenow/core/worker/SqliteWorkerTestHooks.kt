/*
 * Copyright 2026 Toly Pochkin
 *
 * Licensed under the Apache License, Version 2.0.
 */
package dev.goquick.sqlitenow.core.worker

/** Internal test seam; production opens leave this at `normal`. */
internal var bundledSqliteWorkerStartupModeForTest: String = "normal"
