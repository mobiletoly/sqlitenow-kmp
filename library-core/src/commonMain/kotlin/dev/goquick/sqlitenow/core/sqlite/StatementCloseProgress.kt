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
package dev.goquick.sqlitenow.core.sqlite

import kotlin.concurrent.atomics.AtomicReference
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlinx.coroutines.CompletableDeferred

/** One physical statement close, shared by synchronous callers and the suspending owner. */
@OptIn(ExperimentalAtomicApi::class)
internal class StatementCloseProgress {
    private class Attempt(val thread: Any) {
        val result = CompletableDeferred<Throwable?>()
        @kotlin.concurrent.Volatile var complete = false
    }

    private val current = AtomicReference<Attempt?>(null)
    val physicallyComplete: Boolean get() = current.load()?.complete == true

    private fun claim(): Pair<Attempt, Boolean> {
        while (true) {
            val old = current.load()
            if (old != null) {
                if (old.complete || !old.result.isCompleted) return old to false
            }
            val next = Attempt(statementCloseThreadIdentity())
            if (current.compareAndSet(old, next)) return next to true
        }
    }

    fun close(block: () -> Unit) = closeWith(block, ::awaitStatementCloseBlocking)

    suspend fun closeFromOwner(block: () -> Unit) = closeWith(block) { it.await() }

    private inline fun closeWith(
        noinline block: () -> Unit,
        await: (CompletableDeferred<Throwable?>) -> Throwable?,
    ) {
        val (attempt, owner) = claim()
        val failure = if (owner) perform(attempt, block) else {
            if (attempt.complete) return
            check(attempt.thread != statementCloseThreadIdentity()) {
                "Cannot reenter statement close from its cleanup owner"
            }
            await(attempt.result)
        }
        failure?.let { throw it }
    }

    private fun perform(attempt: Attempt, block: () -> Unit): Throwable? {
        val failure = try {
            block()
            attempt.complete = true
            null
        } catch (failure: Throwable) {
            failure
        }
        attempt.result.complete(failure)
        return failure
    }
}

internal expect fun statementCloseThreadIdentity(): Any
internal expect fun awaitStatementCloseBlocking(result: CompletableDeferred<Throwable?>): Throwable?
internal interface OwnedStatementClose {
    val closeProgress: StatementCloseProgress
    suspend fun closeFromOwner()
}
