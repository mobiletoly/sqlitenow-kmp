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
package dev.goquick.sqlitenow.gradle.sqlinspect

import net.sf.jsqlparser.statement.Statement
import net.sf.jsqlparser.statement.delete.Delete
import net.sf.jsqlparser.util.deparser.StatementDeParser

/**
 * Rewrites all named parameters in DELETE statements to '?' and collects named parameters.
 */
class DeleteParametersProcessor(
    stmt: Statement
) : ExecuteParameterProcessor(stmt) {

    override fun processWithClauses(stmtDp: StatementDeParser, buffer: StringBuilder) {
        (stmt as Delete).withItemsList?.forEach {
            it.select.accept(stmtDp)
        }
        buffer.clear()
    }
}
