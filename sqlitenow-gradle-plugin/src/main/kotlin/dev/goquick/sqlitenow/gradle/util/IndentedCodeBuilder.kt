/*
 * Copyright 2025 Anatoliy Pochkin
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
package dev.goquick.sqlitenow.gradle.util

/** Simple indented code builder to reduce manual spacing/newlines in generated code strings. */
internal class IndentedCodeBuilder(private var indent: Int = 0) {
    private val sb = StringBuilder()
    fun line(text: String = "") {
        if (text.isNotEmpty()) sb.append(" ".repeat(indent)).append(text) else sb.append("")
        sb.append('\n')
    }

    fun lineRaw(text: String = "") {
        sb.append(text)
        sb.append('\n')
    }

    fun indent(by: Int = 2, block: IndentedCodeBuilder.() -> Unit) {
        indent += by
        this.block()
        indent -= by
    }

    fun currentIndent(): Int = indent

    fun build(): String = sb.toString()
}
