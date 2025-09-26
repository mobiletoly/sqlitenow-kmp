package dev.goquick.sqlitenow.gradle

/** Simple indented code builder to reduce manual spacing/newlines in generated code strings. */
internal class IndentedCodeBuilder(private var indent: Int = 0) {
    private val sb = StringBuilder()
    fun line(text: String = "") {
        if (text.isNotEmpty()) sb.append(" ".repeat(indent)).append(text) else sb.append("")
        sb.append('\n')
    }
    fun indent(by: Int = 2, block: IndentedCodeBuilder.() -> Unit) {
        indent += by
        this.block()
        indent -= by
    }
    fun build(): String = sb.toString()
}

