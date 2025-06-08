package dev.goquick.sqlitenow.gradle.inspect

import net.sf.jsqlparser.expression.BinaryExpression
import net.sf.jsqlparser.expression.Expression
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter
import net.sf.jsqlparser.expression.JdbcNamedParameter
import net.sf.jsqlparser.expression.operators.relational.Between
import net.sf.jsqlparser.expression.operators.relational.EqualsTo
import net.sf.jsqlparser.expression.operators.relational.ExpressionList
import net.sf.jsqlparser.expression.operators.relational.GreaterThan
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals
import net.sf.jsqlparser.expression.operators.relational.InExpression
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression

import net.sf.jsqlparser.expression.operators.relational.LikeExpression
import net.sf.jsqlparser.expression.operators.relational.MinorThan
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo
import net.sf.jsqlparser.schema.Column

sealed class AssociatedColumn(val columnName: String) {
    data class Default(val name: String) : AssociatedColumn(name)
    data class Collection(val name: String) : AssociatedColumn(name)
}

fun Expression.collectNamedParametersAssociatedWithColumns(): LinkedHashMap<String, AssociatedColumn> {
    val pairs = mutableListOf<Pair<String, AssociatedColumn>>()

    // Walk the WHERE expression tree
    this.accept(object : ExpressionVisitorAdapter() {

        override fun visit(expr: EqualsTo) {
            extract(expr)
            super.visit(expr)
        }

        override fun visit(expr: NotEqualsTo) {
            extract(expr)
            super.visit(expr)
        }

        override fun visit(expr: GreaterThan) {
            extract(expr)
            super.visit(expr)
        }

        override fun visit(expr: GreaterThanEquals) {
            extract(expr)
            super.visit(expr)
        }

        override fun visit(expr: MinorThan) {
            extract(expr)
            super.visit(expr)
        }

        override fun visit(expr: MinorThanEquals) {
            extract(expr)
            super.visit(expr)
        }

        // IN (col IN (:p1, :p2, ...))
        override fun visit(expr: InExpression) {
            val col = expr.leftExpression as? Column
            if (col != null) {
                val itemsList = expr.rightExpression
                if (itemsList is ExpressionList<*>) {
                    // ExpressionList contains a list of expressions
                    itemsList.filterIsInstance<JdbcNamedParameter>().forEach { param ->
                        pairs += param.name.removePrefix(":") to AssociatedColumn.Collection(col.columnName)
                    }
                }
            }
            super.visit(expr)
        }

        // BETWEEN (col BETWEEN :low AND :high)
        override fun visit(expr: Between) {
            val col = expr.leftExpression
            if (col is Column) {
                (expr.betweenExpressionStart as? JdbcNamedParameter)?.also {
                    pairs += it.name.removePrefix(":") to AssociatedColumn.Default(col.columnName)
                }
                (expr.betweenExpressionEnd as? JdbcNamedParameter)?.also {
                    pairs += it.name.removePrefix(":") to AssociatedColumn.Default(col.columnName)
                }
            }
            super.visit(expr)
        }

        // LIKE (col LIKE :pattern)
        override fun visit(expr: LikeExpression) {
            extract(expr)
            super.visit(expr)
        }

        // IS NULL / IS NOT NULL (no named params here, but override for completeness)
        override fun visit(expr: IsNullExpression) {
            super.visit(expr)
        }

        // Shared helper for any BinaryExpression (including LIKE)
        private fun extract(be: BinaryExpression) {
            val l = be.leftExpression
            val r = be.rightExpression
            when {
                l is Column && r is JdbcNamedParameter ->
                    pairs += r.name.removePrefix(":") to AssociatedColumn.Default(l.columnName)

                l is JdbcNamedParameter && r is Column ->
                    pairs += l.name.removePrefix(":") to AssociatedColumn.Default(r.columnName)
            }
        }
    })

    return linkedMapOf(*pairs.toTypedArray())
}
