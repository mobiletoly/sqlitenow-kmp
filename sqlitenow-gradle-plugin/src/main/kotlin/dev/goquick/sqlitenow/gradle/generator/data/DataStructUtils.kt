package dev.goquick.sqlitenow.gradle.generator.data

import com.squareup.kotlinpoet.ClassName
import com.squareup.kotlinpoet.CodeBlock
import com.squareup.kotlinpoet.FunSpec
import com.squareup.kotlinpoet.KModifier
import com.squareup.kotlinpoet.ParameterizedTypeName
import com.squareup.kotlinpoet.PropertySpec
import com.squareup.kotlinpoet.TypeName
import com.squareup.kotlinpoet.TypeSpec

internal object DataStructUtils {
    fun addArraySafeEqualsAndHashCodeIfNeeded(
        classBuilder: TypeSpec.Builder,
        className: String,
        properties: List<PropertySpec>,
    ) {
        if (properties.none { isArrayLike(it.type) }) return

        classBuilder.addFunction(buildEquals(className, properties))
        classBuilder.addFunction(buildHashCode(properties))
    }

    private fun buildEquals(
        className: String,
        properties: List<PropertySpec>
    ): FunSpec {
        val equalsFun = FunSpec.builder("equals")
            .addModifiers(KModifier.OVERRIDE)
            .addParameter("other", ClassName("kotlin", "Any").copy(nullable = true))
            .returns(ClassName("kotlin", "Boolean"))
            .addStatement("if (this === other) return true")
            .addStatement("if (other !is %L) return false", className)

        properties.forEach { prop ->
            val name = prop.name
            val type = prop.type
            if (isArrayLike(type)) {
                val comparison = when {
                    isPrimitiveArrayClass(type) -> "contentEquals"
                    isGenericArray(type) -> "contentDeepEquals"
                    else -> "contentEquals"
                }
                if (type.isNullable) {
                    equalsFun.addStatement("if (%L !== other.%L) {", name, name)
                    equalsFun.addStatement("  if (%L == null || other.%L == null) return false", name, name)
                    equalsFun.addStatement("  if (!%L.%L(other.%L)) return false", name, comparison, name)
                    equalsFun.addStatement("}")
                } else {
                    equalsFun.addStatement("if (!%L.%L(other.%L)) return false", name, comparison, name)
                }
            } else {
                equalsFun.addStatement("if (%L != other.%L) return false", name, name)
            }
        }
        equalsFun.addStatement("return true")
        return equalsFun.build()
    }

    private fun buildHashCode(properties: List<PropertySpec>): FunSpec {
        val hashFun = FunSpec.builder("hashCode")
            .addModifiers(KModifier.OVERRIDE)
            .returns(ClassName("kotlin", "Int"))

        if (properties.isEmpty()) {
            hashFun.addStatement("return 0")
            return hashFun.build()
        }

        properties.forEachIndexed { index, prop ->
            val name = prop.name
            val type = prop.type
            val term = when {
                isArrayLike(type) -> {
                    val call = when {
                        isPrimitiveArrayClass(type) -> "contentHashCode()"
                        isGenericArray(type) -> "contentDeepHashCode()"
                        else -> "contentHashCode()"
                    }
                    if (type.isNullable) {
                        CodeBlock.of("(%L?.%L ?: 0)", name, call)
                    } else {
                        CodeBlock.of("%L.%L", name, call)
                    }
                }
                type.isNullable -> CodeBlock.of("(%L?.hashCode() ?: 0)", name)
                else -> CodeBlock.of("%L.hashCode()", name)
            }

            if (index == 0) {
                hashFun.addStatement("var result = %L", term)
            } else {
                hashFun.addStatement("result = 31 * result + %L", term)
            }
        }
        hashFun.addStatement("return result")
        return hashFun.build()
    }

    private fun isArrayLike(type: TypeName): Boolean = arrayKind(type) != ArrayKind.NONE
    private fun isPrimitiveArrayClass(type: TypeName): Boolean = arrayKind(type) == ArrayKind.PRIMITIVE
    private fun isGenericArray(type: TypeName): Boolean = arrayKind(type) == ArrayKind.GENERIC

    private fun arrayKind(type: TypeName): ArrayKind {
        return when (type) {
            is ClassName -> if (type.packageName == "kotlin" && PRIMITIVE_ARRAY_SIMPLE_NAMES.contains(type.simpleName)) ArrayKind.PRIMITIVE else ArrayKind.NONE
            is ParameterizedTypeName -> {
                val raw = type.rawType
                if (raw.packageName == "kotlin" && raw.simpleName == "Array") ArrayKind.GENERIC else ArrayKind.NONE
            }
            else -> ArrayKind.NONE
        }
    }

    private val PRIMITIVE_ARRAY_SIMPLE_NAMES = setOf(
        "IntArray", "LongArray", "ShortArray", "ByteArray", "CharArray", "FloatArray", "DoubleArray", "BooleanArray"
    )

    private enum class ArrayKind { NONE, PRIMITIVE, GENERIC }
}
