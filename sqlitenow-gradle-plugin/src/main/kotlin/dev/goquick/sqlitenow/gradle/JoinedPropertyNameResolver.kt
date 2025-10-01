package dev.goquick.sqlitenow.gradle

/**
 * Resolves unique property names for _Joined data classes so that JOINed columns
 * do not collide when different tables expose the same column name (e.g., categoryDocId from multiple tables).
 *
 * Centralizes the algorithm so DataStructCodeGenerator (which declares the _Joined class)
 * and QueryCodeGenerator (which populates instances and maps dynamic fields from joined rows)
 * agree on the exact property names.
 */
object JoinedPropertyNameResolver {
  data class JoinedFieldKey(val tableAlias: String, val fieldName: String)

  /**
   * Compute a stable mapping from (tableAlias, fieldName) to a unique property name in the _Joined class.
   * - Base name respects explicit @propertyName and the configured PropertyNameGenerator
   * - When collisions occur, appends a disambiguation suffix derived from the table alias/name (PascalCase)
   * - If still colliding, appends an incrementing number
   */
  fun computeNameMap(
    fields: List<AnnotatedSelectStatement.Field>,
    propertyNameGenerator: PropertyNameGeneratorType,
    selectFieldGenerator: SelectFieldCodeGenerator
  ): Map<JoinedFieldKey, String> {
    val result = LinkedHashMap<JoinedFieldKey, String>()
    val used = HashSet<String>()

    // Only material (non-dynamic) fields are present in the _Joined class
    val materialFields = fields.filter { !it.annotations.isDynamicField }

    materialFields.forEach { field ->
      val baseName = selectFieldGenerator.generateProperty(field, propertyNameGenerator).name
      val tableAlias = field.src.tableName.orEmpty()
      val key = JoinedFieldKey(tableAlias, field.src.fieldName)

      val candidate = createUniqueName(baseName, tableAlias, used)

      result[key] = candidate
      used += candidate

      val originalName = field.src.originalColumnName
      if (!originalName.isNullOrBlank()) {
        val originalKey = JoinedFieldKey(tableAlias, originalName)
        result.putIfAbsent(originalKey, candidate)
      }
    }
    return result
  }

  private fun createUniqueName(
    baseName: String,
    tableAlias: String,
    used: MutableSet<String>
  ): String {
    if (baseName !in used) return baseName

    val aliasSuffix = tableAlias.takeIf { it.isNotBlank() }?.let { "_" + sanitizeAliasPart(it) } ?: ""
    var candidate = if (aliasSuffix.isNotBlank()) baseName + aliasSuffix else baseName
    var index = 2
    while (candidate in used) {
      candidate = if (aliasSuffix.isNotBlank()) baseName + aliasSuffix + index else baseName + index
      index++
    }
    return candidate
  }

  private fun sanitizeAliasPart(raw: String): String {
    val cleaned = raw
      .lowercase()
      .map { ch -> if (ch.isLetterOrDigit()) ch else '_' }
      .joinToString(separator = "")
    var result = cleaned.trim('_')
    if (result.isEmpty()) {
      result = "source"
    }
    // Collapse consecutive underscores for readability
    return result.replace(Regex("_+"), "_")
  }
}
