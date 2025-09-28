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
      val key = JoinedFieldKey(field.src.tableName.orEmpty(), field.src.fieldName)

      var candidate = baseName
      if (candidate in used) {
        val suffix = disambiguationSuffix(field, fields)
        candidate = if (suffix.isNotBlank()) baseName + suffix else baseName
        var i = 2
        while (candidate in used) {
          candidate = if (suffix.isNotBlank()) baseName + suffix + i else baseName + i
          i++
        }
      }

      result[key] = candidate
      used += candidate
    }
    return result
  }

  /**
   * Create a readable suffix from the source table alias or dynamic aliasPrefix.
   * Example: table alias "activity_category" -> "ActivityCategory".
   */
  private fun disambiguationSuffix(
    field: AnnotatedSelectStatement.Field,
    allFields: List<AnnotatedSelectStatement.Field>
  ): String {
    val alias = field.src.tableName
    if (!alias.isNullOrBlank()) {
      return pascalize(alias)
    }
    // Fallback: try to infer from aliasPrefix used for dynamic fields
    val aliasPrefixes = allFields
      .filter { it.annotations.isDynamicField }
      .mapNotNull { it.annotations.aliasPrefix }
      .filter { it.isNotBlank() }
    val visibleName = field.src.fieldName
    val matched = aliasPrefixes.firstOrNull { visibleName.startsWith(it) }
    if (matched != null) {
      val trimmed = if (matched.endsWith("_")) matched.dropLast(1) else matched
      return pascalize(trimmed)
    }
    return ""
  }
}

