package dev.goquick.sqlitenow.gradle

/**
 * Utility class for resolving and canonicalizing adapter parameter names.
 * This centralizes the logic for handling adapter name normalization and deduplication.
 */
internal class AdapterParameterNameResolver {

    /**
     * Remove namespace/table prefix from adapter function name when it belongs to that namespace.
     * Example: "sqlValueToPersonBirthDate" in "person" namespace -> "sqlValueToBirthDate"
     */
    fun canonicalizeAdapterNameForNamespace(namespace: String, functionName: String): String {
        val pascalNs = pascalize(namespace)
        val base = functionName.substringBefore("For")
        return when {
            base.startsWith("sqlValueTo") -> {
                val prop = base.removePrefix("sqlValueTo")
                val canonicalProp = if (prop.startsWith(pascalNs)) prop.removePrefix(pascalNs) else prop
                "sqlValueTo$canonicalProp"
            }
            base.endsWith("ToSqlValue") -> {
                val prop = base.removeSuffix("ToSqlValue")
                val canonicalProp = if (prop.startsWith(pascalNs)) prop.removePrefix(pascalNs) else prop
                "${canonicalProp}ToSqlValue"
            }
            else -> base
        }
    }

    /**
     * Normalize alias noise by removing duplicate tokens.
     * Example: "sqlValueToAddressAddressType" -> "sqlValueToAddressType"
     */
    fun normalizeAliasNoiseForNamespace(namespace: String, functionName: String): String {
        val base = functionName.substringBefore("For")
        fun compress(prop: String): String {
            val tokens = prop.split(Regex("(?=[A-Z])")).filter { it.isNotEmpty() }
            return if (tokens.size >= 2 && tokens[0] == tokens[1]) {
                (listOf(tokens[0]) + tokens.drop(2)).joinToString("")
            } else {
                prop
            }
        }
        return when {
            base.startsWith("sqlValueTo") -> {
                val prop = base.removePrefix("sqlValueTo")
                "sqlValueTo" + compress(prop)
            }
            base.endsWith("ToSqlValue") -> {
                val prop = base.removeSuffix("ToSqlValue")
                compress(prop) + "ToSqlValue"
            }
            else -> base
        }
    }

    /**
     * Choose final adapter parameter names for a statement, canonicalizing and de-duplicating by signature.
     * This is the main entry point for resolving adapter parameter names.
     */
    fun chooseAdapterParamNames(
        configs: List<AdapterConfig.ParamConfig>
    ): Map<AdapterConfig.ParamConfig, String> {
        // Step 1: canonicalize + normalize alias noise
        val canonical = configs.associateWith { config ->
            val base = config.providerNamespace?.let { ns ->
                canonicalizeAdapterNameForNamespace(ns, config.adapterFunctionName)
            } ?: config.adapterFunctionName
            normalizeAliasNoiseForNamespace(config.providerNamespace ?: "", base)
        }
        
        // Step 2: group by (name + signature); assign one shared param name per group
        data class Sig(val inT: String, val outT: String)
        val groups = mutableMapOf<String, MutableMap<Sig, MutableList<AdapterConfig.ParamConfig>>>()
        configs.forEach { cfg ->
            val name = canonical[cfg]!!
            val sig = Sig(cfg.inputType.toString(), cfg.outputType.toString())
            val bySig = groups.getOrPut(name) { linkedMapOf() }
            bySig.getOrPut(sig) { mutableListOf() }.add(cfg)
        }
        
        // Step 3: build chosen names: if a name maps to multiple signatures, disambiguate by signature
        val result = mutableMapOf<AdapterConfig.ParamConfig, String>()
        groups.forEach { (name, bySig) ->
            if (bySig.size == 1) {
                // Single signature: use the canonical name
                bySig.values.first().forEach { cfg -> result[cfg] = name }
            } else {
                // Multiple signatures: disambiguate
                bySig.forEach { (sig, cfgs) ->
                    val disambiguated = "${name}For${sig.inT.substringAfterLast('.')}To${sig.outT.substringAfterLast('.')}"
                    cfgs.forEach { cfg -> result[cfg] = disambiguated }
                }
            }
        }
        return result
    }

    /**
     * Resolve chosen adapter param name for a given output field.
     * This is used by query generation to find the correct parameter name to use in function calls.
     */
    fun resolveOutputAdapterParamNameForField(
        statement: AnnotatedSelectStatement,
        field: AnnotatedSelectStatement.Field,
        tableAliases: Map<String, String>,
        aliasPrefixes: List<String>,
        adapterConfig: AdapterConfig
    ): String? {
        val all = adapterConfig.collectAllParamConfigs(statement)
        val outputConfigs = all.filter { it.adapterFunctionName.startsWith("sqlValueTo") }
        if (outputConfigs.isEmpty()) return null
        val chosen = chooseAdapterParamNames(outputConfigs)

        // Build raw function name as AdapterConfig does (using base column name, not visible name)
        val baseColumnName = adapterConfig.baseOriginalNameForField(field, aliasPrefixes)
        val columnName = PropertyNameGeneratorType.LOWER_CAMEL_CASE.convertToPropertyName(baseColumnName)
        val rawAdapterName = adapterConfig.getOutputAdapterFunctionName(columnName)
        val providerNs = if (field.src.tableName.isNotBlank()) {
            tableAliases[field.src.tableName] ?: field.src.tableName
        } else null

        // Try exact match (by raw name and provider namespace)
        val exact = outputConfigs.firstOrNull { it.adapterFunctionName == rawAdapterName && it.providerNamespace == providerNs }
        if (exact != null) return chosen[exact]

        // Fallback: match by raw name only
        val any = outputConfigs.firstOrNull { it.adapterFunctionName == rawAdapterName }
        return any?.let { chosen[it] }
    }


}
