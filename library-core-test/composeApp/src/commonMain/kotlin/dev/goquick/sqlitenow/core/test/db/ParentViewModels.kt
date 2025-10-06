package dev.goquick.sqlitenow.core.test.db

/**
 * Lightweight data holders referenced by generated SQLiteNow code for the parent_with_children view.
 * Defining them in shared code keeps generated files simple while giving instrumentation tests
 * strongly-typed access to the mapped fields.
 */
data class ParentMainDoc(
    val id: Long,
    val docId: String,
    val categoryId: Long,
)

data class ParentCategoryDoc(
    val id: Long,
    val docId: String,
    val title: String,
)

data class ParentChildDoc(
    val id: Long,
    val parentDocId: String,
    val title: String,
)
