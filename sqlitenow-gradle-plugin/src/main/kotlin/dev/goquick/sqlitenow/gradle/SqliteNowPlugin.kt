package dev.goquick.sqlitenow.gradle

import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.plugins.ExtensionAware
import org.jetbrains.kotlin.gradle.dsl.KotlinMultiplatformExtension

class SqliteNowPlugin : Plugin<Project> {
    override fun apply(project: Project) {
        val ext = project.extensions.create(
            "sqliteNow",
            SqliteNowExtension::class.java,
            project.objects
        )
        (ext as ExtensionAware).extensions.add("databases", ext.databases)

        // ➀ React when the MPP plugin is applied (during configuration)
        project.pluginManager.withPlugin("org.jetbrains.kotlin.multiplatform") {
            val mppExt = project.extensions
                .getByType(KotlinMultiplatformExtension::class.java)

            // For each db entry in commonMain, register task and wire srcDir
            ext.databases.all { db ->
                val genTask = project.tasks.register(
                    "generate${db.name}",
                    GenerateDatabaseFilesTask::class.java
                ) { task ->
                    task.dbName.set(db.name)
                    task.packageName.set(db.packageName)
                    task.schemaDatabaseFile.set(db.schemaDatabaseFile)
                }

                // Add generated dir to commonMain immediately
                mppExt.sourceSets
                    .getByName("commonMain")
                    .kotlin
                    .srcDir(genTask.map { it.outputDir })
            }
        }
    }
}
