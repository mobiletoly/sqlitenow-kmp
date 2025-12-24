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
package dev.goquick.sqlitenow.gradle

import org.gradle.api.artifacts.Configuration
import org.gradle.api.artifacts.component.ComponentIdentifier
import org.gradle.api.artifacts.component.ModuleComponentIdentifier
import org.gradle.api.artifacts.component.ProjectComponentIdentifier
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.plugins.ExtensionAware
import org.gradle.language.jvm.tasks.ProcessResources
import org.jetbrains.kotlin.gradle.dsl.KotlinMultiplatformExtension
import java.util.concurrent.atomic.AtomicBoolean

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

            configureWasmResourceBundling(project)

            // For each db entry in commonMain, register task and wire srcDir
            ext.databases.all { db ->
                val genTask = project.tasks.register(
                    "generate${db.name}",
                    GenerateDatabaseFilesTask::class.java
                ) { task ->
                    task.dbName.set(db.name)
                    task.packageName.set(db.packageName)
                    task.schemaDatabaseFile.set(db.schemaDatabaseFile)
                    task.debug.set(db.debug)
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

private fun configureWasmResourceBundling(project: Project) {
    val configured = AtomicBoolean(false)
    val preferredName = "wasmJsMainCompileClasspath"
    val fallbackName = "wasmJsCompileClasspath"
    project.configurations.matching { it.name == preferredName || it.name == fallbackName }.all { wasmClasspath ->
        if (configured.get()) return@all
        if (wasmClasspath.name == fallbackName &&
            project.configurations.findByName(preferredName) != null
        ) {
            return@all
        }
        configured.set(true)
        project.tasks.withType(ProcessResources::class.java).configureEach { task ->
            if (task.name != "wasmJsProcessResources") return@configureEach
            task.dependsOn(wasmClasspath)
            task.from({ resolveSqliteNowWasmKlibs(project, wasmClasspath).map { project.zipTree(it) } }) { spec ->
                spec.include(
                    "sqlitenow-sqljs.js",
                    "sqlitenow-indexeddb.js",
                    "sql-wasm.wasm"
                )
            }
        }
    }
}

private fun resolveSqliteNowWasmKlibs(project: Project, classpath: Configuration): Set<java.io.File> {
    val artifacts = classpath.incoming.artifactView { }.artifacts
    val klibs = artifacts.artifacts.mapNotNull { artifact ->
        val file = artifact.file
        if (file.extension != "klib") {
            return@mapNotNull null
        }
        if (isSqliteNowComponent(project, artifact.id.componentIdentifier)) {
            return@mapNotNull file
        }
        null
    }.toSet()

    if (klibs.isNotEmpty()) {
        return klibs
    }

    return artifacts.artifacts.mapNotNull { artifact ->
        val file = artifact.file
        if (file.extension == "klib" && file.name.contains("sqlitenow", ignoreCase = true)) {
            file
        } else {
            null
        }
    }.toSet()
}

private fun isSqliteNowComponent(project: Project, id: ComponentIdentifier): Boolean {
    return when (id) {
        is ModuleComponentIdentifier -> id.group == "dev.goquick.sqlitenow"
        is ProjectComponentIdentifier -> {
            val targetProject = project.rootProject.findProject(id.projectPath) ?: return false
            targetProject.group.toString() == "dev.goquick.sqlitenow"
        }
        else -> false
    }
}
