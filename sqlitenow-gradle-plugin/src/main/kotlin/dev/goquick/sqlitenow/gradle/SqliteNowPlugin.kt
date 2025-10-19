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
