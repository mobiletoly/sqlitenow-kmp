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
import androidx.compose.ui.ExperimentalComposeUiApi
import androidx.compose.ui.window.ComposeViewport
import dev.goquick.sqlitenow.samplesynckmp.App
import dev.goquick.sqlitenow.core.SqliteConnectionConfig
import dev.goquick.sqlitenow.core.persistence.IndexedDbSqlitePersistence
import dev.goquick.sqlitenow.samplesynckmp.db
import kotlinx.browser.document
import org.w3c.dom.HTMLElement

@OptIn(ExperimentalComposeUiApi::class)
fun main() {
    db.connectionConfig = SqliteConnectionConfig(
        persistence = IndexedDbSqlitePersistence(storageName = "SqliteNowSampleSync")
    )
    val root = document.getElementById("root") as HTMLElement
    ComposeViewport(viewportContainer = root) {
        App()
    }
}
