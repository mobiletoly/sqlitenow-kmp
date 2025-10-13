import androidx.compose.ui.ExperimentalComposeUiApi
import androidx.compose.ui.window.ComposeViewport
import dev.goquick.sqlitenow.samplekmp.App
import dev.goquick.sqlitenow.core.SqliteConnectionConfig
import dev.goquick.sqlitenow.core.persistence.IndexedDbSqlitePersistence
import dev.goquick.sqlitenow.samplekmp.db
import kotlinx.browser.document
import org.w3c.dom.HTMLElement

@OptIn(ExperimentalComposeUiApi::class)
fun main() {
    db.connectionConfig = SqliteConnectionConfig(
        persistence = IndexedDbSqlitePersistence(storageName = "SqliteNowSample")
    )
    val root = document.getElementById("root") as HTMLElement
    ComposeViewport(viewportContainer = root) {
        App()
    }
}
