import androidx.compose.ui.ExperimentalComposeUiApi
import androidx.compose.ui.window.ComposeViewport
import dev.goquick.sqlitenow.samplekmp.App
import kotlinx.browser.document
import org.w3c.dom.HTMLElement

@OptIn(ExperimentalComposeUiApi::class)
fun main() {
    val root = document.getElementById("root") as HTMLElement
    ComposeViewport(viewportContainer = root) {
        App()
    }
}
