package dev.goquick.sqlitenow.samplekmp

import androidx.compose.ui.window.Window
import androidx.compose.ui.window.application

fun main() = application {
    Window(
        onCloseRequest = ::exitApplication,
        title = "sample-kmp",
    ) {
        App()
    }
}