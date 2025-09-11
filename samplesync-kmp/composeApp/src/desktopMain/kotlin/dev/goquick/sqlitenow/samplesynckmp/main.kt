package dev.goquick.sqlitenow.samplesynckmp

import androidx.compose.ui.window.Window
import androidx.compose.ui.window.application

fun main() = application {
    Window(
        onCloseRequest = ::exitApplication,
        title = "samplesync-kmp",
    ) {
        App()
    }
}