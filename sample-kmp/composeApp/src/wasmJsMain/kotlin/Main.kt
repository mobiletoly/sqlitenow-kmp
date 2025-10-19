@file:OptIn(ExperimentalComposeUiApi::class)

package dev.goquick.sqlitenow.samplekmp.wasm

import androidx.compose.ui.ExperimentalComposeUiApi
import androidx.compose.ui.window.ComposeViewport
import dev.goquick.sqlitenow.samplekmp.App

fun main() {
    ComposeViewport("root") {
        App()
    }
}
