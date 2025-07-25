package dev.goquick.sqlitenow.samplekmp

import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.compose.runtime.Composable
import androidx.compose.ui.tooling.preview.Preview
import dev.goquick.sqlitenow.core.setupAndroidAppContext

class MainActivity : ComponentActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        setupAndroidAppContext(this.applicationContext)

//        val path = databasePath("test04.db")
//        val result = File(path).delete()
//        println("--------> DELETED: $result")

        setContent {
            App()
        }
    }
}

@Preview
@Composable
fun AppAndroidPreview() {
    App()
}
