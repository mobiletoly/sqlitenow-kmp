package dev.goquick.sqlitenow.oversqlite

import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals

internal class BaselineConnectionJvmTest : CrossTargetSyncTestSupport() {
    @Test fun separateConnectionCaptureSurvivesDatabaseCloseAndReopen() = runOwnedWebDatabaseTest {
        val directory=Files.createTempDirectory("oversqlite-baseline-")
        val path=directory.resolve("state.sqlite").toString()
        val server=MockSyncServer();val http=server.newHttpClient()
        var db=newFileBackedDb(path)
        var client=newClient(db,http)
        try {
            createUsersAndPostsTables(db);client.openAndConnect("user-1").getOrThrow()
            insertUser(db,"P","parent");insertUser(db,"Q","other");insertPost(db,"C","P","child")
            client.pushPending().getOrThrow()
            val writer=newFileBackedDb(path)
            try { writer.execSQL("UPDATE posts SET title='second connection',user_id='Q' WHERE id='C'") } finally { writer.close() }
            client.closeFromCoordinatedOwner();db.close()
            db=newFileBackedDb(path);client=newClient(db,http)
            client.openAndConnect("user-1").getOrThrow()
            assertEquals("child",scalarText(db,"SELECT json_extract(base_payload,'$.title.value') FROM _sync_dirty_rows"))
            assertEquals("P",scalarText(db,"SELECT json_extract(parent_key,'$.id') FROM _sync_relationships WHERE view='server'"))
            assertEquals("Q",scalarText(db,"SELECT json_extract(parent_key,'$.id') FROM _sync_relationships WHERE view='desired'"))
            assertEquals("second connection",scalarText(db,"SELECT title FROM posts WHERE id='C'"))
        } finally {
            client.close();db.close();http.close()
            Files.walk(directory).use { paths -> paths.sorted(Comparator.reverseOrder()).forEach(Files::deleteIfExists) }
        }
    }
}
