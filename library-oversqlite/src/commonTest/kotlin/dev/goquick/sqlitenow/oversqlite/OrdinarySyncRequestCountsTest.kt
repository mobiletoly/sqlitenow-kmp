package dev.goquick.sqlitenow.oversqlite

import kotlin.test.*

internal class OrdinarySyncRequestCountsTest : CrossTargetSyncTestSupport() {
    @Test fun oneTerminalUploadAddsOneAckAndNoChangeSyncAddsNoProbe() = runOwnedWebDatabaseTest {
        val db=newDb();val server=MockSyncServer();val http=server.newHttpClient();val client=newClient(db,http)
        try {
            createUsersAndPostsTables(db);client.openAndConnect("user-1").getOrThrow()
            db.execSQL("INSERT INTO users(id,name) VALUES('item','ordinary')")
            server.requestTrace.clear();client.sync().getOrThrow()
            assertEquals(listOf("POST /sync/push","POST /sync/push-outcomes/ack","GET /sync/pull"),server.requestTrace)
            assertEquals(1,server.pushRequestCount);assertEquals(1,server.ackRequests.size);assertEquals(0,server.resolveRequests.size)
            server.requestTrace.clear();client.sync().getOrThrow()
            assertEquals(listOf("GET /sync/pull"),server.requestTrace)
            assertEquals(1,server.ackRequests.size);assertEquals(0,server.resolveRequests.size)
            println("ORDINARY_REQUEST_COUNTS_COMPLETE upload=push,ack,pull no_change=pull")
        } finally {client.closeFromCoordinatedOwner();http.close();db.close()}
    }
}
