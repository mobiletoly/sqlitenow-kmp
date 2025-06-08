package dev.goquick.sqlitenow.samplekmp

interface Platform {
    val name: String
}

expect fun getPlatform(): Platform