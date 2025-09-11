package dev.goquick.sqlitenow.samplesynckmp

interface Platform {
    val name: String
}

expect fun getPlatform(): Platform