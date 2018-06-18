package io.ktor.tests.server.netty

import io.ktor.server.netty.*
import io.ktor.server.testing.*

class NettyStressTest : EngineStressSuite<NettyApplicationEngine.Configuration>(Netty) {
}
