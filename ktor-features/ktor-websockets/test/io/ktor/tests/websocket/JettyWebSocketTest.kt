package io.ktor.tests.websocket

import io.ktor.server.jetty.*

class JettyWebSocketTest : WebSocketEngineSuite<JettyApplicationEngineBase.Configuration>(Jetty)
