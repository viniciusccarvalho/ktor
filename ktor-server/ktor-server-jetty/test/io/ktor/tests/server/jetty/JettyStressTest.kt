package io.ktor.tests.server.jetty

import io.ktor.server.jetty.*
import io.ktor.server.testing.*

class JettyStressTest : EngineStressSuite<JettyApplicationEngineBase.Configuration>(Jetty)
