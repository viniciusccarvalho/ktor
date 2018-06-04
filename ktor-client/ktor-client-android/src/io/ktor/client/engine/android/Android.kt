package io.ktor.client.engine.android

import io.ktor.client.engine.*

object Android : HttpClientEngineFactory<HttpClientEngineConfig> {
    override fun create(block: HttpClientEngineConfig.() -> Unit): HttpClientEngine =
        AndroidClientEngine(HttpClientEngineConfig().apply(block))
}