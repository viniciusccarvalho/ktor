package io.ktor.client.engine.mock

import io.ktor.client.call.*
import io.ktor.client.engine.*
import io.ktor.client.request.*
import io.ktor.http.*
import kotlinx.coroutines.experimental.*


class MockEngine(override val config: MockEngineConfig) : HttpClientEngine {
    override val dispatcher: CoroutineDispatcher =
        Unconfined

    override suspend fun execute(call: HttpClientCall, data: HttpRequestData): HttpEngineCall {
        val request = data.toRequest(call)
        val response = config.check(request, call)
        return HttpEngineCall(request, response)
    }

    override fun close() {}

    companion object : HttpClientEngineFactory<MockEngineConfig> {
        override fun create(block: MockEngineConfig.() -> Unit): HttpClientEngine =
            MockEngine(MockEngineConfig().apply(block))

        operator fun invoke(check: suspend HttpRequest.(call: HttpClientCall) -> MockHttpResponse): MockEngine =
            MockEngine(MockEngineConfig().apply {
                this.check = check
            })

        val RESPONSE_OK: (HttpClientCall, HttpRequest) -> MockHttpResponse = { call, request ->
            MockHttpResponse(call, HttpStatusCode.OK)
        }
    }
}
