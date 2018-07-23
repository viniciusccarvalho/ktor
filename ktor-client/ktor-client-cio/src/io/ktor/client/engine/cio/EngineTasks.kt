package io.ktor.client.engine.cio

import io.ktor.client.request.*
import io.ktor.http.*
import io.ktor.util.date.*
import kotlinx.coroutines.experimental.*

internal data class RequestTask(
    val request: DefaultHttpRequest,
    val continuation: CancellableContinuation<CIOHttpResponse>
)

internal fun RequestTask.requiresDedicatedConnection(): Boolean = listOf(request.headers, request.content.headers).any {
    it[HttpHeaders.Connection] == "close" || it.contains(HttpHeaders.Upgrade)
} || request.method !in listOf(HttpMethod.Get, HttpMethod.Head)


internal class ConnectionResponseTask(
    val requestTime: GMTDate,
    val continuation: CancellableContinuation<CIOHttpResponse>,
    val request: DefaultHttpRequest
)