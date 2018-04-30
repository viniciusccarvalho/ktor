package io.ktor.server.testing

import io.ktor.http.*

class CookieTrackerTestApplicationEngine(
    val engine: TestApplicationEngine,
    var trackedCookies: List<Cookie> = listOf()
)

fun CookieTrackerTestApplicationEngine.handleRequest(
    method: HttpMethod,
    uri: String,
    setup: TestApplicationRequest.() -> Unit = {}
): TestApplicationCall {
    return engine.handleRequest(method, uri) {
        val cookieValue = trackedCookies.joinToString("; ") {
            encodeURLQueryComponent(it.name) + "=" + encodeURLQueryComponent(it.value)
        }
        addHeader("Cookie", cookieValue)
        setup()
    }.apply {
        trackedCookies = response.headers.values("Set-Cookie").map { parseServerSetCookieHeader(it) }
    }
}

fun TestApplicationEngine.cookiesSession(
    initialCookies: List<Cookie> = listOf(),
    callback: CookieTrackerTestApplicationEngine.() -> Unit
) {
    callback(CookieTrackerTestApplicationEngine(this, initialCookies))
}
