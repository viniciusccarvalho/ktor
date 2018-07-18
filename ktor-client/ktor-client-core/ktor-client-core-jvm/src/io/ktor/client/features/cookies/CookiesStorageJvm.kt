package io.ktor.client.features.cookies

import io.ktor.client.features.*
import io.ktor.http.*

/**
 * Runs a [block] of code, for all the cookies set in the specified [host].
 */
suspend inline fun CookiesStorage.forEach(host: String, block: (Cookie) -> Unit) {
    get(host)?.forEach { block(it.value) }
}

/**
 * [CookiesStorage] that ignores [addCookie] and returns a list of specified [cookies] when constructed.
 */
class ConstantCookieStorage(vararg cookies: Cookie) : CookiesStorage {
    private val storage: Map<String, Cookie> = cookies.map { it.name to it }.toMap()

    override suspend fun get(host: String): Map<String, Cookie>? = storage

    override suspend fun get(host: String, name: String): Cookie? = storage[name]

    override suspend fun addCookie(host: String, cookie: Cookie) {}
}
