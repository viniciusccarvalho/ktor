package io.ktor.client.features

import io.ktor.http.*
import java.util.*
import java.util.concurrent.*

/**
 * [CookiesStorage] that stores all the cookies in an in-memory map.
 */

actual class AcceptAllCookiesStorage actual constructor() : CookiesStorage {
    private val data = ConcurrentHashMap<String, MutableMap<String, Cookie>>()

    override suspend fun get(host: String): Map<String, Cookie>? = data[host]?.let {
        Collections.unmodifiableMap(data[host])
    }

    override suspend fun get(host: String, name: String): Cookie? = data[host]?.get(name)

    override suspend fun addCookie(host: String, cookie: Cookie) {
        init(host)
        data[host]?.set(cookie.name, cookie)
    }

    private fun init(host: String) {
        if (!data.containsKey(host)) {
            data[host] = mutableMapOf()
        }
    }
}
