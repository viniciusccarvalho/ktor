package io.ktor.sessions.redis

import io.ktor.client.redis.*
import io.ktor.sessions.*

class RedisSessionStorage(val redis: Redis, val prefix: String = "session_", val ttlSeconds: Int = 3600) :
    SessionStorageBaseByteArray() {
    private fun buildKey(id: String) = "$prefix$id"

    override suspend fun read(id: String): ByteArray? {
        val key = buildKey(id)
        return redis.get(key)?.unhex?.apply {
            redis.expire(key, ttlSeconds) // refresh
        }
    }

    override suspend fun write(id: String, data: ByteArray?) {
        val key = buildKey(id)
        if (data == null) {
            redis.del(buildKey(id))
        } else {
            redis.set(key, data.hex)
            redis.expire(key, ttlSeconds)
        }
    }
}
