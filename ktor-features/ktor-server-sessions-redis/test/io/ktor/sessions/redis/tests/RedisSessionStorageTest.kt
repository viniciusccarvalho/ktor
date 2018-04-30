package io.ktor.sessions.redis.tests

import io.ktor.application.*
import io.ktor.client.redis.*
import io.ktor.http.*
import io.ktor.response.*
import io.ktor.routing.*
import io.ktor.server.testing.*
import io.ktor.sessions.*
import io.ktor.sessions.redis.*
import kotlinx.coroutines.experimental.*
import org.junit.Test
import kotlin.test.*

class RedisSessionStorageTest {
    private val sessionsMap = HashMap<String, String>()
    private val log = ArrayList<String>()

    private val redis = object : Redis {
        override suspend fun commandAny(vararg args: Any?): Any? {
            log += args.toList().joinToString(":")
            return when (args.first()) {
                "set" -> "OK".apply { sessionsMap[args[1] as String] = args[2] as String }
                "get" -> sessionsMap[args[1] as String]
                "expire" -> "OK"
                else -> error("Unhandled command ${args.toList()}")
            }
        }
    }

    @Test
    fun testRedisSessionStorage() {
        runBlocking {
            val storage = RedisSessionStorage(redis)
            storage.write("hello", byteArrayOf(1, 2, 3))
            val result = storage.read("hello")
            assertEquals(byteArrayOf(1, 2, 3).toList(), result?.toList())
            assertEquals(mapOf("session_hello" to "010203"), sessionsMap)
            assertEquals(
                listOf(
                    "set:session_hello:010203",
                    "expire:session_hello:3600",
                    "get:session_hello",
                    "expire:session_hello:3600"
                ),
                log
            )
        }
    }

    @Test
    fun testIntegration() {
        class MySession(val visits: Int)

        withTestApplication {
            application.apply {
                install(Sessions) {
                    this.cookie<MySession>("mycookie", RedisSessionStorage(redis))
                }
                routing {
                    get("/") {
                        val session = call.sessions.get<MySession>()
                        val nvisits = session?.visits ?: 0
                        call.sessions.set(MySession(nvisits + 1))
                        call.respondText("Visits $nvisits")
                    }
                }
            }

            repeat(2) {
                cookiesSession {
                    assertEquals("Visits 0", handleRequest(HttpMethod.Get, "/").response.content)
                    assertEquals("Visits 1", handleRequest(HttpMethod.Get, "/").response.content)
                }
            }

            assertEquals(
                "set,expire,get,expire,set,expire,set,expire,get,expire,set,expire",
                log.joinToString(",") { it.split(':').first() }
            )

            assertEquals(2, sessionsMap.size)
            assertEquals("visits=%23i2", sessionsMap.values.first().unhex.toString(Charsets.UTF_8))
        }
    }
}
