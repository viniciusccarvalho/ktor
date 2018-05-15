package io.ktor.client.redis.engine.cio

import io.ktor.client.redis.*
import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.io.*
import kotlinx.io.core.*
import java.io.*
import java.nio.charset.*
import kotlin.test.*

class RedisTest {
    @Test
    fun testBasicProtocol() {
        runBlocking {
            val serverWrite = ByteChannel(true)
            val clientWrite = ByteChannel(true)

            val redis = RedisCIOClient({
                RedisCIOClient.Pipes(
                    serverWrite, clientWrite, Closeable { }
                )
            })

            serverWrite.writeStringUtf8("-ERROR\r\n")
            assertFailsWith<RedisResponseException>("ERROR") {
                runBlocking {
                    redis.set("hello", "world")
                }
            }

            assertEquals(
                listOf("*3", "\$3", "set", "\$5", "hello", "\$5", "world", ""),
                clientWrite.readAllLines(redis.charset)
            )

            serverWrite.writeStringUtf8("\$3\r\nabc\r\n")
            assertEquals("abc", redis.get("hello"))

            assertEquals(
                listOf("*2", "\$3", "get", "\$5", "hello", ""),
                clientWrite.readAllLines(redis.charset)
            )

            serverWrite.writeStringUtf8(":11\r\n")
            assertEquals(11L, redis.hincrby("a", "b", 1L))

            assertEquals(
                listOf("*4", "\$7", "hincrby", "\$1", "a", "\$1", "b", "\$1", "1", ""),
                clientWrite.readAllLines(redis.charset)
            )
        }
    }

    private suspend fun ByteReadChannel.readAllLines(charset: Charset): List<String> = readAllString(charset).split("\r\n")
    private suspend fun ByteReadChannel.readAllString(charset: Charset): String = readBytesExact(availableForRead).toString(charset)
    private suspend fun ByteReadChannel.readBytesExact(count: Int): ByteArray = readPacket(count).readBytes(count)
}
