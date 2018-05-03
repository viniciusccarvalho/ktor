package io.ktor.client.redis.tests

import io.ktor.client.redis.*
import io.ktor.network.sockets.*
import io.ktor.network.sockets.ServerSocket
import kotlinx.coroutines.experimental.*
import org.junit.Test
import java.io.*
import java.net.*
import kotlin.test.*

class RedisIntegrationTest {
    @Test
    fun testIntegration() {
        runBlocking {
            val server = object : RedisMiniServer() {
                val keys = HashMap<String, String>()

                override fun processCommand(cmd: List<Any?>): Any? {
                    val verb = cmd.first().toString().toLowerCase()
                    when (verb) {
                        "get" -> return keys[cmd[1].toString()]
                        "set" -> keys[cmd[1].toString()] = cmd[2].toString()
                        else -> error("Unsupported $verb : ($cmd)")
                    }
                    return "OK"
                }
            }.listen()
            val redis = Redis(listOf(InetSocketAddress("127.0.0.1", server.localPort)))
            redis.set("hello", "world")
            assertEquals("world", redis.get("hello"))
            server.close()
        }
    }
}

abstract class RedisMiniServer : Closeable {
    private lateinit var serverSocket: ServerSocket
    private lateinit var job: Job

    abstract fun processCommand(cmd: List<Any?>): Any?

    fun listen(host: String = "127.0.0.1", port: Int = 0) = this.apply {
        serverSocket = aSocket().tcp().bind(InetSocketAddress(host, port))
        job = launch {
            while (true) {
                val socket = serverSocket.accept()
                launch {
                    val read = socket.openReadChannel()
                    val write = socket.openWriteChannel()
                    val respReader = RESP.Reader(1024, Charsets.UTF_8)
                    val respWriter = RESP.Writer(Charsets.UTF_8)
                    while (true) {
                        val info = respReader.readValue(read)
                        val result = try {
                            processCommand(info as List<Any?>)
                        } catch (e: Throwable) {
                            e
                        }
                        val bytes = respWriter.buildValue(result)
                        write.writeFully(bytes)
                        write.flush()
                    }
                }
            }
        }
    }

    override fun close() {
        job.cancel()
    }

    val localPort get() = (serverSocket.localAddress as InetSocketAddress).port
}
