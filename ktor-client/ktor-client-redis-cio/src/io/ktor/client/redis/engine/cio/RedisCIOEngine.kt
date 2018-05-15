package io.ktor.client.redis.engine.cio

import io.ktor.client.redis.*
import io.ktor.network.sockets.*
import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.channels.*
import kotlinx.coroutines.experimental.io.*
import java.io.*
import java.nio.charset.*
import java.util.concurrent.atomic.*

object RedisCIOEngine : RedisEngine {
    override fun createClient(host: String, port: Int, charset: Charset, password: String?): Redis {
        return RedisCIOClient(host, port, charset, password)
    }

    override fun createPubSubClient(host: String, port: Int, charset: Charset, password: String?): RedisPubSub {
        val psubDef = async {
            RedisSubscription.open(RedisCIOClient(host, port, charset, password).capturePipes(), charset)
        }
        return object : RedisPubSub {
            override suspend fun events(): ReceiveChannel<RedisPubSub.Event> {
                val psub = psubDef.await()
                return psub.messages.map { RedisPubSub.Message(it.pattern, it.channel, it.content) }
            }

            override suspend fun subscribeOp(op: String, channel: String) {
                val psub = psubDef.await()
                when (op) {
                    "psubscribe" -> psub.psubscribe(channel)
                    "subscribe" -> psub.subscribe(channel)
                    "punsubscribe" -> psub.punsubscribe(channel)
                    "unsubscribe" -> psub.unsubscribe(channel)
                }
            }
        }
    }
}


internal class RedisCIOClient(
    val pipeFactory: suspend () -> Pipes,
    override val charset: Charset = Charsets.UTF_8,
    val password: String? = null
) : Redis {
    constructor(
        host: String,
        port: Int,
        charset: Charset = Charsets.UTF_8,
        password: String? = null
    ) : this({
        val socket = aSocket().tcp().connect(host, port)
        Pipes(socket.openReadChannel(), socket.openWriteChannel(autoFlush = true), socket)
    }, charset, password)

    private val stats: RedisStats = RedisStats()

    companion object {
        val bufferSize = 0x1000
        val MAX_RETRIES = 10
    }

    data class Pipes(
        val reader: ByteReadChannel,
        val writer: ByteWriteChannel,
        val closeable: Closeable
    )

    lateinit var _pipes: Pipes
    private val initOnce = OnceAsync()
    private val commandQueue = AsyncTaskQueue()
    private val respReader = RESP.Reader(charset, bufferSize)
    private val respBuilder = RESP.Writer(charset)
    private val cmd = BlobBuilder(bufferSize, charset)

    suspend fun reader(): ByteReadChannel = initOnce().reader
    suspend fun writer(): ByteWriteChannel = initOnce().writer
    suspend fun closeable(): Closeable = initOnce().closeable

    suspend fun reconnect() {
        try {
            _pipes = pipeFactory()
            if (password != null) auth(password)
        } catch (e: Throwable) {
            println("Failed to connect, retrying... ${e.message}")
            throw e
        }
    }

    suspend fun reconnectRetrying() {
        var retryCount = 0
        retry@ while (true) {
            try {
                reconnect()
                return
            } catch (e: IOException) {
                delay(500 * retryCount)
                retryCount++
                if (retryCount < MAX_RETRIES) {
                    continue@retry
                } else {
                    throw IOException("Giving up trying to connect to redis max retries: $MAX_RETRIES")
                }
            }
        }
    }

    private suspend fun initOnce(): Pipes {
        initOnce {
            commandQueue {
                reconnectRetrying()
            }
        }
        return _pipes
    }

    suspend fun close() = closeable().close()

    override suspend fun commandAny(vararg args: Any?): Any? {
        if (capturedPipes) error("Can't emit plain redis commands after entering into a redis sub-state")
        val writer = writer()
        stats.commandsQueued.incrementAndGet()
        return commandQueue {
            cmd.reset()
            respBuilder.writeValue(args, cmd)

            // Common queue is not required align reading because Redis support pipelining : https://redis.io/topics/pipelining
            var retryCount = 0

            retry@ while (true) {
                stats.commandsStarted.incrementAndGet()
                try {
                    stats.commandsPreWritten.incrementAndGet()

                    cmd.writeTo(writer)
                    writer.flush()

                    stats.commandsWritten.incrementAndGet()

                    val res = respReader.readValue(reader())

                    stats.commandsFinished.incrementAndGet()
                    return@commandQueue res
                } catch (t: IOException) {
                    t.printStackTrace()
                    stats.commandsErrored.incrementAndGet()
                    try {
                        reconnect()
                    } catch (e: Throwable) {
                    }
                    delay(500 * retryCount)
                    retryCount++
                    if (retryCount < MAX_RETRIES) {
                        continue@retry
                    } else {
                        throw IOException("Giving up with this redis request max retries $MAX_RETRIES")
                    }
                } catch (t: RedisResponseException) {
                    stats.commandsFailed.incrementAndGet()
                    throw t
                }
            }
        }
    }

    private var capturedPipes: Boolean = false
    suspend fun capturePipes(): Pipes {
        capturedPipes = true
        return initOnce()
    }
}

data class RedisStats(
    val commandsQueued: AtomicLong = AtomicLong(),
    val commandsStarted: AtomicLong = AtomicLong(),
    val commandsPreWritten: AtomicLong = AtomicLong(),
    val commandsWritten: AtomicLong = AtomicLong(),
    val commandsErrored: AtomicLong = AtomicLong(),
    val commandsFailed: AtomicLong = AtomicLong(),
    val commandsFinished: AtomicLong = AtomicLong()
)
