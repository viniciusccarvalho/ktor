package io.ktor.client.redis

import io.ktor.network.sockets.*
import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.io.*
import kotlinx.io.core.*
import java.io.*
import java.net.*
import java.nio.*
import java.nio.ByteBuffer
import java.nio.charset.*
import java.util.*
import java.util.concurrent.atomic.*
import kotlin.coroutines.experimental.*

/**
 * A Redis basic interface exposing emiting commands receiving their responses.
 *
 * Specific commands are exposed as extension methods.
 */
// String, thrown ResponseException, Long, List<*>

interface Redis {
    /**
     * Executes a raw command. Each [args] will be sent as a String.
     *
     * It returns a type depending on the command.
     * The returned value can be of type [String], [Long] or [List].
     *
     * It may throw a [RedisResponseException]
     */
    suspend fun commandAny(vararg args: Any?): Any?
}

/**
 * Constructs a Redis multi-client that will connect to [addresses] in a round-robin fashion keeping a connection pool,
 * keeping as much as [maxConnections] and using the [charset].
 * Optionally you can define the [password] of the connection.
 * You can specify a [stats] object that will be populated by the clients.
 */
fun Redis(
    addresses: List<SocketAddress> = listOf(InetSocketAddress("127.0.0.1", 6379)),
    maxConnections: Int = 50,
    charset: Charset = Charsets.UTF_8,
    password: String? = null,
    stats: RedisStats = RedisStats(),
    bufferSize: Int = 0x1000
): Redis {
    var index = 0
    return RedisCluster(maxConnections = maxConnections, charset = charset) {
        val tcpClientFactory = aSocket().tcp()
        RedisClient(
            reconnect = { client ->
                index = (index + 1) % addresses.size
                val host = addresses[index] // Round Robin
                val socket = tcpClientFactory.connect(host)
                if (password != null) client.auth(password)
                RedisCluster.Pipes(socket.openReadChannel(), socket.openWriteChannel(autoFlush = true), socket)
            },
            bufferSize = bufferSize,
            charset = charset,
            stats = stats
        )
    }
}

class RedisStats {
    val commandsQueued = AtomicLong()
    val commandsStarted = AtomicLong()
    val commandsPreWritten = AtomicLong()
    val commandsWritten = AtomicLong()
    val commandsErrored = AtomicLong()
    val commandsFailed = AtomicLong()
    val commandsFinished = AtomicLong()

    override fun toString(): String {
        return "Stats(commandsQueued=$commandsQueued, commandsStarted=$commandsStarted, commandsPreWritten=$commandsPreWritten, commandsWritten=$commandsWritten, commandsErrored=$commandsErrored, commandsFinished=$commandsFinished)"
    }
}

/**
 * Redis client implementing the redis wire protocol defined in https://redis.io/topics/protocol
 */
internal class RedisCluster(
    internal val maxConnections: Int = 50,
    internal val charset: Charset = Charsets.UTF_8,
    internal val clientFactory: suspend () -> RedisClient
) : Redis {
    data class Pipes(
        val reader: ByteReadChannel,
        val writer: ByteWriteChannel,
        val closeable: Closeable
    )

    private val clientPool = AsyncPool(maxItems = maxConnections) { clientFactory() }

    override suspend fun commandAny(vararg args: Any?): Any? = clientPool.tempAlloc { it.commandAny(*args) }
}

internal class RedisClient(
    private val charset: Charset = Charsets.UTF_8,
    private val stats: RedisStats = RedisStats(),
    private val bufferSize: Int = 0x1000,
    private val reconnect: suspend (RedisClient) -> RedisCluster.Pipes
) : Redis {
    companion object {
        val MAX_RETRIES = 10
        private const val LF = '\n'.toByte()
        private val LF_BB = ByteBuffer.wrap(byteArrayOf(LF))
    }

    val charsetDecoder = charset.newDecoder()

    lateinit var pipes: RedisCluster.Pipes
    private val initOnce = OnceAsync()
    private val commandQueue = AsyncTaskQueue()

    suspend fun reader(): ByteReadChannel = initOnce().reader
    suspend fun writer(): ByteWriteChannel = initOnce().writer
    suspend fun closeable(): Closeable = initOnce().closeable

    suspend fun reconnect() {
        try {
            pipes = reconnect(this@RedisClient)
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

    private suspend fun initOnce(): RedisCluster.Pipes {
        initOnce {
            commandQueue {
                reconnectRetrying()
            }
        }
        return pipes
    }

    suspend fun close() {
        closeable().close()
    }

    private val valueSB = StringBuilder(bufferSize)
    private val valueCB = CharBuffer.allocate(bufferSize)
    private val valueBB = ByteBuffer.allocate((bufferSize / charsetDecoder.maxCharsPerByte()).toInt())
    private val tempCRLF = ByteArray(2)
    private suspend fun readValue(): Any? {
        val reader = reader()

        valueSB.setLength(0)
        val line = reader.readUntilString(
            valueSB, LF_BB, charsetDecoder, valueCB, valueBB
        ).trimEnd().toString()

        if (line.isEmpty()) throw RedisResponseException("Empty value")
        return when (line[0]) {
            '+' -> line.substring(1) // Status reply
            '-' -> throw RedisResponseException(line.substring(1)) // Error reply
            ':' -> line.substring(1).toLong() // Integer reply
            '$' -> { // Bulk replies
                val bytesToRead = line.substring(1).toInt()
                if (bytesToRead == -1) {
                    null
                } else {
                    val data = reader.readBytesExact(bytesToRead)
                    reader.readFully(tempCRLF) // CR LF
                    data.toString(charset)
                }
            }
            '*' -> { // Array reply
                val arraySize = line.substring(1).toInt()
                (0 until arraySize).map { readValue() }
            }
            else -> throw RedisResponseException("Unknown param type '${line[0]}'")
        }
    }

    private val cmdChunk = BOS(bufferSize, charset)
    private val cmd = BOS(bufferSize, charset)
    override suspend fun commandAny(vararg args: Any?): Any? {
        val writer = writer()
        stats.commandsQueued.incrementAndGet()
        return commandQueue {
            cmd.reset()
            cmd.append('*')
            cmd.append(args.size)
            cmd.append('\r')
            cmd.append('\n')
            for (arg in args) {
                cmdChunk.reset()
                when (arg) {
                    is Int -> cmdChunk.append(arg)
                    is Long -> cmdChunk.append(arg)
                    else -> cmdChunk.append(arg.toString())
                }
                // Length of the argument.
                cmd.append('$')
                cmd.append(cmdChunk.size())
                cmd.append('\r')
                cmd.append('\n')
                cmd.append(cmdChunk)
                cmd.append('\r')
                cmd.append('\n')
            }

            // Common queue is not required align reading because Redis support pipelining : https://redis.io/topics/pipelining
            val data = cmd.buf()
            val dataLen = cmd.size()
            var retryCount = 0

            retry@ while (true) {
                stats.commandsStarted.incrementAndGet()
                try {
                    stats.commandsPreWritten.incrementAndGet()
                    writer.writeFully(data, 0, dataLen) // @TODO: Maybe use writeAvailable instead of appending?
                    writer.flush()
                    stats.commandsWritten.incrementAndGet()
                    val res = readValue()
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
}

class RedisResponseException(message: String) : Exception(message)

@Suppress("UNCHECKED_CAST")
suspend fun Redis.commandArray(vararg args: Any?): List<String> =
    (commandAny(*args) as List<String>?) ?: listOf()

suspend fun Redis.commandString(vararg args: Any?): String? = commandAny(*args)?.toString()
suspend fun Redis.commandLong(vararg args: Any?): Long = commandAny(*args)?.toString()?.toLongOrNull() ?: 0L
suspend fun Redis.commandUnit(vararg args: Any?): Unit = run { commandAny(*args) }

/**
 * Optimized class for allocation-free String/ByteArray building
 */
private class BOS(size: Int, val charset: Charset) : ByteArrayOutputStream(size) {
    private val charsetEncoder = charset.newEncoder()
    private val tempCB = CharBuffer.allocate(1024)
    private val tempBB = ByteBuffer.allocate((tempCB.count() * charsetEncoder.maxBytesPerChar()).toInt())
    private val tempSB = StringBuilder(64)

    fun buf(): ByteArray {
        return buf
    }

    fun append(value: Long) {
        tempSB.setLength(0)
        tempSB.append(value)
        tempCB.clear()
        tempSB.getChars(0, tempSB.length, tempCB.array(), 0)
        tempCB.position(tempSB.length)
        tempCB.flip()
        append(tempCB)
    }

    fun append(value: Int) {
        when (value) {
            in 0..9 -> {
                write(('0' + value).toInt())
            }
            in 10..99 -> {
                write(('0' + (value / 10)).toInt())
                write(('0' + (value % 10)).toInt())
            }
            else -> {
                tempSB.setLength(0)
                tempSB.append(value)
                tempCB.clear()
                tempSB.getChars(0, tempSB.length, tempCB.array(), 0)
                tempCB.position(tempSB.length)
                tempCB.flip()
                append(tempCB)
            }
        }
    }

    fun append(char: Char) {
        if (char.toInt() <= 0xFF) {
            write(char.toInt())
        } else {
            tempCB.clear()
            tempCB.put(char)
            tempCB.flip()
            append(tempCB)
        }
    }

    fun append(str: String) {
        val len = str.length
        if (len == 0) return

        val chunk = Math.min(len, 1024)

        for (n in 0 until len step chunk) {
            tempCB.clear()
            val cend = Math.min(len, n + chunk)
            str.toCharArray(tempCB.array(), 0, n, cend)
            tempCB.position(cend - n)
            tempCB.flip()
            append(tempCB)
        }
    }

    fun append(bb: ByteBuffer) {
        while (bb.hasRemaining()) {
            write(bb.get().toInt())
        }
    }

    fun append(cb: CharBuffer) {
        charsetEncoder.reset()
        while (cb.hasRemaining()) {
            tempBB.clear()
            charsetEncoder.encode(cb, tempBB, false)
            tempBB.flip()
            append(tempBB)
        }
        tempBB.clear()
        charsetEncoder.encode(cb, tempBB, true)
        tempBB.flip()
        append(tempBB)
    }

    fun append(that: BOS) {
        this.write(that.buf(), 0, that.size())
    }

    override fun toString() = toByteArray().toString(charset)
}

private class OnceAsync {
    var deferred: kotlinx.coroutines.experimental.Deferred<Unit>? = null

    suspend operator fun invoke(callback: suspend () -> Unit) {
        if (deferred == null) {
            deferred = async { callback() }
        }
        return deferred!!.await()
    }
}

class AsyncTaskQueue {
    var running = AtomicBoolean(false); private set
    private var queue = LinkedList<suspend () -> Unit>()

    val queued get() = synchronized(queue) { queue.size }

    suspend operator fun <T> invoke(func: suspend () -> T): T {
        val deferred = CompletableDeferred<T>()

        synchronized(queue) {
            queue.add {
                val result = try {
                    func()
                } catch (e: Throwable) {
                    deferred.completeExceptionally(e)
                    return@add
                }
                deferred.complete(result)
            }
        }
        if (running.compareAndSet(false, true)) {
            runTasks(coroutineContext)
        }
        return deferred.await()
    }

    private fun runTasks(baseContext: CoroutineContext) {
        val item = synchronized(queue) { if (queue.isNotEmpty()) queue.remove() else null }
        if (item != null) {
            item.startCoroutine(object : Continuation<Unit> {
                override val context: CoroutineContext = baseContext
                override fun resume(value: Unit) = runTasks(baseContext)
                override fun resumeWithException(exception: Throwable) = runTasks(baseContext)
            })
        } else {
            running.set(false)
        }
    }
}

private class AsyncPool<T>(val maxItems: Int = Int.MAX_VALUE, val create: suspend (index: Int) -> T) {
    var createdItems = AtomicInteger()
    private val freedItem = LinkedList<T>()
    private val waiters = LinkedList<CompletableDeferred<Unit>>()
    val availableFreed: Int get() = synchronized(freedItem) { freedItem.size }

    suspend fun <TR> tempAlloc(callback: suspend (T) -> TR): TR {
        val item = alloc()
        try {
            return callback(item)
        } finally {
            free(item)
        }
    }

    suspend fun alloc(): T {
        while (true) {
            // If we have an available item just retrieve it
            synchronized(freedItem) {
                if (freedItem.isNotEmpty()) {
                    val item = freedItem.remove()
                    if (item != null) {
                        return item
                    }
                }
            }

            // If we don't have an available item yet and we can create more, just create one
            if (createdItems.get() < maxItems) {
                val index = createdItems.getAndAdd(1)
                return create(index)
            }
            // If we shouldn't create more items and we don't have more, just await for one to be freed.
            else {
                val deferred = CompletableDeferred<Unit>()
                synchronized(waiters) {
                    waiters += deferred
                }
                deferred.await()
            }
        }
    }

    fun free(item: T) {
        synchronized(freedItem) {
            freedItem.add(item)
        }
        val waiter = synchronized(waiters) { if (waiters.isNotEmpty()) waiters.remove() else null }
        waiter?.complete(Unit)
    }
}

// Simple version
private suspend fun ByteReadChannel.readUntilString(
    delimiter: Byte,
    charset: Charset,
    bufferSize: Int = 1024,
    expectedMinSize: Int = 16
): String {
    return readUntilString(
        out = StringBuilder(expectedMinSize),
        delimiter = ByteBuffer.wrap(byteArrayOf(delimiter)),
        decoder = charset.newDecoder(),
        charBuffer = CharBuffer.allocate(bufferSize),
        buffer = ByteBuffer.allocate(bufferSize)
    ).toString()
}

// Allocation free version
private suspend fun ByteReadChannel.readUntilString(
    out: StringBuilder,
    delimiter: ByteBuffer,
    decoder: CharsetDecoder,
    charBuffer: CharBuffer,
    buffer: ByteBuffer
): StringBuilder {
    decoder.reset()
    do {
        buffer.clear()
        readUntilDelimiter(delimiter, buffer)
        buffer.flip()

        if (!buffer.hasRemaining()) {
            // EOF of delimiter encountered
            //for (n in 0 until delimiter.remaining()) readByte()
            skipDelimiter(delimiter)

            charBuffer.clear()
            decoder.decode(buffer, charBuffer, true)
            charBuffer.flip()
            out.append(charBuffer)
            break
        }

        // do something with a buffer
        while (buffer.hasRemaining()) {
            charBuffer.clear()
            decoder.decode(buffer, charBuffer, false)
            charBuffer.flip()
            out.append(charBuffer)
        }
    } while (true)
    return out
}

private suspend fun ByteReadChannel.readUntil(delimiter: Byte, bufferSize: Int = 1024): ByteArray {
    return readUntil(
        out = ByteArrayOutputStream(),
        delimiter = ByteBuffer.wrap(byteArrayOf(delimiter)),
        bufferSize = bufferSize
    ).toByteArray()
}

// Allocation free version
private suspend fun ByteReadChannel.readUntil(
    out: ByteArrayOutputStream,
    delimiter: ByteBuffer,
    bufferSize: Int = 1024
): ByteArrayOutputStream {
    out.reset()
    val temp = ByteArray(bufferSize)
    val buffer = ByteBuffer.allocate(bufferSize)
    do {
        buffer.clear()
        readUntilDelimiter(delimiter, buffer)
        buffer.flip()

        if (!buffer.hasRemaining()) {
            skipDelimiter(delimiter)

            // EOF of delimiter encountered
            break
        }

        var pos = 0
        while (buffer.hasRemaining()) {
            val rem = buffer.remaining()
            buffer.get(temp, pos, rem)
            pos += rem
        }
        out.write(temp, 0, pos)
    } while (true)
    return out
}

internal suspend fun ByteReadChannel.readBytesExact(count: Int): ByteArray = readPacket(count).readBytes(count)
