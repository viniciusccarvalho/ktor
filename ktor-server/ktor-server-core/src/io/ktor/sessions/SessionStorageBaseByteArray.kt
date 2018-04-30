package io.ktor.sessions

import kotlinx.coroutines.experimental.io.*
import java.io.*
import kotlin.coroutines.experimental.*

/**
 * Simplified [SessionStorage] that uses [ByteArray]
 * instead of [ByteWriteChannel] and [ByteReadChannel].
 *
 * Useful for small sessions and storages not supporting streaming like databases.
 */
abstract class SessionStorageBaseByteArray : SessionStorage {
    abstract suspend fun read(id: String): ByteArray?
    abstract suspend fun write(id: String, data: ByteArray?): Unit

    override suspend fun invalidate(id: String) {
        write(id, null)
    }

    override suspend fun <R> read(id: String, consumer: suspend (ByteReadChannel) -> R): R {
        val data = read(id) ?: throw NoSuchElementException("Session $id not found")
        return consumer(ByteReadChannel(data))
    }

    override suspend fun write(id: String, provider: suspend (ByteWriteChannel) -> Unit) {
        return provider(reader(coroutineContext, autoFlush = true) {
            val data = ByteArrayOutputStream()
            val temp = ByteArray(1024)
            while (!channel.isClosedForRead) {
                val read = channel.readAvailable(temp)
                if (read <= 0) break
                data.write(temp, 0, read)
            }
            write(id, data.toByteArray())
        }.channel)
    }
}
