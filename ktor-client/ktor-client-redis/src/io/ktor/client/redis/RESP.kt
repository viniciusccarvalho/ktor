package io.ktor.client.redis

import kotlinx.coroutines.experimental.io.*
import kotlinx.io.core.*
import java.io.*
import java.nio.*
import java.nio.ByteBuffer
import java.nio.charset.*

/**
 * REdis Serialization Protocol: https://redis.io/topics/protocol
 */
object RESP {
    private const val LF = '\n'.toByte()
    private val LF_BB = ByteBuffer.wrap(byteArrayOf(LF))

    /**
     * Reader for the RESP protocol. You can call only one readValue at once.
     */
    class Reader(val bufferSize: Int = 1024, val charset: Charset) {
        private val charsetDecoder = charset.newDecoder()
        private val valueSB = StringBuilder(bufferSize)
        private val valueCB = CharBuffer.allocate(bufferSize)
        private val valueBB = ByteBuffer.allocate((bufferSize / charsetDecoder.maxCharsPerByte()).toInt())
        private val tempCRLF = ByteArray(2)

        suspend fun readValue(reader: ByteReadChannel): Any? {
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
                        val data = reader.readPacket(bytesToRead).readBytes()
                        reader.readFully(tempCRLF) // CR LF
                        data.toString(charset)
                    }
                }
                '*' -> { // Array reply
                    val arraySize = line.substring(1).toInt()
                    (0 until arraySize).map { readValue(reader) }
                }
                else -> throw RedisResponseException("Unknown param type '${line[0]}'")
            }
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
    }

    /**
     * Writer for the RESP protocol.
     */
    class Writer(val charset: Charset) {
        private val cmd = BlobBuilder(1024, charset)
        private val chunk = BlobBuilder(1024, charset)

        fun writeCommandTo(args: Array<out Any?>, out: ByteArrayOutputStream) {
            fillCmd(args)
            cmd.writeTo(out)
        }

        private fun fillCmd(args: Array<out Any?>) {
            cmd.reset()
            cmd.append('*')
            cmd.append(args.size)
            cmd.append('\r')
            cmd.append('\n')
            for (arg in args) {
                chunk.reset()
                when (arg) {
                    is Int -> chunk.append(arg)
                    is Long -> chunk.append(arg)
                    else -> chunk.append(arg.toString())
                }
                // Length of the argument.
                cmd.append('$')
                cmd.append(chunk.size())
                cmd.append('\r')
                cmd.append('\n')
                cmd.append(chunk)
                cmd.append('\r')
                cmd.append('\n')
            }
        }
    }
}
