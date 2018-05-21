package io.ktor.network.dns

import kotlinx.coroutines.experimental.io.*
import kotlinx.io.core.*
import java.net.*
import java.nio.charset.*

suspend fun ByteReadChannel.readDnsMessage(tcp: Boolean): Message {
    if (tcp) {
        readShort() // TODO process parts
    }

    val support = DomainNameCompressionSupport()
    val decoder = Charsets.ISO_8859_1.newDecoder()
    val header = readHeader()

    return Message(header,
        (1..header.questionsCount).mapNotNull { readQuestion(decoder, support) },
        (1..header.answersCount).mapNotNull { readResource(decoder, support) },
        (1..header.nameServersCount).mapNotNull { readResource(decoder, support) },
        (1..header.additionalResourcesCount).mapNotNull { readResource(decoder, support) }
    )
}

private class DomainNameCompressionSupport {
    val stringMap = HashMap<Int, List<String>>()
    var currentOffset = 12
}

private suspend fun ByteReadChannel.readHeader(): Header {
    val id = readShort()
    val flags1 = readByte()
    val flags2 = readByte()

    val questionsCount = readUShort()
    val answersCount = readUShort()
    val nameServersCount = readUShort()
    val additionalCount = readUShort()

    val opcode = Opcode.byValue[(flags1.toInt() and 0xff) shr 3 and 0xf]
        ?: throw IllegalArgumentException("Wrong opcode")
    val responseCode = ResponseCode.byValue[flags2.toInt() and 0xf]
        ?: throw IllegalArgumentException("Wrong response code")

    return Header(id,
        isQuery = !flags1.isBitSet(7),
        opcode = opcode,
        authoritativeAnswer = flags1.isBitSet(2),
        truncation = flags1.isBitSet(1),
        recursionDesired = flags1.isBitSet(0),
        recursionAvailable = flags2.isBitSet(7),
        authenticData = flags2.isBitSet(5),
        checkingDisabled = flags2.isBitSet(4),
        responseCode = responseCode,
        questionsCount = questionsCount,
        answersCount = answersCount,
        nameServersCount = nameServersCount,
        additionalResourcesCount = additionalCount
    )
}

@Suppress("NOTHING_TO_INLINE")
private inline fun Byte.isBitSet(n: Int): Boolean = ((toInt() and 0xff) and (1 shl n)) != 0

private suspend fun ByteReadChannel.readQuestion(decoder: CharsetDecoder, support: DomainNameCompressionSupport): Question? {
    val name = readName(decoder, support)

    val typeValue = readUShort()
    val type = Type.byValue[typeValue]

    if (type == null) {
        System.err.println("Wrong question record type $typeValue")
        readUShort()
        return null
    }

    val classValue = readUShort()
    val qclass = Class.byValue[classValue]

    if (qclass == null) {
        System.err.println("Wrong question record class $classValue")
        return null
    }

    support.currentOffset += 4

    return Question(name, type, qclass)
}

private suspend fun ByteReadChannel.readResource(decoder: CharsetDecoder, support: DomainNameCompressionSupport): Resource<*>? {
//    val startOffset = support.currentOffset
    val name = readName(decoder, support)

    val typeValue = readUShort()
    val value1 = readUShort() // value1 and value2 are interpreted according to the type
    val value2 = readUInt()
    val length = readUShort()

    support.currentOffset += 10

    val type = Type.byValue[typeValue]
//    println("Got $type ($typeValue) at $startOffset")
    val result: Resource<*>? = when (type) {
        Type.A -> readA4(value1, length, support, name, ttl = value2)
        Type.AAAA -> readA6(value1, length, support, name, ttl = value2)
        Type.NS -> readNS(value1, length, decoder, support, name, ttl = value2)
        Type.OPT -> readOPT(name, length, support, value1, value2)
        Type.SOA -> readSOA(decoder, support, name)
        Type.CNAME -> readCNAME(decoder, support, name, value2)
        Type.TXT -> readTXT(length, decoder, support, name)
        Type.MX -> readMX(support, decoder, name)
        Type.SPF -> null
        else -> null // TODO more types
    }

    if (result == null) {
        System.err.println("Unable to parse record of type $typeValue (${type
            ?: "Unknown type"}), index ${support.currentOffset}")
        skipExact(length)  // we simply skip unsupported record types
        support.currentOffset += length
    }

    return result
}

private suspend fun ByteReadChannel.readMX(support: DomainNameCompressionSupport,
                                           decoder: CharsetDecoder,
                                           name: List<String>): Resource.MX {
    val preference = readUShort()
    support.currentOffset += 2
    val exchange = readName(decoder, support)

    return Resource.MX(name, preference, exchange)
}

private suspend fun ByteReadChannel.readTXT(length: Int,
                                            decoder: CharsetDecoder,
                                            support: DomainNameCompressionSupport,
                                            name: List<String>): Resource.Text {
    var remaining = length
    val texts = ArrayList<String>(2)

    while (remaining > 0) {
        val textSize = readUByte()

        val text = readPacket(textSize).readText(decoder)

        texts += text
        support.currentOffset += textSize + 1 // it is not compressed however we have to move currentOffset
        remaining -= textSize + 1
    }

    return Resource.Text(name, texts, length)
}

private suspend fun ByteReadChannel.readCNAME(decoder: CharsetDecoder,
                                              support: DomainNameCompressionSupport,
                                              name: List<String>,
                                              ttl: Long): Resource.CName {
    val cname = readName(decoder, support)
    return Resource.CName(name, cname, ttl)
}

private suspend fun ByteReadChannel.readSOA(decoder: CharsetDecoder,
                                            support: DomainNameCompressionSupport,
                                            name: List<String>): Resource.SOA {
    val mname = readName(decoder, support)
    val rname = readName(decoder, support)

    val serial = readUInt()
    val refresh = readUInt()
    val retry = readUInt()
    val expire = readUInt()
    val minimum = readUInt()

    support.currentOffset += 20

    return Resource.SOA(name, mname, rname, serial, refresh, retry, expire, minimum)
}

private suspend fun ByteReadChannel.readOPT(name: List<String>,
                                            length: Int,
                                            support: DomainNameCompressionSupport,
                                            udpPayloadSize: Int,
                                            value2: Long): Resource.Opt {
    if (name.isNotEmpty()) {
        System.err.println("OPT record should have root name")
    }

    if (length > 0) {
        skipExact(length)
        support.currentOffset += length
    }

    // just to eliminate warning, we actually don't support EDNS0
    return Resource.Opt(name, udpPayloadSize,
        extendedRCode = ((value2 shr 24) and 0xffL).toByte(),
        version = ((value2 shr 16) and 0xffL).toByte())
}

private suspend fun ByteReadChannel.readNS(value1: Int, length: Int, decoder: CharsetDecoder, support: DomainNameCompressionSupport, name: List<String>, ttl: Long): Resource.Ns? {
    val qClass = Class.byValue[value1]
    return if (length == 0 || qClass == null) {
        null
    } else {
        val nameServer = readName(decoder, support)
        Resource.Ns(name, qClass, nameServer, ttl)
    }
}

private suspend fun ByteReadChannel.readA6(value1: Int,
                                           length: Int,
                                           support: DomainNameCompressionSupport,
                                           name: List<String>,
                                           ttl: Long): Resource.A.V6? {
    val qClass = Class.byValue[value1]

    return if (length == 16 && qClass != null) {
        val buffer = ByteBuffer.allocate(16)
        readFully(buffer)
        support.currentOffset += 16
        Resource.A.V6(name, qClass, InetAddress.getByAddress(buffer.array()) as Inet6Address, ttl)
    } else null
}

private suspend fun ByteReadChannel.readA4(value1: Int,
                                           length: Int,
                                           support: DomainNameCompressionSupport,
                                           name: List<String>,
                                           ttl: Long): Resource.A.V4? {
    val qClass = Class.byValue[value1]

    return if (length == 4 && qClass != null) {
        val buffer = ByteBuffer.allocate(4)
        readFully(buffer)
        support.currentOffset += 4
        Resource.A.V4(name, qClass, InetAddress.getByAddress(buffer.array()) as Inet4Address, ttl)
    } else null
}

private suspend fun ByteReadChannel.readName(decoder: CharsetDecoder,
                                             support: DomainNameCompressionSupport): List<String> {
    val initialOffset = support.currentOffset
    var currentOffset = initialOffset
    val name = ArrayList<String>()

    do {
        val partLength = readByte().toInt() and 0xff
        if (partLength == 0) {
            currentOffset++
            break
        } else if (partLength and 0xc0 == 0xc0) { // two higher bits are 11 so use compressed
            val lower = readByte().toInt() and 0xff
            val offset = lower or ((partLength and 0x3f) shl 8)

            if (offset >= currentOffset) {
                // TODO in theory compressed message could have forward references
                // however for now I haven't faced it yet
                throw UnsupportedOperationException("Forward references are not supported")
            }

            val referred = support.stringMap[offset] ?: illegalOffset(offset, support, initialOffset)

            updateSupport(support, initialOffset, name, referred)
            currentOffset += 2
            support.currentOffset = currentOffset
            return name + referred
        }

        name += getStringByRawLength(partLength, decoder)
        currentOffset += partLength + 1
    } while (true)

    updateSupport(support, initialOffset, name, emptyList())
    support.currentOffset = currentOffset

    return name
}

private fun illegalOffset(offset: Int, support: DomainNameCompressionSupport, initialOffset: Int): Nothing {
    throw IllegalArgumentException("Illegal offset $offset for compressed domain name, " +
        "known offsets are ${support.stringMap.keys}, name start $initialOffset")
}

private fun updateSupport(support: DomainNameCompressionSupport,
                          initialOffset: Int,
                          before: List<String>,
                          after: List<String>) {

    for (idx in 0 until before.size) {
        val index = initialOffset + before.subList(0, idx).sumBy { it.length + 1 }
        val value = when {
            idx == 0 && after.isEmpty() -> before
            else -> before.subList(idx, before.size) + after
        }

        support.stringMap[index] = value
    }
}

private suspend fun ByteReadChannel.getStringByRawLength(length: Int, decoder: CharsetDecoder): String {
    return readPacket(length).readText(decoder)
}

private suspend fun ByteReadChannel.skipExact(length: Int) {
    readPacket(length).release()
}

private suspend inline fun ByteReadChannel.readUByte(): Int = readByte().toInt() and 0xff
private suspend inline fun ByteReadChannel.readUShort(): Int = readShort().toInt() and 0xffff
private suspend inline fun ByteReadChannel.readUInt(): Long = readInt().toLong() and 0xffffffffL

