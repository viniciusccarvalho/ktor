package io.ktor.network.dns

import io.ktor.network.sockets.*
import io.ktor.network.util.*
import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.channels.*
import kotlinx.coroutines.experimental.io.*
import kotlinx.coroutines.experimental.io.ByteOrder
import kotlinx.io.core.*
import java.net.*
import java.util.*

fun main(args: Array<String>) {
    runBlocking {
//        val dnsServer = inet4address(198, 41, 0, 4) // root DNS server
//            val dnsServer = inet4address(8, 8, 8, 8) // google DNS server
        val dnsServer = inet4address(77, 88, 8, 8) // yandex DNS server

//        val results = resolve(dnsServer, "kotlinlang.org", Type.A, tcp = false)
        val results = resolve(dnsServer, "github.com", Type.A, tcp = false)

        if (results.answers.isNotEmpty()) {
            println("Answers:")
            results.answers.forEach { it.printRecord() }
        }

        if (results.nameServers.isNotEmpty()) {
            println()
            println("Name servers:")
            results.nameServers.forEach { it.printRecord() }
        }

        if (results.additional.isNotEmpty()) {
            println()
            println("Additional:")
            results.additional.forEach { it.printRecord() }
        }
    }
}

suspend fun resolve(server: InetAddress, host: String, type: Type, tcp: Boolean): Message {
    val remoteAddress = InetSocketAddress(server, 53)

    return if (tcp) {
        aSocket().tcp().tcpNoDelay().connect(remoteAddress).use { socket ->
            val output = socket.openWriteChannel().apply { writeByteOrder = ByteOrder.BIG_ENDIAN }
            val input = socket.openReadChannel().apply { readByteOrder = ByteOrder.BIG_ENDIAN }

            doResolve(output, input, host, type, tcp)
        }
    } else {
        aSocket().udp().connect(remoteAddress).use { endpoint ->
            val output = endpoint.outgoing.asWriteChannel(remoteAddress)
            val input = endpoint.incoming.asReadChannel()

            doResolve(output, input, host, type, tcp = false)
        }
    }
}

private fun Resource<*>.printRecord() {
    when (this) {
        is Resource.A -> println("${type.name} ${name.joinToString(".")} $address (ttl $ttl sec)")
        is Resource.Ns -> println("NS ${name.joinToString(".")} ${nameServer.joinToString(".")} (ttl $ttl sec)")
        is Resource.SOA -> println("SOA ${name.joinToString(".")} MNAME ${mname.joinToString(".")}, RNAME ${rname.joinToString(".")}, serial $serial, refresh $refresh sec, retry $retry sec, expire $expire sec, minimum $minimum sec")
        is Resource.CName -> println("CNAME ${name.joinToString(".")} ${cname.joinToString(".")} (ttl $ttl sec)")
        is Resource.Opt -> println("OPT ${name.takeIf { it.isNotEmpty() }?.joinToString(".") ?: "<root>"}")
        is Resource.Text -> println("TEXT ${name.joinToString(".")} $texts")
        is Resource.MX -> println("MX ${name.joinToString(".")}, preference $preference, exchange ${exchange.joinToString(".")}")
        else -> println("$type ${name.joinToString(".")} ???")
    }
}

private fun inet4address(a: Int, b: Int, c: Int, d: Int): InetAddress {
    return InetAddress.getByAddress(byteArrayOf(a.toByte(), b.toByte(), c.toByte(), d.toByte()))
}

private suspend fun doResolve(out: ByteWriteChannel, input: ByteReadChannel, host: String, type: Type, tcp: Boolean): Message {
    val rnd = Random()
    val id = (rnd.nextInt() and 0xffff).toShort()

    val message = Message(
        Header(id, true, Opcode.Query, false, false, true, false, true, false, ResponseCode.OK, 1, 0, 0, 1),
        listOf(
            Question(host.split('.'), type, Class.Internet)
        ),
        emptyList(), emptyList(),
        additional = listOf(
            Resource.Opt(emptyList(), 4096, 0, 0)
        )
    )

    val encoder = Charsets.ISO_8859_1.newEncoder()

    out.writeDnsMessage(message, encoder, tcp)
    out.flush()

    val result = input.readDnsMessage(tcp)

    if (result.header.id != id) {
        System.err.println("Bad response id")
    }
    return result
}


// workaround, need to be redesigned

private fun ReceiveChannel<Datagram>.asReadChannel(): ByteReadChannel {
    return writer(ioCoroutineDispatcher) {
        while (true) {
            val datagram = receive()

            channel.writePacket(datagram.packet)
            channel.flush()
        }
    }.channel
}

private fun SendChannel<Datagram>.asWriteChannel(address: SocketAddress): ByteWriteChannel {
    return reader(ioCoroutineDispatcher) {
        val buffer: ByteBuffer = ByteBuffer.allocate(65536)

        while (true) {
            val builder = BytePacketBuilder()

            buffer.clear()
            val rc = channel.readAvailable(buffer)
            if (rc == -1) break

            var yieldCount = 0
            await@while (buffer.hasRemaining()) {
                if (channel.availableForRead == 0) {
                    if (channel.isClosedForRead) break@await
                    yield()
                    if (++yieldCount > 10) break@await
                } else {
                    if (channel.readAvailable(buffer) == -1) break
                    yieldCount = 0
                }
            }

            buffer.flip()
            builder.writeFully(buffer)

            send(Datagram(builder.build(), address))
        }

        close()
    }.channel
}
