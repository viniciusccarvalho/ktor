package io.ktor.server.testing

import io.ktor.application.*
import io.ktor.client.engine.*
import io.ktor.content.*
import io.ktor.response.*
import io.ktor.routing.*
import io.ktor.server.engine.*
import kotlinx.coroutines.experimental.io.jvm.javaio.*
import org.junit.*
import org.junit.Test
import java.io.*
import java.net.*
import java.util.*
import kotlin.test.*


class TransferEngineTest<TConfiguration : ApplicationEngine.Configuration>(
    hostFactory: EngineFactoryWithConfig<ApplicationEngine, TConfiguration>,
    clientFactory: HttpClientEngineFactory<HttpClientEngineConfig>,
    mode: TestMode
) : EngineTestBase<TConfiguration>(hostFactory, clientFactory, mode) {

    @Test
    fun testBigFile() {
        val file = File("build/large-file.dat")
        val rnd = Random()

        if (!file.exists()) {
            file.bufferedWriter().use { out ->
                for (line in 1..9000000) {
                    for (col in 1..(30 + rnd.nextInt(40))) {
                        out.append('a' + rnd.nextInt(25))
                    }
                    out.append('\n')
                }
            }
        }

        val originalSha1WithSize = file.inputStream().use { it.sha1WithSize() }

        createAndStartServer {
            get("/file") {
                call.respond(LocalFileContent(file))
            }
        }

        withUrl("/file") {
            assertEquals(originalSha1WithSize, content.toInputStream().sha1WithSize())
        }
    }

    @Test
    fun testBigFileHttpUrlConnection() {
        val file = File("build/large-file.dat")
        val rnd = Random()

        if (!file.exists()) {
            file.bufferedWriter().use { out ->
                for (line in 1..9000000) {
                    for (col in 1..(30 + rnd.nextInt(40))) {
                        out.append('a' + rnd.nextInt(25))
                    }
                    out.append('\n')
                }
            }
        }

        val originalSha1WithSize = file.inputStream().use { it.sha1WithSize() }

        createAndStartServer {
            get("/file") {
                call.respond(LocalFileContent(file))
            }
        }

        val connection = URL("http://localhost:$port/file").openConnection(Proxy.NO_PROXY) as HttpURLConnection
        connection.connectTimeout = 10_000
        connection.readTimeout = 10_000

        try {
            assertEquals(originalSha1WithSize, connection.inputStream.sha1WithSize())
        } finally {
            connection.disconnect()
        }
    }
}
