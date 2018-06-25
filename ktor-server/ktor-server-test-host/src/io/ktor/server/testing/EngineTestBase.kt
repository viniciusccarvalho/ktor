package io.ktor.server.testing

import com.googlecode.junittoolbox.*
import io.ktor.application.*
import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.engine.*
import io.ktor.client.engine.apache.*
import io.ktor.client.engine.cio.*
import io.ktor.client.engine.jetty.*
import io.ktor.client.request.*
import io.ktor.client.response.*
import io.ktor.features.*
import io.ktor.network.tls.certificates.*
import io.ktor.routing.*
import io.ktor.server.engine.*
import io.ktor.server.tomcat.*
import kotlinx.coroutines.experimental.*
import org.junit.*
import org.junit.rules.*
import org.junit.runner.*
import org.junit.runners.*
import org.junit.runners.model.*
import org.slf4j.*
import org.slf4j.Logger
import java.io.*
import java.net.*
import java.security.*
import java.util.concurrent.*
import java.util.logging.*
import javax.net.ssl.*
import kotlin.test.*

@RunWith(ParallelParameterized::class)
abstract class EngineTestBase<TConfiguration : ApplicationEngine.Configuration>(
    val factoryWithConfig: EngineFactoryWithConfig<ApplicationEngine, TConfiguration>,
    clientEngineFactory: HttpClientEngineFactory<HttpClientEngineConfig>,
    val mode: TestMode
) {
    class PublishedTimeout(val seconds: Long) : Timeout(seconds, TimeUnit.SECONDS)

    protected val isUnderDebugger: Boolean =
        java.lang.management.ManagementFactory.getRuntimeMXBean().inputArguments.orEmpty()
            .any { "-agentlib:jdwp" in it }

    protected var port = findFreePort()
    protected var sslPort = findFreePort()
    protected var server: ApplicationEngine? = null
    protected var callGroupSize = -1
        private set
    protected val exceptions = ArrayList<Throwable>()

    private val allConnections = CopyOnWriteArrayList<HttpURLConnection>()

    val testLog: Logger = LoggerFactory.getLogger("EngineTestBase")

    @get:Rule
    val test = TestName()

    @get:Rule
    open val timeout = PublishedTimeout(
        if (isUnderDebugger) 1000000L else (System.getProperty("host.test.timeout.seconds")?.toLong() ?: 120L)
    )

    protected val socketReadTimeout by lazy { timeout.seconds.toInt() * 1000 }

    protected val client = HttpClient(clientEngineFactory)

    @Before
    fun setUpBase() {
        testLog.trace("Starting server on port $port (SSL $sslPort)")
        exceptions.clear()
    }

    @After
    fun tearDownBase() {
        allConnections.forEach { it.disconnect() }
        testLog.trace("Disposing server on port $port (SSL $sslPort)")
        server?.stop(1000, 5000, TimeUnit.MILLISECONDS)
        if (exceptions.isNotEmpty()) {
            fail("Server exceptions logged, consult log output for more information")
        }
    }

    protected open fun createServer(log: Logger?, module: Application.() -> Unit): ApplicationEngine {
        val _port = this.port
        val environment = applicationEngineEnvironment {
            val delegate = LoggerFactory.getLogger("ktor.test")
            this.log = log ?: object : Logger by delegate {
                override fun error(msg: String?, cause: Throwable?) {
                    cause?.let { exceptions.add(it) }
                    delegate.error(msg, cause)
                }
            }

            connector { port = _port }
            if (mode != TestMode.HTTP) {
                sslConnector(keyStore, "mykey", { "changeit".toCharArray() }, { "changeit".toCharArray() }) {
                    this.port = sslPort
                    this.keyStorePath = keyStoreFile.absoluteFile
                }
            }

            module(module)
        }

        return embeddedServer(factoryWithConfig.factory, environment) {
            this@EngineTestBase.callGroupSize = callGroupSize
            factoryWithConfig.configuration(this)
        }
    }

    protected open fun features(application: Application, routingConfigurer: Routing.() -> Unit) {
        application.install(CallLogging)
        application.install(Routing, routingConfigurer)
    }

    protected fun createAndStartServer(log: Logger? = null, routingConfigurer: Routing.() -> Unit): ApplicationEngine {
        var lastFailures = emptyList<Throwable>()
        for (attempt in 1..5) {
            val server = createServer(log) {
                features(this, routingConfigurer)
            }

            val failures = startServer(server)
            when {
                failures.isEmpty() -> return server
                failures.any { it is BindException } -> {
                    port = findFreePort()
                    sslPort = findFreePort()
                    server.stop(1L, 1L, TimeUnit.SECONDS)
                    lastFailures = failures
                }
                else -> {
                    server.stop(1L, 1L, TimeUnit.SECONDS)
                    throw MultipleFailureException(failures)
                }
            }
        }

        throw MultipleFailureException(lastFailures)
    }

    private fun startServer(server: ApplicationEngine): List<Throwable> {
        this.server = server

        val l = CountDownLatch(1)
        val failures = CopyOnWriteArrayList<Throwable>()

        val starting = launch(CommonPool + CoroutineExceptionHandler { _, _ -> }) {
            l.countDown()
            server.start()
        }
        l.await()

        val waitForPorts = launch(CommonPool) {
            server.environment.connectors.forEach { connector ->
                waitForPort(connector.port)
            }
        }

        starting.invokeOnCompletion { cause ->
            if (cause != null) {
                failures.add(cause)
                waitForPorts.cancel()
            }
        }

        runBlocking {
            waitForPorts.join()
        }

        return failures
    }

    protected fun findFreePort() = ServerSocket(0).use { it.localPort }

    protected fun withUrl(
        path: String, builder: HttpRequestBuilder.() -> Unit = {}, block: suspend HttpResponse.(Int) -> Unit
    ): Unit = when (mode) {
        TestMode.HTTP -> withUrl(URL("http://127.0.0.1:$port$path"), port, builder, block)
        else -> withUrl(URL("https://127.0.0.1:$sslPort$path"), sslPort, builder, block)
    }

    protected fun socket(block: Socket.() -> Unit) {
        Socket("localhost", port).use { socket ->
            socket.tcpNoDelay = true
            socket.soTimeout = socketReadTimeout

            block(socket)
        }
    }

    private fun withUrl(
        url: URL, port: Int, builder: HttpRequestBuilder.() -> Unit,
        block: suspend HttpResponse.(Int) -> Unit
    ) = runBlocking {
        withTimeout(timeout.seconds, TimeUnit.SECONDS) {
            client.call(url, builder).response.use { response ->
                block(response, port)
            }
        }
    }

    companion object {
        val keyStoreFile = File("build/temp.jks")
        lateinit var keyStore: KeyStore
        lateinit var sslContext: SSLContext
        lateinit var trustManager: X509TrustManager

        @BeforeClass
        @JvmStatic
        fun setupAll() {
            keyStore = generateCertificate(keyStoreFile, algorithm = "SHA256withECDSA", keySizeInBits = 256)
            val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm())
            tmf.init(keyStore)
            sslContext = SSLContext.getInstance("TLS")
            sslContext.init(null, tmf.trustManagers, null)
            trustManager = tmf.trustManagers.first { it is X509TrustManager } as X509TrustManager
        }

        val JettyServer = testServer(io.ktor.server.jetty.Jetty)
        val NettyServer = testServer(io.ktor.server.netty.Netty) {
            shareWorkGroup = true
        }

        val CIOServer = testServer(io.ktor.server.cio.CIO)
        val TomcatServer = testServer(Tomcat) {
            listOf("org.apache.coyote", "org.apache.tomcat", "org.apache.catalina").map {
                java.util.logging.Logger.getLogger(it).apply { level = Level.WARNING }
            }
        }

        val JettyAsyncServletServer = testServer(JettyTestServlet(async = true))
        val JettyBlockingServletServer = testServer(JettyTestServlet(async = false))

        val ApacheClient = Apache.config { sslContext = Companion.sslContext }
        val CIOClient = CIO.config { https.trustManager = trustManager }
        val JettyClient = Jetty

        @Parameterized.Parameters(name = "server:{0}, client:{1}, mode:{2}")
        @JvmStatic
        fun parameters() =
            combine(
                listOf(NettyServer, JettyServer, TomcatServer, JettyAsyncServletServer, JettyBlockingServletServer),
                listOf(ApacheClient, CIOClient),
                listOf(TestMode.HTTP, TestMode.HTTPS)
            ) + combine(
                listOf(CIOServer),
                listOf(CIOClient, ApacheClient),
                listOf(TestMode.HTTP)
            ) + combine(
                listOf(NettyServer, JettyServer, JettyAsyncServletServer, JettyBlockingServletServer),
                listOf(JettyClient),
                listOf(TestMode.HTTP2)
            )

        private suspend fun CoroutineScope.waitForPort(port: Int) {
            do {
                delay(50)
                try {
                    Socket("localhost", port).close()
                    break
                } catch (expected: IOException) {
                }
            } while (isActive)
        }
    }
}
