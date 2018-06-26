package io.ktor.server.testing

import io.ktor.server.engine.*
import io.ktor.server.jetty.*
import io.ktor.server.servlet.*
import org.eclipse.jetty.servlet.*
import javax.servlet.*


// the factory and engine are only suitable for testing
// you shouldn't use it for production code

class JettyTestServlet(private val async: Boolean) :
    ApplicationEngineFactory<JettyTestServletApplicationEngine, JettyApplicationEngineBase.Configuration> {
    override fun create(
        environment: ApplicationEngineEnvironment,
        configure: JettyApplicationEngineBase.Configuration.() -> Unit
    ): JettyTestServletApplicationEngine {
        return JettyTestServletApplicationEngine(environment, configure, async)
    }

    override fun toString(): String = if (async) "AsyncJettyTestServlet" else "BlockingJettyTestServlet"
}

class JettyTestServletApplicationEngine(
    environment: ApplicationEngineEnvironment,
    configure: JettyApplicationEngineBase.Configuration.() -> Unit,
    async: Boolean
) : JettyApplicationEngineBase(environment, configure) {
    init {
        server.handler = ServletContextHandler().apply {
            classLoader = environment.classLoader
            setAttribute(ServletApplicationEngine.ApplicationEngineEnvironmentAttributeKey, environment)

            insertHandler(ServletHandler().apply {
                val holder = ServletHolder("ktor-servlet", ServletApplicationEngine::class.java).apply {
                    isAsyncSupported = async
                    registration.setLoadOnStartup(1)
                    registration.setMultipartConfig(MultipartConfigElement(System.getProperty("java.io.tmpdir")))
                    registration.setAsyncSupported(async)
                }

                addServlet(holder)
                addServletMapping(ServletMapping().apply {
                    pathSpecs = arrayOf("*.", "/*")
                    servletName = "ktor-servlet"
                })
            })
        }
    }
}