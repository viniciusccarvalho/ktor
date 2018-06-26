package io.ktor.server.testing

import io.ktor.http.*
import io.ktor.server.engine.*
import kotlin.coroutines.experimental.*
import kotlin.test.*

object On

object It

enum class TestMode {
    HTTP,
    HTTPS,
    HTTP2
}

class EngineFactoryWithConfig<TEngine : ApplicationEngine, TConfig : ApplicationEngine.Configuration>(
    val factory: ApplicationEngineFactory<TEngine, TConfig>,
    val configuration: TConfig.() -> Unit
) {
    override fun toString(): String = factory.toString()
}

fun <TEngine : ApplicationEngine, TConfig : ApplicationEngine.Configuration> testServer(
    factory: ApplicationEngineFactory<TEngine, TConfig>,
    configuration: TConfig.() -> Unit = {}
): EngineFactoryWithConfig<TEngine, TConfig> = EngineFactoryWithConfig(factory, configuration)

@Suppress("UNUSED_PARAMETER")
fun on(comment: String, body: On.() -> Unit) = On.body()

@Suppress("UNUSED_PARAMETER")
inline fun On.it(description: String, body: It.() -> Unit) = It.body()

internal suspend fun assertFailsSuspend(block: suspend () -> Unit): Throwable {
    var exception: Throwable? = null
    try {
        block()
    } catch (cause: Throwable) {
        exception = cause
    }

    assertNotNull(exception)
    return exception!!
}

fun TestApplicationResponse.contentType(): ContentType {
    val contentTypeHeader = requireNotNull(headers[HttpHeaders.ContentType])
    return ContentType.parse(contentTypeHeader)
}

internal fun combine(vararg data: List<Any>): Array<Array<Any>> = buildSequence {
    if (data.isEmpty()) return@buildSequence
    val head = data.first().asSequence()
    if (data.size == 1) {
        head.forEach { it: Any -> yield(sequenceOf(it)) }
        return@buildSequence
    }

    val tail = combine(*data.sliceArray(1 until data.size))

    head.forEach { headElement ->
        tail.forEach { tailElement ->
            yield(sequenceOf(headElement) + tailElement)
        }
    }
}.map { it.toList().toTypedArray() }.toList().toTypedArray()
