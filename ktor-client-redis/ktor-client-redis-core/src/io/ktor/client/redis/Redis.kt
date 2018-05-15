package io.ktor.client.redis

import kotlinx.coroutines.experimental.channels.*
import java.nio.charset.*

interface RedisEngine {
    fun createClient(host: String, port: Int, charset: Charset, password: String?): Redis
    fun createPubSubClient(host: String, port: Int, charset: Charset, password: String?): RedisPubSub
}

/**
 * A Redis basic interface exposing emiting commands receiving their responses.
 *
 * Specific commands are exposed as extension methods.
 */
interface Redis {
    val charset: Charset get() = Charsets.UTF_8

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

class RedisResponseException(message: String) : Exception(message)

interface RedisPubSub {
    val charset: Charset get() = Charsets.UTF_8

    interface Event
    data class Subscription(
        val subscribe: Boolean,
        val isPattern: Boolean,
        val channel: String,
        val subscriptors: Long
    ) : Event

    data class Message(val pattern: String?, val channel: String, val message: String) : Event

    suspend fun events(): ReceiveChannel<Event>
    suspend fun subscribeOp(op: String, channel: String)
}

suspend fun RedisPubSub.psubscribe(pattern: String): RedisPubSub = this.apply { subscribeOp("psubscribe", pattern) }
suspend fun RedisPubSub.punsubscribe(pattern: String): RedisPubSub = this.apply { subscribeOp("punsubscribe", pattern) }
suspend fun RedisPubSub.subscribe(channel: String): RedisPubSub = this.apply { subscribeOp("subscribe", channel) }
suspend fun RedisPubSub.unsubscribe(channel: String): RedisPubSub = this.apply { subscribeOp("unsubscribe", channel) }

suspend fun RedisPubSub.subscriptions(): ReceiveChannel<RedisPubSub.Subscription> =
    events().mapNotNull { it as? RedisPubSub.Subscription }

suspend fun RedisPubSub.messages(): ReceiveChannel<RedisPubSub.Message> =
    events().mapNotNull { it as? RedisPubSub.Message }

/**
 * Constructs a Redis multi-client that will connect to [addresses] in a round-robin fashion keeping a connection pool,
 * keeping as much as [maxConnections] and using the [charset].
 * Optionally you can define the [password] of the connection.
 * You can specify a [stats] object that will be populated by the clients.
 */
fun Redis(
    engine: RedisEngine,
    host: String = "127.0.0.1",
    port: Int = 6379,
    charset: Charset = Charsets.UTF_8,
    password: String? = null
): Redis {
    return engine.createClient(host, port, charset, password)
}

fun RedisPubSub(
    engine: RedisEngine,
    host: String = "127.0.0.1",
    port: Int = 6379,
    charset: Charset = Charsets.UTF_8,
    password: String? = null
): RedisPubSub {
    return engine.createPubSubClient(host, port, charset, password)
}

@Suppress("UNCHECKED_CAST")
suspend fun Redis.commandArrayString(vararg args: Any?): List<String> =
    (commandAny(*args) as List<String>?) ?: listOf()

suspend fun Redis.commandArrayLong(vararg args: Any?): List<Long> =
    (commandAny(*args) as List<Long>?) ?: listOf()

suspend fun Redis.commandString(vararg args: Any?): String? = commandAny(*args)?.toString()
suspend fun Redis.commandLong(vararg args: Any?): Long = commandAny(*args)?.toString()?.toLongOrNull() ?: 0L
suspend fun Redis.commandUnit(vararg args: Any?): Unit = run { commandAny(*args) }

