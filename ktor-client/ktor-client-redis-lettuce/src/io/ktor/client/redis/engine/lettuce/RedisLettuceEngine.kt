package io.ktor.client.redis.engine.lettuce

import io.ktor.client.redis.*
import io.lettuce.core.codec.*
import io.lettuce.core.output.*
import io.lettuce.core.protocol.*
import io.lettuce.core.pubsub.*
import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.channels.*
import java.nio.charset.*
import kotlin.coroutines.experimental.*

open class RedisLettuceEngine : RedisEngine {
    companion object : RedisLettuceEngine()

    override fun createClient(host: String, port: Int, charset: Charset, password: String?): Redis =
        RedisLettuceClient(host, port, charset, password)

    override fun createPubSubClient(host: String, port: Int, charset: Charset, password: String?): RedisPubSub =
        RedisLettucePubSubClient(host, port, charset, password)
}

class RedisLettuceClient(
    val host: String = "127.0.0.1",
    val port: Int = 6379,
    override val charset: Charset = Charsets.UTF_8,
    val password: String? = null
) : Redis {
    val codec = StringCodec(charset)
    val client = io.lettuce.core.RedisClient.create("redis://$host:$port")
    val async = client.connect().async().apply {
        setAutoFlushCommands(true)
        if (password != null) auth(password)
    }

    override suspend fun commandAny(vararg args: Any?): Any? = suspendCoroutine { c ->
        val keyword = StringProtocolKeyword(args.first().toString())
        val commandArgs = CommandArgs(codec)
        for (n in 1 until args.size) commandArgs.add(args[n].toString())
        val output = object : NestedMultiOutput<String, String>(codec) {
            fun getROut() = output.firstOrNull()
        }
        val response = async.dispatch(keyword, output, commandArgs)
        response.handleAsync { t, u: Throwable? ->
            when {
                u != null -> c.resumeWithException(u)
                output.getROut() is Throwable -> c.resumeWithException(output.getROut() as Throwable)
                else -> c.resume(output.getROut())
            }
        }
    }

    class StringProtocolKeyword(val str: String) : ProtocolKeyword {
        val _bytes = str.toByteArray(Charsets.UTF_8)
        override fun getBytes(): ByteArray = _bytes
        override fun name(): String = str
    }
}

class RedisLettucePubSubClient(
    val host: String = "127.0.0.1",
    val port: Int = 6379,
    override val charset: Charset = Charsets.UTF_8,
    val password: String? = null
) : RedisPubSub {
    val codec = StringCodec(charset)
    val client = io.lettuce.core.RedisClient.create("redis://$host:$port")
    val pubSub = client.connectPubSub()
    val async = pubSub.async().apply {
        setAutoFlushCommands(true)
        if (password != null) auth(password)
    }

    override suspend fun events(): ReceiveChannel<RedisPubSub.Event> {
        val rchannel = Channel<RedisPubSub.Event>(Channel.UNLIMITED)
        lateinit var listener: RedisPubSubAdapter<String, String>
        listener = object : RedisPubSubAdapter<String, String>() {
            private fun send(event: RedisPubSub.Event) {
                if (rchannel.isClosedForReceive || rchannel.isClosedForSend) {
                    pubSub.removeListener(listener)
                }
                launch {
                    rchannel.send(event)
                }
            }

            override fun psubscribed(pattern: String, count: Long) =
                send(RedisPubSub.Subscription(true, true, pattern, count))

            override fun punsubscribed(pattern: String, count: Long) =
                send(RedisPubSub.Subscription(false, true, pattern, count))

            override fun unsubscribed(channel: String, count: Long) =
                send(RedisPubSub.Subscription(false, false, channel, count))

            override fun subscribed(channel: String, count: Long) =
                send(RedisPubSub.Subscription(true, false, channel, count))

            override fun message(channel: String, message: String) =
                send(RedisPubSub.Message(null, channel, message))

            override fun message(pattern: String, channel: String, message: String) =
                send(RedisPubSub.Message(pattern, channel, message))

        }
        pubSub.addListener(listener)
        return rchannel
    }

    override suspend fun subscribeOp(op: String, channel: String) {
        when (op) {
            "psubscribe" -> async.psubscribe(channel)
            "subscribe" -> async.subscribe(channel)
            "unsubscribe" -> async.unsubscribe(channel)
            "punsubscribe" -> async.punsubscribe(channel)
        }
    }
}
