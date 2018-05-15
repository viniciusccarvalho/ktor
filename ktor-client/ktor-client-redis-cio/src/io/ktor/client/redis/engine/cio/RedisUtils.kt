package io.ktor.client.redis.engine.cio

import kotlinx.coroutines.experimental.*
import java.util.*
import java.util.concurrent.atomic.*
import kotlin.coroutines.experimental.*

class OnceAsync {
    var deferred: kotlinx.coroutines.experimental.Deferred<Unit>? = null

    suspend operator fun invoke(callback: suspend () -> Unit) {
        if (deferred == null) {
            deferred = async { callback() }
        }
        return deferred!!.await()
    }
}

class AsyncTaskQueue {
    var running = AtomicBoolean(false); private set
    private var queue = LinkedList<suspend () -> Unit>()

    val queued get() = synchronized(queue) { queue.size }

    suspend operator fun <T> invoke(func: suspend () -> T): T {
        val deferred = CompletableDeferred<T>()

        synchronized(queue) {
            queue.add {
                val result = try {
                    func()
                } catch (e: Throwable) {
                    deferred.completeExceptionally(e)
                    return@add
                }
                deferred.complete(result)
            }
        }
        if (running.compareAndSet(false, true)) {
            runTasks(coroutineContext)
        }
        return deferred.await()
    }

    private fun runTasks(baseContext: CoroutineContext) {
        val item = synchronized(queue) { if (queue.isNotEmpty()) queue.remove() else null }
        if (item != null) {
            item.startCoroutine(object : Continuation<Unit> {
                override val context: CoroutineContext = baseContext
                override fun resume(value: Unit) = runTasks(baseContext)
                override fun resumeWithException(exception: Throwable) = runTasks(baseContext)
            })
        } else {
            running.set(false)
        }
    }
}

class AsyncPool<T>(val maxItems: Int = Int.MAX_VALUE, val create: suspend (index: Int) -> T) {
    var createdItems = AtomicInteger()
    private val freedItem = LinkedList<T>()
    private val waiters = LinkedList<CompletableDeferred<Unit>>()
    val availableFreed: Int get() = synchronized(freedItem) { freedItem.size }

    suspend fun <TR> tempAlloc(callback: suspend (T) -> TR): TR {
        val item = alloc()
        try {
            return callback(item)
        } finally {
            free(item)
        }
    }

    suspend fun alloc(): T {
        while (true) {
            // If we have an available item just retrieve it
            synchronized(freedItem) {
                if (freedItem.isNotEmpty()) {
                    val item = freedItem.remove()
                    if (item != null) {
                        return item
                    }
                }
            }

            // If we don't have an available item yet and we can create more, just create one
            if (createdItems.get() < maxItems) {
                val index = createdItems.getAndAdd(1)
                return create(index)
            }
            // If we shouldn't create more items and we don't have more, just await for one to be freed.
            else {
                val deferred = CompletableDeferred<Unit>()
                synchronized(waiters) {
                    waiters += deferred
                }
                deferred.await()
            }
        }
    }

    fun free(item: T) {
        synchronized(freedItem) {
            freedItem.add(item)
        }
        val waiter = synchronized(waiters) { if (waiters.isNotEmpty()) waiters.remove() else null }
        waiter?.complete(Unit)
    }
}
