package io.ktor.client.call

import kotlin.reflect.*

expect interface Type

class TypeInfo(val type: KClass<*>, val reifiedType: Type)

expect inline fun <reified T> typeInfo(): TypeInfo
