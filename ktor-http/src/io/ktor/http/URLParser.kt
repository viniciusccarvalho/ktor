package io.ktor.http

import io.ktor.http.parsing.*

/**
 * Construct [Url] from [String]
 */
fun Url(urlString: String): Url = urlString.parseUrl()

/**
 * Parse [Url] from [CharSequence]
 * Note: only http(s) and ws(s) urls supported for now
 */
fun String.parseUrl(): Url {
    val parts = URL_PARSER.parse(this) ?: error("Invalid url format: $this")
    return URLBuilder().apply {
        protocol = URLProtocol.createOrDefault(parts["protocol"])
        port = protocol.defaultPort
        host = parts["host"]

        if (parts.contains("encodedPath")) encodedPath = parts["encodedPath"]
        parts.with("port") { port = it.toInt() }
        parts.with("user") { user = it }
        parts.with("password") { password = it }
        parts.with("fragment") { fragment = it }

        parts.with("parameters") { rawParams ->
            val rawParameters = parseQueryString(rawParams)
            rawParameters.forEach { key, values ->
                parameters.appendAll(key, values)
            }
        }
    }.build()
}

/**
 * According to https://tools.ietf.org/html/rfc1738
 */
private val safe = anyOf("$-_.+")
private val extra = anyOf("!*'(),")
private val escape = "%" then hex then hex

private val unreserved = alphaDigit or safe or extra
private val urlChar = unreserved or escape
private val protocolChar = lowAlpha or digit or anyOf("+-.")

private val protocol = (protocolChar then many(protocolChar)).named("protocol")
private val domainLabel = alphaDigit or (alphaDigit then many(alphaDigit or "-") then alphaDigit)
private val topLabel = alpha or (alpha then many(alphaDigit or "-") then alphaDigit)
private val hostName = many(domainLabel then ".") then topLabel

private val hostNumber = digits then "." then digits then "." then digits then "." then digits
private val credentialChar = urlChar or anyOf(";?&=")
private val user = (credentialChar then many(credentialChar)).named("user")
private val password = (credentialChar then many(credentialChar)).named("password")
private val auth = user then maybe(":" then password) then "@"
private val host = (hostName or hostNumber).named("host")
private val port = ":" then digits.named("port")
private val pathSegment = many(urlChar or anyOf(";&="))
private val parameters = pathSegment.named("parameters")
private val encodedPath = ("/" then pathSegment then maybe("/" then pathSegment)).named("encodedPath")
private val fragment = ("#" then maybe(pathSegment).named("fragment"))

private val URL_PARSER = grammar {
    +protocol
    +"://"
    +maybe(auth)
    +{ host then maybe(port) }
    +maybe(encodedPath then maybe("?" then parameters) then maybe(fragment))
}.buildRegexParser()


