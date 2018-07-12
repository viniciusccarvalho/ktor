package io.ktor.http.parsing


internal interface Parser {
    fun parse(input: String): ParseResult?
}

internal class RegexParser(
    private val expression: Regex,
    private val indexes: Map<String, List<Int>>
) : Parser {
    override fun parse(input: String): ParseResult? {
        val match = expression.matchEntire(input)
        if (match == null || match.value.length != input.length) {
            return null
        }

        val mapping = mutableMapOf<String, List<String>>()
        indexes.forEach { (key, locations) ->
            locations.forEach { index ->
                val result = mutableListOf<String>()
                match.groups[index]?.let { result += it.value }
                if (result.isNotEmpty()) mapping[key] = result
            }
        }

        return ParseResult(mapping)
    }
}

internal class ParseResult(
    private val mapping: Map<String, List<String>>
) {
    operator fun get(key: String): String = mapping[key]?.firstOrNull() ?: error("No match found: $key")
    fun getAll(key: String): List<String> = mapping[key] ?: emptyList()

    fun contains(key: String): Boolean = mapping.contains(key)
}
