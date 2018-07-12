package io.ktor.http.parsing

internal fun Grammar.buildRegexParser(): Parser {
    fun <T> MutableList<T>.pop(): T = removeAt(size - 1)

    val groups = mutableMapOf<String, MutableList<Int>>()
    val expression = toRegex(groups)

    return RegexParser(Regex(expression.regex), groups)
}

private class GrammarRegex(val regex: String, val groupsCount: Int = 0)

/**
 * Convert grammar to regex.
 * [offset] first available group index
 */
private fun Grammar.toRegex(groups: MutableMap<String, MutableList<Int>>, offset: Int = 1): GrammarRegex = when (this) {
    is StringGrammar -> GrammarRegex(Regex.escape(value))
    is RawGrammar -> GrammarRegex(value)
    is NamedGrammar -> {
        val nested = grammar.toRegex(groups, offset + 1)
        groups.add(name, offset)
        GrammarRegex("(${nested.regex})", nested.groupsCount + 1)
    }
    is SequenceGrammar -> {
        var currentOffset = offset + 1
        val expression = StringBuilder()

        expression.append("(")
        grammars.forEach { grammar ->
            val current = grammar.toRegex(groups, currentOffset)

            expression.append(current.regex)
            currentOffset += current.groupsCount
        }
        expression.append(")")

        GrammarRegex(expression.toString(), currentOffset - offset)
    }
    is OrGrammar -> {
        var currentOffset = offset + 1
        val expression = StringBuilder()

        expression.append("(")
        grammars.forEachIndexed { index, grammar ->
            val current = grammar.toRegex(groups, currentOffset)

            if (index != 0) expression.append("|")
            expression.append(current.regex)
            currentOffset += current.groupsCount
        }
        expression.append(")")

        GrammarRegex(expression.toString(), currentOffset - offset)
    }
    is MaybeGrammar -> {
        val nested = grammar.toRegex(groups, offset + 1)
        GrammarRegex("(${nested.regex})?", nested.groupsCount)
    }
    is ManyGrammar -> {
        val nested = grammar.toRegex(groups, offset + 1)
        GrammarRegex("(${nested.regex})*", nested.groupsCount)
    }
    is AnyOfGrammar -> GrammarRegex("[${Regex.escape(value)}]")
    is RangeGrammar -> GrammarRegex("[$from-$to]")
}

private fun MutableMap<String, MutableList<Int>>.add(key: String, value: Int) {
    if (!contains(key)) this[key] = mutableListOf()
    this[key]!! += value
}
