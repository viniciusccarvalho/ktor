package io.ktor.http.parsing

internal sealed class Grammar
internal class StringGrammar(val value: String) : Grammar()
internal class NamedGrammar(val name: String, val grammar: Grammar) : Grammar()
internal class SequenceGrammar(val grammars: List<Grammar>) : Grammar()
internal class OrGrammar(val grammars: List<Grammar>) : Grammar()
internal class MaybeGrammar(val grammar: Grammar) : Grammar()
internal class ManyGrammar(val grammar: Grammar) : Grammar()
internal class AnyOfGrammar(val value: String) : Grammar()
internal class RangeGrammar(val from: Char, val to: Char) : Grammar()
internal class RawGrammar(val value: String) : Grammar()

internal fun maybe(grammar: Grammar): Grammar = MaybeGrammar(grammar)
internal fun maybe(block: GrammarBuilder.() -> Unit): () -> Grammar = { maybe(GrammarBuilder().apply(block).build()) }

internal infix fun String.then(grammar: Grammar): Grammar = StringGrammar(this) then grammar
internal infix fun Grammar.then(grammar: Grammar): Grammar = SequenceGrammar(listOf(this, grammar))
internal infix fun Grammar.then(value: String): Grammar = this then StringGrammar(value)

internal infix fun Grammar.or(grammar: Grammar): Grammar = OrGrammar(listOf(this, grammar))
internal infix fun Grammar.or(value: String): Grammar = this or StringGrammar(value)
internal infix fun String.or(grammar: Grammar): Grammar = StringGrammar(this) or grammar

internal fun many(grammar: Grammar): Grammar = ManyGrammar(grammar)

internal fun Grammar.named(name: String): Grammar = NamedGrammar(name, this)

internal fun anyOf(value: String): Grammar = AnyOfGrammar(value)
internal infix fun Char.to(other: Char): Grammar = RangeGrammar(this, other)
