package io.ktor.http.parsing

internal val lowAlpha = 'a' to 'z'
internal val alpha = ('a' to 'z') or ('A' to 'Z')
internal val digit = RawGrammar("\\d")
internal val hex = digit or ('A' to 'F') or ('a' to 'f')

internal val alphaDigit = alpha or digit
internal val alphas = atLeastOne(alpha)
internal val digits = atLeastOne(digit)
