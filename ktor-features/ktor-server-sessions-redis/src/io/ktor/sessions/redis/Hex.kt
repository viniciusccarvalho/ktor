package io.ktor.sessions.redis

internal object Hex {
    private val DIGITS = "0123456789ABCDEF"
    private val DIGITS_UPPER = DIGITS.toUpperCase()
    private val DIGITS_LOWER = DIGITS.toLowerCase()

    fun decodeChar(c: Char): Int = when (c) {
        in '0'..'9' -> c - '0'
        in 'a'..'f' -> c - 'a' + 10
        in 'A'..'F' -> c - 'A' + 10
        else -> -1
    }

    fun encodeCharLower(v: Int): Char = DIGITS_LOWER[v]
    fun encodeCharUpper(v: Int): Char = DIGITS_UPPER[v]

    fun isHexDigit(c: Char): Boolean = decodeChar(c) >= 0

    fun decode(str: String): ByteArray {
        val out = ByteArray((str.length + 1) / 2)
        var opos = 0
        var nibbles = 0
        var value = 0
        for (c in str) {
            val vv = decodeChar(c)
            if (vv >= 0) {
                value = (value shl 4) or vv
                nibbles++
            }
            if (nibbles == 2) {
                out[opos++] = value.toByte()
                nibbles = 0
                value = 0
            }
        }
        return if (opos != out.size) out.copyOf(opos) else out
    }

    fun encodeLower(src: ByteArray): String =
        encodeBase(src, DIGITS_LOWER)

    private fun encodeBase(data: ByteArray, digits: String = DIGITS): String {
        val out = StringBuilder(data.size * 2)
        for (n in data.indices) {
            val v = data[n].toInt() and 0xFF
            out.append(digits[(v ushr 4) and 0xF])
            out.append(digits[(v ushr 0) and 0xF])
        }
        return out.toString()
    }
}

internal val String.unhex get() = Hex.decode(this)
internal val ByteArray.hex get() = Hex.encodeLower(this)
