package io.ktor.tests.http

import io.ktor.http.*
import io.ktor.util.*
import kotlin.test.*

class CodecTest {
    private val swissAndGerman = "\u0047\u0072\u00fc\u0065\u007a\u0069\u005f\u007a\u00e4\u006d\u00e4"
    private val russian = "\u0412\u0441\u0435\u043c\u005f\u043f\u0440\u0438\u0432\u0435\u0442"

    @Test/*(timeout = 1000L)*/
    fun testDecodeRandom() {
        val chars = "+%0123abc"

        for (step in 0..1000) {
            val size = random(15) + 1
            val sb = CharArray(size)

            for (i in 0 until size) {
                sb[i] = chars[random(chars.length)]
            }

            try {
                String(sb).decodeURLQueryComponent()
            } catch (ignore: URLDecodeException) {
            } catch (t: Throwable) {
                fail("Failed at ${String(sb)}")
            }
        }
    }

    @Test
    fun testUTFEncodeDecode() {
        assertEquals("%D0%92%D1%81%D0%B5%D0%BC_%D0%BF%D1%80%D0%B8%D0%B2%D0%B5%D1%82", russian.encodeURLQueryComponent())
        assertEquals(
            "%D0%92%D1%81%D0%B5%D0%BC%5F%D0%BF%D1%80%D0%B8%D0%B2%D0%B5%D1%82",
            russian.encodeURLQueryComponent(encodeFull = true)
        )

        assertEquals("Gr%C3%BCezi_z%C3%A4m%C3%A4", swissAndGerman.encodeURLQueryComponent())
        assertEquals("Gr%C3%BCezi%5Fz%C3%A4m%C3%A4", swissAndGerman.encodeURLQueryComponent(encodeFull = true))

        for (msg in listOf(russian, swissAndGerman)) {
            encodeAndDecodeTest(msg)
        }
    }

    @Test
    fun testBasicEncodeDecode() {
        val test = "Test me!"
        assertEquals("Test%20me!", test.encodeURLQueryComponent())
        assertEquals("Test%20me%21", test.encodeURLParameter())
        encodeAndDecodeTest(test)
    }

    @Test
    fun testSimpleBasicEncodeDecode() {
        val s = "simple"
        val encoded = s.encodeURLQueryComponent()

        assertEquals("simple", encoded)
        encodeAndDecodeTest(s)
    }

    @Test
    fun testBasicEncodeDecodeURLPart() {
        val s = "Test me!"
        val encoded = s.encodeURLParameter()

        assertEquals("Test%20me%21", encoded)
        encodeAndDecodeTest(s)
    }

    @Test
    fun testAllReserved() {
        val text = "*~!@#$%^&()+{}\"\\;:`,/[]"
        assertEquals("*~!@#$%25%5E&()+%7B%7D%22%5C;:%60,/[]", text.encodeURLQueryComponent())
        assertEquals(
            "%2A%7E%21%40%23%24%25%5E%26%28%29%2B%7B%7D%22%5C%3B%3A%60%2C%2F%5B%5D",
            text.encodeURLQueryComponent(encodeFull = true)
        )

        encodeAndDecodeTest(text)
    }

    @Test
    fun testBrokenOrIncompleteHEX() {
        assertFails {
            "foo+%+bar".decodeURLQueryComponent()
        }
        assertEquals("0", "%30".decodeURLQueryComponent())
        assertFails {
            "%".decodeURLQueryComponent()
        }
        assertFails {
            "%0".decodeURLQueryComponent()
        }
    }

    private fun encodeAndDecodeTest(text: String) {
        assertEquals(text, text.encodeURLQueryComponent().decodeURLQueryComponent())
        assertEquals(text, text.encodeURLParameter().decodeURLPart())
    }
}
