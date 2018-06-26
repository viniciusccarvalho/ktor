package io.ktor.server.testing

import io.ktor.util.*
import java.io.*
import java.security.*

internal fun InputStream.sha1WithSize(): Pair<String, Long> {
    val md = MessageDigest.getInstance("SHA1")
    val bytes = ByteArray(8192)
    var count = 0L

    do {
        val rc = read(bytes)
        if (rc == -1) {
            break
        }
        count += rc
        md.update(bytes, 0, rc)
    } while (true)

    return hex(md.digest()) to count
}

