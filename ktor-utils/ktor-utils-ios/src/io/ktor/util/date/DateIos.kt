package io.ktor.util.date

import kotlinx.cinterop.*
import platform.posix.*

actual fun GMTDate(timestamp: Long?): GMTDate = memScoped {
    val current: Long = timestamp ?: time(alloc<LongVar>().ptr)
    val dateInfo = alloc<tm>()
    localtime_r(current.toCPointer(), dateInfo.ptr)

    with(dateInfo) {
        // from (SUN, 0) to (SUN, 6)
        val weekDay = (tm_wday + 7 - 1) % 7

        GMTDate(
            tm_sec, tm_min, tm_hour,
            WeekDay.from(weekDay), tm_mday, tm_yday,
            Month.from(tm_mon), tm_year, current
        )
    }
}
