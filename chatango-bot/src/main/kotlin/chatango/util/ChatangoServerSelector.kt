package chatango.util

import kotlin.math.max
import kotlin.math.min
import kotlin.random.Random

private val specials = mapOf("tango-hyoo" to 60, "monosekai" to 76, "nico-nico" to 29)

private val tsweights: List<Pair<Int, Int>> = listOf(
    listOf(5, 6, 7, 8, 16, 17, 18) to 75,
    listOf(9, 11, 12, 13, 14, 15) to 95,
    listOf(19, 23, 24, 25, 26) to 110,
    listOf(28, 29, 30, 31, 32, 33) to 104,
    (35..50).toList() to 101,
    listOf(52, 53, 55, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66) to 110,
    listOf(68) to 95,
    (71..84).toList() to 116
).flatMap { (ids, w) -> ids.map { it to w } }

fun resolveChatangoServer(groupIn: String): String {
    val sn: Int = specials[groupIn] ?: run {
        val group = groupIn.replace("_", "q").replace("-", "q")
        val fnv = substringClamped(group, 0, min(5, group.length)).toLong(36).toDouble()
        val lnvStr = substringClamped(group, 6, 6 + min(3, group.length - 5))
        val lnv = if (lnvStr.isNotEmpty()) max(lnvStr.toLong(36).toDouble(), 1000.0) else 1000.0
        val num = (fnv % lnv) / lnv
        val maxnum = tsweights.sumOf { it.second }.toDouble()
        var cumfreq = 0.0
        var result = 0
        for ((id, w) in tsweights) {
            cumfreq += w / maxnum
            if (num <= cumfreq) {
                result = id
                break
            }
        }
        result
    }
    return "s$sn.chatango.com"
}

fun getServer(groupIn: String): String = resolveChatangoServer(groupIn)

fun generateRoomSessionId(): String =
    Random.nextLong(1_000_000_000_000_000L, 10_000_000_000_000_000L).toString()

fun genUid(): String = generateRoomSessionId()
