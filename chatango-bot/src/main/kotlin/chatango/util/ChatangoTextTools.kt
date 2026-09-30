package chatango.util

import kotlin.math.max
import kotlin.math.min

private val reN = Regex("<n(.*?)/>")
private val reF = Regex("<f(.*?)>")
private val reNAll = Regex("<n.*?/>")
private val reFAll = Regex("<f.*?>")
private val reTag = Regex("</?[^>]*>")

fun substringClamped(s: String, a: Int, b: Int = s.length): String {
    val st = min(max(a, 0), s.length)
    val en = min(max(b, 0), s.length)
    return if (st >= en) "" else s.substring(st, en)
}

fun sl(s: String, a: Int, b: Int = s.length): String = substringClamped(s, a, b)

fun currentEpochSeconds(): Double = System.currentTimeMillis() / 1000.0

fun now(): Double = currentEpochSeconds()

fun stripHtmlTags(msg: String): String = reTag.replace(msg, "")

fun stripHtml(msg: String): String = stripHtmlTags(msg)

fun decodeIncomingMessage(msg0: String): Triple<String, String?, String?> {
    val n = reN.find(msg0)?.groupValues?.get(1)
    val f = reF.find(msg0)?.groupValues?.get(1)
    var msg = reNAll.replace(msg0, "")
    msg = reFAll.replace(msg, "")
    msg = stripHtmlTags(msg)
    msg = msg.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
    return Triple(msg, n, f)
}

fun cleanMessage(msg0: String): Triple<String, String?, String?> = decodeIncomingMessage(msg0)

fun parseNameColor(n: String): String = n

fun parseFont(f: String): Triple<String?, String?, Int?> {
    return try {
        val eq = f.indexOf('=')
        val sizecolor = f.substring(0, eq).trim()
        val size = substringClamped(sizecolor, 1, 3).toInt()
        var col: String? = substringClamped(sizecolor, 3, 6)
        if (col == "") col = null
        val face = f.split("\"", limit = 3)[1]
        Triple(col, face, size)
    } catch (e: Exception) {
        Triple(null, null, null)
    }
}

fun deriveAnonymousUserId(nIn: String?, ssid: String): String {
    val n = nIn ?: "5504"
    return try {
        val a = n.map { it.toString().toInt() }
        val b = ssid.drop(4).map { it.toString().toInt() }
        a.zip(b).map { (it.first + it.second).toString().last() }.joinToString("")
    } catch (e: NumberFormatException) {
        "NNNN"
    }
}

fun getAnonId(nIn: String?, ssid: String): String = deriveAnonymousUserId(nIn, ssid)

fun splitProtocolFrames(text: String): List<String> =
    text.split('\u0000').map { it.trimEnd('\r', '\n').replace("&#39;", "'") }

fun splitFrames(text: String): List<String> = splitProtocolFrames(text)
