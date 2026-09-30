package chatango.util

private fun bits(vararg p: Pair<String, Int>): Map<String, Long> = p.associate { it.first to (1L shl it.second) }

val MessageFlags: Map<String, Long> = bits(
    "PREMIUM" to 2, "BG_ON" to 3, "MEDIA_ON" to 4, "CENSORED" to 5, "SHOW_MOD_ICON" to 6,
    "SHOW_STAFF_ICON" to 7, "CHANNEL_RED" to 8, "CHANNEL_ORANGE" to 9, "CHANNEL_GREEN" to 10,
    "CHANNEL_CYAN" to 11, "CHANNEL_BLUE" to 12, "CHANNEL_PURPLE" to 13, "CHANNEL_PINK" to 14,
    "CHANNEL_MOD" to 15
)

val RoomFlags: Map<String, Long> = bits(
    "LIST_TAXONOMY" to 0, "NO_ANONS" to 2, "NO_FLAGGING" to 3, "NO_COUNTER" to 4, "NO_IMAGES" to 5,
    "NO_LINKS" to 6, "NO_VIDEOS" to 7, "NO_STYLED_TEXT" to 8, "NO_LINKS_CHATANGO" to 9,
    "NO_BROADCAST_MSG_WITH_BW" to 10, "RATE_LIMIT_REGIMEON" to 11, "CHANNELS_DISABLED" to 13,
    "NLP_SINGLEMSG" to 14, "NLP_MSGQUEUE" to 15, "BROADCAST_MODE" to 16, "CLOSED_IF_NO_MODS" to 17,
    "IS_CLOSED" to 18, "SHOW_MOD_ICONS" to 19, "MODS_CHOOSE_VISIBILITY" to 20, "NLP_NGRAM" to 21,
    "NO_PROXIES" to 22, "HAS_XML" to 28, "UNSAFE" to 29
)

val ModeratorFlags: Map<String, Long> = bits(
    "DELETED" to 0, "EDIT_MODS" to 1, "EDIT_MOD_VISIBILITY" to 2, "EDIT_BW" to 3,
    "EDIT_RESTRICTIONS" to 4, "EDIT_GROUP" to 5, "SEE_COUNTER" to 6, "SEE_MOD_CHANNEL" to 7,
    "SEE_MOD_ACTIONS" to 8, "EDIT_NLP" to 9, "EDIT_GP_ANNC" to 10, "EDIT_ADMINS" to 11,
    "EDIT_SUPERMODS" to 12, "NO_SENDING_LIMITATIONS" to 13, "SEE_IPS" to 14, "CLOSE_GROUP" to 15,
    "CAN_BROADCAST" to 16, "MOD_ICON_VISIBLE" to 17, "IS_STAFF" to 18, "STAFF_ICON_VISIBLE" to 19,
    "UNBAN_ALL" to 20
)

fun getFlag(n: Long, flagSet: Map<String, Long>): Map<String, Long> {
    if (n <= 0) return emptyMap()
    val named = LinkedHashMap(flagSet)
    val known = flagSet.values.toSet()
    var bit = n
    var c = 0
    while (bit != 0L) {
        bit = bit shr 1
        c++
        val tv = 1L shl c
        if (tv !in known) named[tv.toString()] = tv
    }
    val out = LinkedHashMap<String, Long>()
    for ((flag, number) in named) if ((n and number) != 0L) out[flag] = number
    return out
}
