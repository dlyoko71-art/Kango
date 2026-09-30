package chatango.model

import chatango.connection.Room

class ChatangoUser private constructor(val raw: String) {
    val name: String = raw.lowercase()
    var puid: String = ""
    internal val sids = HashMap<Room, MutableSet<String>>()
    val msgs = ArrayList<Message>()
    var nameColor: String = "000"
    var fontSize: Int = 12
    var fontFace: String = "0"
    var fontColor: String = "000"
    var mbg: Boolean = false
    var mrec: Boolean = false

    fun getSessionIds(room: Room? = null): Set<String> =
        if (room != null) sids[room] ?: emptySet() else sids.values.flatten().toSet()

    val rooms: Set<Room> get() = sids.keys
    val roomnames: List<String> get() = sids.keys.map { it.name }

    fun addSessionId(room: Room, sid: String) {
        sids.getOrPut(room) { HashSet() }.add(sid)
    }

    fun removeSessionId(room: Room, sid: String) {
        val set = sids[room] ?: return
        set.remove(sid)
        if (set.isEmpty()) sids.remove(room)
    }

    fun clearSessionIds(room: Room) {
        sids.remove(room)
    }

    fun hasSessionId(room: Room, sid: String): Boolean = sids[room]?.contains(sid) ?: false

    fun updatePuid(puid: String) {
        this.puid = puid
    }

    override fun toString() = "<User: $name>"

    companion object {
        private val users = HashMap<String, User>()

        fun get(name: String?): User = synchronized(users) {
            val n = name ?: ""
            users.getOrPut(n.lowercase()) { User(n) }
        }
    }
}

typealias User = ChatangoUser
