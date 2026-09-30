package chatango.model

import chatango.connection.Room

class ChatMessage(
    msgid: String? = null,
    time: Double = 0.0,
    channel: String? = null,
    val user: User,
    body: String? = null,
    room: Room? = null,
    raw: String? = null,
    ip: String? = null,
    unid: String? = null,
    nameColor: String? = null,
    fontSize: Int? = null,
    fontFace: String? = null,
    fontColor: String? = null,
    uid: String? = null
) {
    var msgid: String? = msgid
        private set
    val time: Double = time
    val channel: String = channel ?: "0"
    val body: String = body ?: ""
    var room: Room? = room
        private set
    val raw: String = raw ?: ""
    val ip: String? = ip
    val unid: String = unid ?: ""
    val nameColor: String = nameColor ?: "000"
    val fontSize: Int = fontSize ?: 12
    val fontFace: String = fontFace ?: "0"
    val fontColor: String = fontColor ?: "000"
    val uid: String = uid ?: ""

    fun attach(room: Room, msgid: String) {
        if (this.msgid == null) {
            this.room = room
            this.msgid = msgid
            room.msgs[msgid] = this
        }
    }

    fun detach() {
        val id = msgid
        val r = room
        if (id != null && r != null && r.msgs.containsKey(id)) {
            r.msgs.remove(id)
            msgid = null
        }
    }
}

typealias Message = ChatMessage
