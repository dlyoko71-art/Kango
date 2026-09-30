package chatango.connection

import chatango.manager.SessionManager
import chatango.model.BanRecord
import chatango.model.Conn
import chatango.model.Message
import chatango.model.Participant
import chatango.model.User
import chatango.util.MessageFlags
import chatango.util.BigMessage
import chatango.util.ModeratorFlags
import chatango.util.RoomFlags
import chatango.util.decodeIncomingMessage
import chatango.util.generateRoomSessionId
import chatango.util.deriveAnonymousUserId
import chatango.util.getFlag
import chatango.util.resolveChatangoServer
import chatango.util.currentEpochSeconds
import chatango.util.parseFont
import chatango.util.parseNameColor
import chatango.util.substringClamped
import chatango.util.splitProtocolFrames
import okhttp3.Request
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import okio.ByteString
import java.net.URLDecoder
import java.util.Locale

class ChatRoom(
    val name: String,
    uid: String? = null,
    server: String? = null,
    port: Int? = null,
    val mgr: SessionManager
) : Conn {
    private val server: String = server ?: resolveChatangoServer(name)
    private val port: Int = port ?: 8081

    var uid: String = uid ?: generateRoomSessionId()
        private set
    private var aid: String = ""
    private var channel = "0"
    var owner: User? = null
        private set
    private var mods: MutableMap<User, Map<String, Long>> = LinkedHashMap()
    private val mqueue = HashMap<String, Message>()
    internal val history = ArrayList<Message>()
    private val status = LinkedHashMap<String, Participant>()
    private var connectAmount = 0
    var premium = false
        private set
    private var userCountRaw = 0
    internal val msgs = HashMap<String, Message>()
    var silent = false
    private var banlistMap = LinkedHashMap<User, BanRecord>()
    private var unbanlistMap = LinkedHashMap<User, BanRecord>()
    private val bannedwordsList = ArrayList<String>()
    var flags: Map<String, Long> = emptyMap()
        private set
    private var iLog = ArrayList<Message>()

    private var ws: WebSocket? = null
    private var wlock = true
    private val wlockbuf = StringBuilder()
    private var firstCommand = true
    var connected = false
        private set
    private var reconnecting = false
    private var pingTask: SessionManager.Task? = null
    private var attempt = 0

    @Volatile
    var floodPauseUntil = 0L
    val isFloodPaused: Boolean get() = System.currentTimeMillis() < floodPauseUntil

    init {
        mgr.roomsMap[name] = this
        _connect()
    }

    private fun fire(evt: String, vararg args: Any?, call: SessionManager.(ChatRoom) -> Unit) {
        mgr.call(this)
        mgr.onEventCalled(this, evt, args)
    }

    val userlist: List<User> get() = status.values.map { it.user }.distinct()
    val realUserlist: List<User> get() = userlist.filter { it.name[0] != '!' && it.name[0] != '#' }
    val usernames: List<String> get() = userlist.map { it.name }
    val participants: List<Participant> get() = status.values.toList()
    val ownername: String? get() = owner?.name
    val modSet: Set<User> get() = mods.keys.toSet()
    val modnames: List<String> get() = mods.keys.map { it.name }
    val usercount: Int get() = if (userCountRaw == 0) userlist.size else userCountRaw
    val banlist: List<User> get() = banlistMap.keys.toList()
    val unbanlist: List<Pair<User, User>> get() = unbanlistMap.values.map { it.target to it.src }
    val bannedwords: List<String> get() = bannedwordsList

    fun getMessage(mid: String): Message? = msgs[mid]

    fun connect() = _connect()

    private fun _connect() {
        firstCommand = true
        wlock = true
        wlockbuf.setLength(0)
        val req = Request.Builder()
            .url("wss://$server:$port/")
            .header("Origin", "https://st.chatango.com")
            .header("Pragma", "no-cache")
            .header("Cache-Control", "no-cache")
            .build()
        ws = mgr.wsClient.newWebSocket(req, object : WebSocketListener() {
            override fun onOpen(webSocket: WebSocket, response: Response) {
                mgr.post { if (ws === webSocket) onSocketOpen() }
            }

            override fun onMessage(webSocket: WebSocket, text: String) {
                mgr.post { if (ws === webSocket) feed(text) }
            }

            override fun onMessage(webSocket: WebSocket, bytes: ByteString) {
                onMessage(webSocket, bytes.utf8())
            }

            override fun onClosing(webSocket: WebSocket, code: Int, reason: String) {
                webSocket.close(code, null)
            }

            override fun onClosed(webSocket: WebSocket, code: Int, reason: String) {
                mgr.post { if (ws === webSocket) handleLoss() }
            }

            override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) {
                mgr.post {
                    if (ws === webSocket) {
                        println("$name: ${t.message}")
                        handleLoss()
                    }
                }
            }
        })
    }

    private fun onSocketOpen() {
        _auth()
        pingTask?.cancel()
        pingTask = mgr.setInterval(mgr.pingDelay) { ping() }
    }

    private fun handleLoss() {
        if (mgr.roomsMap[name] !== this) return
        connected = false
        if (attempt == 0) {
            attempt = 3
            tryReconnect()
            return
        }
        attempt -= 1
        if (attempt == 0) {
            println("failed to reconnect $name")
            disconnect()
            return
        }
        mgr.setTimeout(10.0) { tryReconnect() }
    }

    private fun tryReconnect() {
        if (mgr.roomsMap[name] !== this) return
        println("try reconnect $name attempt $attempt")
        try {
            _reconnect()
        } catch (e: Exception) {
            println(e)
            handleLoss()
        }
    }

    fun reconnect() {
        attempt = 3
        tryReconnect()
    }

    private fun _reconnect() {
        reconnecting = true
        try {
            _disconnect()
            _connect()
        } finally {
            reconnecting = false
        }
    }

    fun disconnect() {
        _disconnect()
        fire("onDisconnect") { onDisconnect(it) }
    }

    private fun _disconnect() {
        connected = false
        pingTask?.cancel()
        pingTask = null
        val w = ws
        ws = null
        try {
            w?.close(1000, null)
        } catch (_: Exception) {
        }
        if (!reconnecting) mgr.roomsMap.remove(name)
    }

    private fun _auth() {
        val n = mgr.name
        val p = mgr.password
        val cmd = if (!n.isNullOrEmpty() && !p.isNullOrEmpty()) command("bauth", name, uid, n, p)
        else command("bauth", name, uid)
        ws?.send(cmd)
        wlock = true
    }

    fun login(name: String, password: String? = null) {
        if (password != null) sendCommand("blogin", name, password) else sendCommand("blogin", name)
    }

    fun logout() = sendCommand("blogout")

    private fun command(vararg args: String): String {
        val terminator = if (firstCommand) {
            firstCommand = false
            "\u0000"
        } else "\r\n\u0000"
        return args.joinToString(":") + terminator
    }

    private fun sendCommand(vararg args: String) = write(command(*args))

    private fun write(data: String) {
        if (wlock) wlockbuf.append(data) else ws?.send(data)
    }

    private fun setWriteLock(lock: Boolean) {
        wlock = lock
        if (!lock && wlockbuf.isNotEmpty()) {
            val b = wlockbuf.toString()
            wlockbuf.setLength(0)
            ws?.send(b)
        }
    }

    private fun feed(text: String) {
        for (food in splitProtocolFrames(text)) {
            if (food.isEmpty()) continue
            try {
                process(food)
            } catch (e: Exception) {
                println("$name error: $e")
                if (SessionManager.DEBUG) e.printStackTrace()
            }
        }
    }

    private fun process(raw: String) {
        fire("onRaw", raw) { onRaw(it, raw) }
        val data = raw.split(":")
        val cmd = data[0]
        val args = data.drop(1)
        when (cmd) {
            "ok" -> rcmdOk(args)
            "groupflagsupdate" -> rcmdGroupFlagsUpdate(args)
            "denied" -> {
                _disconnect()
                fire("onConnectFail") { onConnectFail(it) }
            }
            "inited" -> rcmdInited()
            "getratelimit" -> {}
            "bw" -> rcmdBw(args)
            "premium" -> rcmdPremium(args)
            "mods" -> rcmdMods(args)
            "b" -> rcmdB(args)
            "u" -> rcmdU(args)
            "i" -> rcmdI(args)
            "g_participants" -> rcmdGParticipants(args)
            "participant" -> rcmdParticipant(args)
            "show_fw" -> {
                floodPauseUntil = System.currentTimeMillis() + mgr.floodPauseMs
                println("🚫 FLOOD WARNING! Bot paused for ${mgr.floodPauseMs / 1000} seconds.")
                fire("onFloodWarning") { onFloodWarning(it) }
            }
            "show_tb" -> fire("onFloodBan") { onFloodBan(it) }
            "tb" -> fire("onFloodBanRepeat") { onFloodBanRepeat(it) }
            "delete" -> rcmdDelete(args)
            "deleteall" -> args.forEach { rcmdDelete(listOf(it)) }
            "n" -> {
                userCountRaw = args[0].toInt(16)
                fire("onUserCountChange") { onUserCountChange(it) }
            }
            "blocklist" -> {
                banlistMap = parseBanSections(args)
                fire("onBanlistUpdate") { onBanlistUpdate(it) }
            }
            "unblocklist" -> {
                unbanlistMap = parseBanSections(args)
                fire("onUnBanlistUpdate") { onUnBanlistUpdate(it) }
            }
            "blocked" -> rcmdBlocked(args)
            "unblocked" -> rcmdUnblocked(args)
        }
    }

    private fun parseMods(s: String): MutableMap<User, Map<String, Long>> {
        val out = LinkedHashMap<User, Map<String, Long>>()
        for (x in s.split(";")) {
            val p = x.split(",")
            out[User.get(p[0])] = getFlag(p[1].toLong(), ModeratorFlags)
        }
        return out
    }

    private fun rcmdOk(args: List<String>) {
        connected = true
        attempt = 0
        if (args[2] == "C" && mgr.password == null && mgr.name == null) {
            val n = args[4].substringBeforeLast('.').takeLast(4)
            mgr.user.nameColor = n
        } else if (args[2] == "C" && mgr.password == null) {
            login(mgr.name ?: "")
        } else if (args[2] != "M") {
            fire("onLoginFail") { onLoginFail(it) }
            disconnect()
            return
        }
        owner = User.get(args[0])
        uid = args[1]
        aid = substringClamped(args[1], 4, 8)
        if (args[6].isNotEmpty()) mods = parseMods(args[6])
        flags = getFlag(args[7].toLong(), RoomFlags)
        iLog = ArrayList()
    }

    private fun rcmdGroupFlagsUpdate(args: List<String>) {
        val old = flags.entries.map { it.key to it.value }.toSet()
        flags = getFlag(args[0].toLong(), RoomFlags)
        val new = flags.entries.map { it.key to it.value }.toSet()
        val added = (new - old).toMap()
        if (added.isNotEmpty()) fire("onGroupFlagsAdded", added) { onGroupFlagsAdded(it, added) }
        val removed = (old - new).toMap()
        if (removed.isNotEmpty()) fire("onGroupFlagsRemoved", removed) { onGroupFlagsRemoved(it, removed) }
        fire("onGroupFlagsUpdate") { onGroupFlagsUpdate(it) }
    }

    private fun rcmdInited() {
        sendCommand("g_participants", "start")
        sendCommand("getpremium", "1")
        sendCommand("getbannedwords")
        sendCommand("getratelimit")
        requestBanlist()
        requestUnBanlist()
        if (connectAmount == 0) {
            fire("onConnect") { onConnect(it) }
            for (msg in iLog.reversed()) {
                val user = msg.user
                fire("onHistoryMessage", user, msg) { onHistoryMessage(it, user, msg) }
                addHistory(msg)
            }
            iLog.clear()
        } else {
            fire("onReconnect") { onReconnect(it) }
        }
        connectAmount += 1
        setWriteLock(false)
    }

    private fun rcmdBw(args: List<String>) {
        for (word in args) {
            val decoded = URLDecoder.decode(word.replace("+", "%2B"), "UTF-8")
            for (w in decoded.split(",")) {
                if (w !in bannedwordsList && w.isNotEmpty()) bannedwordsList.add(w)
            }
        }
        val list = bannedwordsList.toList()
        fire("onBannedWordsUpdated", list) { onBannedWordsUpdated(it, list) }
    }

    private fun rcmdPremium(args: List<String>) {
        if (args[1].toDouble() > currentEpochSeconds()) {
            premium = true
            if (mgr.user.mbg) setBgMode(1)
            if (mgr.user.mrec) setRecordingMode(1)
        } else premium = false
    }

    private fun rcmdMods(args: List<String>) {
        val newMods = LinkedHashMap<User, Map<String, Long>>()
        for (x in args) {
            val p = x.split(",")
            newMods[User.get(p[0])] = getFlag(p[1].toLong(), ModeratorFlags)
        }
        val cur = newMods.keys
        val pre = mods.keys.toSet()
        for (user in cur - pre) fire("onModAdd", user) { onModAdd(it, user) }
        for (user in pre - cur) fire("onModRemove", user) { onModRemove(it, user) }
        fire("onModChange") { onModChange(it) }
        mods = newMods
    }

    private fun rcmdB(args: List<String>) {
        val mtime = args[0].toDouble()
        val channelId = args[7]
        val puid = args[3]
        val ip = args[6]
        var name = args[1]
        val rawmsg = args.drop(8).joinToString(":")
        val (msg, n, f) = decodeIncomingMessage(rawmsg)
        var nameColor: String? = null
        if (name == "") {
            name = "#" + args[2]
            if (name == "#") name = "!anon" + deriveAnonymousUserId(n, puid)
        } else if (!n.isNullOrEmpty()) {
            nameColor = parseNameColor(n)
        }
        val i = args[5]
        val unid = args[4]
        val user = User.get(name)
        if (puid.isNotEmpty()) user.updatePuid(puid)
        var fontColor: String? = null
        var fontFace: String? = null
        var fontSize: Int? = null
        if (!f.isNullOrEmpty()) {
            val t = parseFont(f)
            fontColor = t.first
            fontFace = t.second
            fontSize = t.third
        }
        mqueue[i] = Message(
            time = mtime, channel = channelId, user = user,
            body = msg.drop(1), raw = rawmsg.drop(1), uid = puid, ip = ip,
            nameColor = nameColor, fontColor = fontColor, fontFace = fontFace, fontSize = fontSize,
            unid = unid, room = this
        )
    }

    private fun rcmdU(args: List<String>) {
        val msg = mqueue[args[0]] ?: return
        if (msg.user != mgr.user) {
            msg.user.fontColor = msg.fontColor
            msg.user.fontFace = msg.fontFace
            msg.user.fontSize = msg.fontSize
            msg.user.nameColor = msg.nameColor
        }
        mqueue.remove(args[0])
        msg.attach(this, args[1])
        addHistory(msg)
        channel = msg.channel
        fire("onMessage", msg.user, msg) { onMessage(it, msg.user, msg) }
    }

    private fun rcmdI(args: List<String>) {
        val mtime = args[0].toDouble()
        val channelId = args[7]
        val puid = args[3]
        var name = args[1]
        val rawmsg = args.drop(8).joinToString(":")
        val (msg, n, f) = decodeIncomingMessage(rawmsg)
        val msgid = args[5]
        var nameColor: String? = null
        if (name == "") {
            name = "#" + args[2]
            if (name == "#") name = "!anon" + deriveAnonymousUserId(n, puid)
        } else if (!n.isNullOrEmpty()) {
            nameColor = parseNameColor(n)
        }
        var fontColor: String? = null
        var fontFace: String? = null
        var fontSize: Int? = null
        if (!f.isNullOrEmpty()) {
            val t = parseFont(f)
            fontColor = t.first
            fontFace = t.second
            fontSize = t.third
        }
        val user = User.get(name)
        if (puid.isNotEmpty()) user.updatePuid(puid)
        val m = msgs.getOrPut(msgid) {
            Message(
                msgid = msgid, time = mtime, channel = channelId, user = user,
                body = msg, raw = rawmsg, ip = args[6], unid = args[4],
                nameColor = nameColor, fontColor = fontColor, fontFace = fontFace, fontSize = fontSize,
                room = this
            )
        }
        iLog.add(m)
    }

    private fun anonName(usertime: Double, puid: String, tmp: String): String {
        val n = usertime.toLong().toString().takeLast(4)
        return if (tmp.lowercase() == "none") "!anon" + deriveAnonymousUserId(n, puid) else "#$tmp"
    }

    private fun rcmdGParticipants(args: List<String>) {
        for (entry in args.joinToString(":").split(";")) {
            val data = entry.split(":")
            if (data.size < 5) continue
            val sid = data[0]
            val usertime = data[1].toDouble()
            var name = data[3]
            val puid = data[2]
            if (name.lowercase() == "none") name = anonName(usertime, puid, data[4])
            val user = User.get(name)
            if (puid.isNotEmpty()) user.updatePuid(puid)
            user.addSessionId(this, sid)
            if (sid !in status) status[sid] = Participant(user, usertime, data[2], data[3], data[4])
        }
        val snap = status.values.toList()
        fire("onSnapshot", snap) { onSnapshot(it, snap) }
    }

    private fun rcmdParticipant(args: List<String>) {
        var name = args[3]
        val sid = args[1]
        val usertime = args[6].toDouble()
        val puid = args[2]
        if (name.lowercase() == "none") name = anonName(usertime, puid, args[4])
        val user = User.get(name)
        if (puid.isNotEmpty()) user.updatePuid(puid)
        if (args[0] == "0") {
            user.removeSessionId(this, sid)
            if (status.remove(sid) != null) fire("onLeave", user, puid) { onLeave(it, user, puid) }
        }
        if (args[0] == "1" || args[0] == "2") {
            user.addSessionId(this, sid)
            status[sid] = Participant(user, usertime, args[2], args[3], args[4])
            fire("onJoin", user, puid) { onJoin(it, user, puid) }
        }
    }

    private fun rcmdDelete(args: List<String>) {
        val msg = getMessage(args[0]) ?: return
        if (history.remove(msg)) {
            fire("onMessageDelete", msg.user, msg) { onMessageDelete(it, msg.user, msg) }
            msg.detach()
        }
    }

    private fun parseBanSections(args: List<String>): LinkedHashMap<User, BanRecord> {
        val out = LinkedHashMap<User, BanRecord>()
        for (section in args.joinToString(":").split(";")) {
            val p = section.split(":")
            if (p.size != 5) continue
            if (p[2] == "") continue
            val user = User.get(p[2])
            out[user] = BanRecord(p[0], p[1], user, p[3].toDouble(), User.get(p[4]))
        }
        return out
    }

    private fun rcmdBlocked(args: List<String>) {
        try {
            val msg = getLastMessageByIp(args[1])
            val target = if (args[2] == "") (msg ?: return).user else User.get(args[2])
            val user = User.get(args[3])
            banlistMap[target] = BanRecord(args[0], args[1], target, args[4].toDouble(), user)
            fire("onBan", user, target) { onBan(it, user, target) }
            requestBanlist()
        } catch (e: Exception) {
            println(e)
        }
    }

    private fun rcmdUnblocked(args: List<String>) {
        if (args[2] == "") return
        val target = User.get(args[2])
        val user = User.get(args[3])
        if (banlistMap.remove(target) == null) return
        unbanlistMap[user] = BanRecord(args[0], args[1], target, args[4].toDouble(), user)
        fire("onUnban", user, target) { onUnban(it, user, target) }
        requestUnBanlist()
    }

    fun ping() {
        sendCommand("")
        fire("onPing") { onPing(it) }
    }

    fun rawMessage(channel: String, msg0: String) {
        if (silent) return
        if (isFloodPaused) {
            println("⏳ Flood cooldown active. Message blocked.")
            return
        }
        val u = mgr.user
        val msg = String.format(Locale.ROOT, "<f x%02d%s=\"%s\">", u.fontSize, u.fontColor, u.fontFace) +
                "<n${u.nameColor}/>" + msg0
        sendCommand("bm", "tl2r", channel, msg)
    }

    fun message(msg: String?, html: Boolean = false, channel: String? = null) {
        if (msg == null) return
        var m: String = msg
        if (!html) m = m.replace("<", "&lt;").replace(">", "&gt;")
        if (m.length > mgr.maxLength) {
            if (mgr.tooBigMessage == BigMessage.Cut) {
                message(m.take(mgr.maxLength), html, channel)
            } else if (mgr.tooBigMessage == BigMessage.Multiple) {
                while (m.isNotEmpty()) {
                    val sect = m.take(mgr.maxLength)
                    m = m.drop(mgr.maxLength)
                    message(sect, html, channel)
                }
            }
            return
        }
        rawMessage(channel ?: this.channel, m)
    }

    fun setBgMode(mode: Int) = sendCommand("msgbg", mode.toString())
    fun setRecordingMode(mode: Int) = sendCommand("msgmedia", mode.toString())

    fun addBadWord(word: String) {
        if (getLevel(mgr.user) == 2) {
            bannedwordsList.add(word)
            sendCommand("setbannedwords", "403", bannedwordsList.joinToString(", "))
        }
    }

    fun removeBadWord(word: String) {
        if (getLevel(mgr.user) == 2) {
            bannedwordsList.remove(word)
            sendCommand("setbannedwords", "403", bannedwordsList.joinToString(", "))
        }
    }

    fun addMod(user: User) {
        if (getLevel(mgr.user) == 2) sendCommand("addmod", user.name)
    }

    fun removeMod(user: User) {
        if (getLevel(mgr.user) == 2) sendCommand("removemod", user.name)
    }

    fun flag(message: Message) {
        message.msgid?.let { sendCommand("g_flag", it) }
    }

    fun flagUser(user: User): Boolean {
        val msg = getLastMessage(user) ?: return false
        flag(msg)
        return true
    }

    fun delete(message: Message) {
        if (getLevel(mgr.user) > 0) message.msgid?.let { sendCommand("delmsg", it) }
    }

    fun rawClearUser(unid: String, ip: String, user: String) = sendCommand("delallmsg", unid, ip, user)

    fun clearUser(user: User): Boolean {
        if (getLevel(mgr.user) > 0) {
            val msg = getLastMessage(user)
            if (msg != null) {
                val username = if (user.name[0] == '!' || user.name[0] == '#') "" else user.name
                rawClearUser(msg.unid, msg.ip ?: "", username)
            }
            return true
        }
        return false
    }

    fun clearall() {
        sendCommand("clearall")
        if (getLevel(mgr.user) == 1) {
            for (msg in history.toList()) clearUser(msg.user)
        }
        fire("onClearAll") { onClearAll(it) }
    }

    fun rawBan(name: String, ip: String, unid: String) = sendCommand("block", unid, ip, name)

    fun ban(msg: Message) {
        if (getLevel(mgr.user) > 0) {
            val username = if (msg.user.name[0] == '!' || msg.user.name[0] == '#') "" else msg.user.name
            rawBan(username, msg.ip ?: "", msg.unid)
        }
    }

    fun banUser(user: User): Boolean {
        val msg = getLastMessage(user) ?: return false
        ban(msg)
        return true
    }

    fun requestBanlist() = sendCommand("blocklist", "block", "", "next", "500")
    fun requestUnBanlist() = sendCommand("blocklist", "unblock", "", "next", "500")

    fun rawUnban(name: String, ip: String, unid: String) = sendCommand("removeblock", unid, ip, name)

    fun unban(user: User): Boolean {
        val rec = banlistMap[user] ?: return false
        rawUnban(rec.target.name, rec.ip, rec.unid)
        return true
    }

    fun getLevel(user: User): Int {
        if (user == owner) return 2
        if (user in mods) return 1
        return 0
    }

    fun getBadge(msg: Message): Int {
        val channelNum = msg.channel.toLongOrNull() ?: return 0
        val badge = getFlag(channelNum, MessageFlags)
        return if ("SHOW_MOD_ICON" in badge || "SHOW_STAFF_ICON" in badge) 1 else 0
    }

    fun getLastMessage(user: User? = null): Message? {
        if (user == null) return history.lastOrNull()
        return history.asReversed().firstOrNull { it.user == user }
    }

    fun getLastMessageByIp(ip: String? = null): Message? {
        if (ip == null) return history.lastOrNull()
        return history.asReversed().firstOrNull { it.ip == ip }
    }

    fun findUser(nameIn: String): User? {
        val q = nameIn.lowercase()
        val udi = userlist.associateBy { it.name }
        var cname: String? = null
        for (n in udi.keys) {
            if (n.contains(q)) {
                if (cname != null) return null
                cname = n
            }
        }
        return cname?.let { udi[it] }
    }

    private fun addHistory(msg: Message) {
        history.add(msg)
        if (getBadge(msg) > 0) {
            if (msg.user !in mods && msg.user != owner) mods[msg.user] = emptyMap()
        }
        if (history.size > mgr.maxHistoryLength) {
            val cut = history.size - mgr.maxHistoryLength
            val rest = history.take(cut)
            history.subList(0, cut).clear()
            rest.forEach { it.detach() }
        }
    }
}

typealias ChatangoRoom = ChatRoom
typealias Room = ChatRoom
