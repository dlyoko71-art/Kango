package chatango.connection

import chatango.manager.SessionManager
import chatango.model.Conn
import chatango.model.PMStatus
import chatango.model.User
import chatango.util.currentEpochSeconds
import chatango.util.splitProtocolFrames
import chatango.util.stripHtmlTags
import okhttp3.FormBody
import okhttp3.Request
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import okio.ByteString
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class PrivateMessageSession(val mgr: SessionManager) : Conn {
    private val authRe = Regex("auth\\.chatango\\.com ?= ?([^;]*)", RegexOption.IGNORE_CASE)

    var connected = false
        private set
    private var reconnecting = false
    private var closedByUser = false
    private var auid: String? = null
    var premium = false
        private set
    var blocklist: MutableSet<User> = LinkedHashSet()
        private set
    val contacts: MutableSet<User> = LinkedHashSet()
    private val status = ConcurrentHashMap<User, PMStatus>()
    private val trackWaiters = ConcurrentHashMap<User, CountDownLatch>()

    private var ws: WebSocket? = null
    private var wlock = true
    private val wlockbuf = StringBuilder()
    private var firstCommand = true
    private var pingTask: SessionManager.Task? = null
    private var attempt = 0

    private fun fire(evt: String, vararg args: Any?, call: SessionManager.(PrivateMessageSession) -> Unit) {
        mgr.call(this)
        mgr.onEventCalled(this, evt, args)
    }

    private fun getAuth(name: String, password: String): String? {
        return try {
            val client = mgr.http.newBuilder().followRedirects(false).followSslRedirects(false).build()
            val body = FormBody.Builder()
                .add("user_id", name)
                .add("password", password)
                .add("storecookie", "on")
                .add("checkerrors", "yes")
                .build()
            val req = Request.Builder().url("https://chatango.com/login").post(body).build()
            client.newCall(req).execute().use { resp: Response ->
                for (v in resp.headers("Set-Cookie")) {
                    val m = authRe.find(v)
                    if (m != null) {
                        val auth = m.groupValues[1]
                        return@use if (auth == "") null else auth
                    }
                }
                null
            }
        } catch (e: Exception) {
            null
        }
    }

    fun connect() {
        closedByUser = false
        _connect()
    }

    private fun _connect() {
        val n = mgr.name
        val p = mgr.password
        val auth = if (n.isNullOrEmpty() || p.isNullOrEmpty()) null else getAuth(n, p)
        if (auth == null) {
            fire("onLoginFail") { onLoginFail(it) }
            return
        }
        auid = auth
        firstCommand = true
        wlock = true
        wlockbuf.setLength(0)

        val req = Request.Builder()
            .url("ws://${mgr.pmHost}:${mgr.pmPort}/")
            .header("Origin", "https://st.chatango.com")
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
                        println("PM: ${t.message}")
                        handleLoss()
                    }
                }
            }
        })
    }

    private fun onSocketOpen() {
        ws?.send(command("tlogin", auid ?: "", "2"))
        wlock = true
        pingTask?.cancel()
        pingTask = mgr.setInterval(mgr.pingDelay) { ping() }
    }

    private fun handleLoss() {
        if (closedByUser) return
        connected = false
        if (attempt == 0) {
            attempt = 3
            tryReconnect()
            return
        }
        attempt -= 1
        if (attempt == 0) {
            println("failed to reconnect PM")
            disconnect()
            return
        }
        mgr.setTimeout(10.0) { tryReconnect() }
    }

    private fun tryReconnect() {
        if (closedByUser) return
        println("try reconnect PM attempt $attempt")
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
        fire("onPMReconnect") { onPMReconnect(it) }
    }

    fun disconnect() {
        closedByUser = true
        _disconnect()
        fire("onPMDisconnect") { onPMDisconnect(it) }
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
    }

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
                println("PM error: $e")
            }
        }
    }

    private fun process(raw: String) {
        fire("onRaw", raw) { onRaw(it, raw) }
        val data = raw.split(":")
        val cmd = data[0]
        val args = data.drop(1)
        when (cmd) {
            "OK" -> rcmdOK()
            "wl" -> rcmdWl(args)
            "wladd" -> rcmdWladd(args)
            "wldelete" -> rcmdWldelete(args)
            "block_list" -> rcmdBlockList(args)
            "idleupdate" -> rcmdIdleUpdate(args)
            "track", "status", "connect" -> updateTrack(args)
            "DENIED" -> {
                _disconnect()
                fire("onLoginFail") { onLoginFail(it) }
            }
            "msg" -> {
                val user = User.get(args[0])
                val body = stripHtmlTags(args.drop(5).joinToString(":"))
                fire("onPMMessage", user, body) { onPMMessage(it, user, body) }
            }
            "msgoff" -> {
                val user = User.get(args[0])
                val body = stripHtmlTags(args.drop(5).joinToString(":"))
                fire("onPMOfflineMessage", user, body) { onPMOfflineMessage(it, user, body) }
            }
            "wlonline" -> {
                val user = User.get(args[0])
                val lastOn = args[1].toDouble()
                status[user] = PMStatus(lastOn, "online", lastOn)
                fire("onPMContactOnline", user) { onPMContactOnline(it, user) }
            }
            "wlapp" -> {
                val user = User.get(args[0])
                val lastOn = args[1].toDouble()
                status[user] = PMStatus(lastOn, "app", lastOn)
                fire("onPMContactApp", user) { onPMContactApp(it, user) }
            }
            "wloffline" -> {
                val user = User.get(args[0])
                val lastOn = args[1].toDouble()
                status[user] = PMStatus(lastOn, "offline", 0.0)
                fire("onPMContactOffline", user) { onPMContactOffline(it, user) }
            }
            "premium" -> {
                if (args[1].toDouble() > currentEpochSeconds()) {
                    premium = true
                    setBgMode(1)
                    setRecordingMode(1)
                } else premium = false
            }
            "kickingoff", "toofast" -> disconnect()
            "unblocked" -> {
                val user = User.get(args[0])
                if (blocklist.remove(user)) fire("onPMUnblock", user) { onPMUnblock(it, user) }
            }
        }
    }

    private fun rcmdOK() {
        connected = true
        attempt = 0
        setWriteLock(false)
        sendCommand("wl")
        sendCommand("getblock")
        sendCommand("getpremium", "1")
        fire("onPMConnect") { onPMConnect(it) }
    }

    private fun rcmdWl(args: List<String>) {
        contacts.clear()
        for (i in 0 until args.size / 4) {
            val name = args[i * 4]
            val lastOn = args[i * 4 + 1]
            val isOn = args[i * 4 + 2]
            val idle = args[i * 4 + 3]
            val user = User.get(name)
            when {
                lastOn == "None" -> {}
                isOn == "off" -> status[user] = PMStatus(lastOn.toDouble(), "offline", 0.0)
                isOn == "app" -> status[user] = PMStatus(lastOn.toDouble(), "app", 0.0)
                idle == "0" && isOn == "on" -> status[user] = PMStatus(lastOn.toDouble(), "online", 0.0)
                else -> status[user] = PMStatus(lastOn.toDouble(), "online", currentEpochSeconds() - idle.toDouble() * 60)
            }
            contacts.add(user)
        }
        fire("onPMContactlistReceive") { onPMContactlistReceive(it) }
    }

    private fun rcmdWladd(args: List<String>) {
        val user = User.get(args[0])
        val isOn = args[1]
        val lastOn = args[2]
        val st = when (isOn) {
            "on", "app" -> {
                val idle = if (lastOn == "0") 0.0 else currentEpochSeconds() - lastOn.toDouble() * 60
                PMStatus(0.0, if (isOn == "on") "online" else "app", idle)
            }
            else -> PMStatus(if (lastOn == "None") 0.0 else lastOn.toDouble(), "offline", 0.0)
        }
        status[user] = st
        contacts.add(user)
        fire("onPMContactAdd", user) { onPMContactAdd(it, user) }
    }

    private fun rcmdWldelete(args: List<String>) {
        val user = User.get(args[0])
        contacts.remove(user)
        fire("onPMContactRemove", user) { onPMContactRemove(it, user) }
    }

    private fun rcmdBlockList(args: List<String>) {
        blocklist = LinkedHashSet()
        for (name in args) {
            if (name == "") continue
            blocklist.add(User.get(name))
        }
    }

    private fun rcmdIdleUpdate(args: List<String>) {
        val user = User.get(args[0])
        val st = status[user] ?: return
        st.idle = if (args[1] == "1") 0.0 else currentEpochSeconds()
    }

    private fun updateTrack(args: List<String>) {
        val user = User.get(args[0])
        val isOn = args[2]
        val idle: Double
        val lastOn: Double
        if (isOn == "online") {
            idle = if (args[1] == "0") 0.0 else currentEpochSeconds() - args[1].toDouble() * 60
            lastOn = idle
        } else {
            lastOn = args[1].toDouble()
            idle = 0.0
        }
        status[user] = PMStatus(lastOn, isOn, idle)
        trackWaiters.remove(user)?.countDown()
    }

    fun ping() {
        sendCommand("")
        fire("onPMPing") { onPMPing(it) }
    }

    fun message(user: User, msg: String?) {
        if (msg == null) return
        val me = mgr.user
        val body = "<n${me.nameColor.lowercase()}/><m v=\"1\"><g x${me.fontSize}s${me.fontColor.lowercase()}=\"${me.fontFace}\">$msg</g></m>"
        sendCommand("msg", user.name, body)
    }

    fun addContact(user: User) {
        if (user !in contacts) sendCommand("wladd", user.name)
    }

    fun removeContact(user: User) {
        if (user in contacts) sendCommand("wldelete", user.name)
    }

    fun block(user: User) {
        if (user !in blocklist) {
            sendCommand("block", user.name, user.name, "S")
            blocklist.add(user)
            fire("onPMBlock", user) { onPMBlock(it, user) }
        }
    }

    fun unblock(user: User) {
        if (user in blocklist) sendCommand("unblock", user.name)
    }

    fun setBgMode(mode: Int) = sendCommand("msgbg", mode.toString())
    fun setRecordingMode(mode: Int) = sendCommand("msgmedia", mode.toString())
    fun setIdle() = sendCommand("idle:0")
    fun setActive() = sendCommand("idle:1")

    fun rawTrack(user: User) = sendCommand("track", user.name)

    fun track(user: User, timeoutMs: Long = 3000): PMStatus? {
        if (mgr.isLoopThread()) {
            rawTrack(user)
            return status[user]
        }
        val latch = CountDownLatch(1)
        trackWaiters[user] = latch
        mgr.post { rawTrack(user) }
        latch.await(timeoutMs, TimeUnit.MILLISECONDS)
        return status[user]
    }

    fun checkOnline(user: User): String? = status[user]?.state

    fun getIdle(user: User): Double? {
        val st = status[user] ?: return null
        if (st.state.isEmpty()) return 0.0
        if (st.idle == 0.0) return currentEpochSeconds()
        return st.idle
    }
}

typealias PrivateMessageConnection = PrivateMessageSession
typealias PM = PrivateMessageSession
