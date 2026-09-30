package chatango.manager

import chatango.connection.PrivateMessageSession
import chatango.connection.ChatRoom
import chatango.model.Conn
import chatango.model.Message
import chatango.model.Participant
import chatango.model.User
import chatango.util.BigMessage
import chatango.util.currentEpochSeconds
import okhttp3.OkHttpClient
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArraySet
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

open class SessionManager(val name: String? = null, val password: String? = null, usePm: Boolean = true) {
    open val pmHost = "c1.chatango.com"
    open val pmPort = 8080
    open val pingDelay = 20.0
    open val tooBigMessage = BigMessage.Multiple
    open val maxLength = 2800
    open val maxHistoryLength = 150
    open val floodPauseMs = 60_000L

    val http: OkHttpClient = OkHttpClient.Builder()
        .connectTimeout(10, TimeUnit.SECONDS)
        .callTimeout(20, TimeUnit.SECONDS)
        .build()
    val wsClient: OkHttpClient = OkHttpClient.Builder()
        .connectTimeout(10, TimeUnit.SECONDS)
        .readTimeout(0, TimeUnit.MILLISECONDS)
        .build()

    internal val roomsMap = ConcurrentHashMap<String, ChatRoom>()
    val pm: PrivateMessageSession? = if (usePm) PrivateMessageSession(this) else null

    @Volatile
    private var running = false
    private val queue = LinkedBlockingQueue<() -> Unit>()
    private val tasks = CopyOnWriteArraySet<Task>()

    @Volatile
    private var loopThread: Thread? = null

    val user: User get() = User.get(name)
    val rooms: Set<ChatRoom> get() = roomsMap.values.toSet()
    val roomnames: Set<String> get() = roomsMap.keys.toSet()

    fun joinRoom(room: String) {
        val r = room.lowercase()
        if (!roomsMap.containsKey(r)) ChatRoom(r, mgr = this)
    }

    fun leaveRoom(room: String) {
        roomsMap[room.lowercase()]?.disconnect()
    }

    fun getRoom(room: String): ChatRoom? = roomsMap[room.lowercase()]

    open fun onInit() {}
    open fun onConnect(room: ChatRoom) {}
    open fun onReconnect(room: ChatRoom) {}
    open fun onConnectFail(room: ChatRoom) {}
    open fun onDisconnect(room: ChatRoom) {}
    open fun onLoginFail(conn: Conn) {}
    open fun onGroupFlagsUpdate(room: ChatRoom) {}
    open fun onGroupFlagsAdded(room: ChatRoom, flags: Map<String, Long>) {}
    open fun onGroupFlagsRemoved(room: ChatRoom, flags: Map<String, Long>) {}
    open fun onFloodBan(room: ChatRoom) {}
    open fun onFloodBanRepeat(room: ChatRoom) {}
    open fun onFloodWarning(room: ChatRoom) {}
    open fun onMessageDelete(room: ChatRoom, user: User, message: Message) {}
    open fun onModChange(room: ChatRoom) {}
    open fun onModAdd(room: ChatRoom, user: User) {}
    open fun onClearAll(room: ChatRoom) {}
    open fun onModRemove(room: ChatRoom, user: User) {}
    open fun onMessage(room: ChatRoom, user: User, message: Message) {}
    open fun onBannedWordsUpdated(room: ChatRoom, words: List<String>) {}
    open fun onHistoryMessage(room: ChatRoom, user: User, message: Message) {}
    open fun onJoin(room: ChatRoom, user: User, puid: String) {}
    open fun onLeave(room: ChatRoom, user: User, puid: String) {}
    open fun onRaw(conn: Conn, raw: String) {}
    open fun onPing(room: ChatRoom) {}
    open fun onUserCountChange(room: ChatRoom) {}
    open fun onBan(room: ChatRoom, user: User, target: User) {}
    open fun onUnban(room: ChatRoom, user: User, target: User) {}
    open fun onBanlistUpdate(room: ChatRoom) {}
    open fun onUnBanlistUpdate(room: ChatRoom) {}
    open fun onSnapshot(room: ChatRoom, participants: List<Participant>) {}

    open fun onPMConnect(pm: PrivateMessageSession) {}
    open fun onPMReconnect(pm: PrivateMessageSession) {}
    open fun onPMDisconnect(pm: PrivateMessageSession) {}
    open fun onPMPing(pm: PrivateMessageSession) {}
    open fun onPMMessage(pm: PrivateMessageSession, user: User, body: String) {}
    open fun onPMOfflineMessage(pm: PrivateMessageSession, user: User, body: String) {}
    open fun onPMContactlistReceive(pm: PrivateMessageSession) {}
    open fun onPMBlocklistReceive(pm: PrivateMessageSession) {}
    open fun onPMContactAdd(pm: PrivateMessageSession, user: User) {}
    open fun onPMContactRemove(pm: PrivateMessageSession, user: User) {}
    open fun onPMBlock(pm: PrivateMessageSession, user: User) {}
    open fun onPMUnblock(pm: PrivateMessageSession, user: User) {}
    open fun onPMContactOnline(pm: PrivateMessageSession, user: User) {}
    open fun onPMContactApp(pm: PrivateMessageSession, user: User) {}
    open fun onPMContactOffline(pm: PrivateMessageSession, user: User) {}
    open fun onEventCalled(conn: Conn, evt: String, args: Array<out Any?>) {}

    class Task internal constructor(
        private val mgr: SessionManager,
        internal var target: Double,
        internal val timeout: Double,
        internal val func: () -> Unit,
        internal val isInterval: Boolean
    ) {
        fun cancel() = mgr.removeTask(this)
    }

    fun post(block: () -> Unit) {
        queue.add(block)
    }

    fun isLoopThread(): Boolean = loopThread === Thread.currentThread()

    fun setTimeout(timeout: Double, func: () -> Unit): Task {
        val t = Task(this, currentEpochSeconds() + timeout, timeout, func, false)
        tasks.add(t)
        return t
    }

    fun setInterval(timeout: Double, func: () -> Unit): Task {
        val t = Task(this, currentEpochSeconds() + timeout, timeout, func, true)
        tasks.add(t)
        return t
    }

    fun removeTask(task: Task) {
        tasks.remove(task)
    }

    private fun runDueTasks() {
        val now = currentEpochSeconds()
        for (task in tasks.toList()) {
            if (task.target <= now) {
                if (task.isInterval) task.target = now + task.timeout else tasks.remove(task)
                executeScheduledTask(task)
            }
        }
    }

    private fun executeScheduledTask(task: Task) {
        try {
            task.func()
        } catch (e: Exception) {
            println(e)
        }
    }

    private fun dispatchQueuedWork() {
        var action = queue.poll(100, TimeUnit.MILLISECONDS)
        var dispatchedCount = 0
        while (action != null && dispatchedCount < 200) {
            executeQueuedWork(action)
            dispatchedCount++
            action = queue.poll()
        }
    }

    private fun executeQueuedWork(action: () -> Unit) {
        try {
            action()
        } catch (e: Exception) {
            println(e)
            if (DEBUG) e.printStackTrace()
        }
    }

    fun main() {
        loopThread = Thread.currentThread()
        pm?.connect()
        onInit()
        running = true
        while (running) {
            dispatchQueuedWork()
            runDueTasks()
        }
    }

    fun stop() {
        running = false
        for (room in roomsMap.values.toList()) room.disconnect()
        pm?.disconnect()
        http.dispatcher.executorService.shutdown()
        wsClient.dispatcher.executorService.shutdown()
        http.connectionPool.evictAll()
        wsClient.connectionPool.evictAll()
    }

    fun enableBg() {
        user.mbg = true
        rooms.forEach { it.setBgMode(1) }
    }

    fun disableBg() {
        user.mbg = false
        rooms.forEach { it.setBgMode(0) }
    }

    fun enableRecording() {
        user.mrec = true
        rooms.forEach { it.setRecordingMode(1) }
    }

    fun disableRecording() {
        user.mrec = false
        rooms.forEach { it.setRecordingMode(0) }
    }

    fun setNameColor(color3x: String) {
        user.nameColor = color3x
    }

    fun setFontColor(color3x: String) {
        user.fontColor = color3x
    }

    fun setFontFace(face: String) {
        user.fontFace = face
    }

    fun setFontSize(size: Int) {
        user.fontSize = size.coerceIn(9, 22)
    }

    companion object {
        var DEBUG = false
    }
}

typealias ChatangoSessionManager = SessionManager
typealias RoomManager = SessionManager
