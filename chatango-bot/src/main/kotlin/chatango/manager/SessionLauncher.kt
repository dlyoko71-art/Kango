package chatango.manager

fun launchChatangoSession(
    rooms: List<String>? = null,
    name: String? = null,
    password: String? = null,
    pm: Boolean = true,
    factory: (String?, String?, Boolean) -> SessionManager
) {
    try {
        var roomList = rooms
        if (roomList.isNullOrEmpty()) {
            print("Room names (,): ")
            roomList = (readLine() ?: "").split(";")
        }
        if (roomList.size == 1 && roomList[0] == "") roomList = emptyList()

        var n = name
        if (n == null) {
            print("User name: ")
            n = readLine() ?: ""
        }
        if (n == "" || " " in n) n = null

        var p = password?.takeIf { it.isNotEmpty() }
            ?: System.getenv("CHATANGO_PASSWORD")?.takeIf { it.isNotEmpty() }
        if (p == null) {
            print("User password: ")
            p = readLine() ?: ""
        }
        if (p == "") p = null

        var usePm = pm
        if (n == null || p == null) usePm = false

        val mgr = factory(n, p, usePm)
        roomList.forEach { mgr.joinRoom(it) }
        mgr.main()
    } catch (e: Exception) {
        println(e.toString())
    }
}

fun startChatangoSession(
    rooms: List<String>? = null,
    name: String? = null,
    password: String? = null,
    pm: Boolean = true,
    factory: (String?, String?, Boolean) -> SessionManager
) = launchChatangoSession(rooms, name, password, pm, factory)

fun easyStart(
    rooms: List<String>? = null,
    name: String? = null,
    password: String? = null,
    pm: Boolean = true,
    factory: (String?, String?, Boolean) -> SessionManager
) = launchChatangoSession(rooms, name, password, pm, factory)
