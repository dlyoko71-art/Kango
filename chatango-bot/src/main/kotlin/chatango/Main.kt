package chatango

import chatango.bot.Bot
import chatango.manager.easyStart

fun main(args: Array<String>) {
    val rooms = args.getOrNull(0)?.split(";")
    val name = args.getOrNull(1)
    val password = args.getOrNull(2)
    easyStart(rooms = rooms, name = name, password = password) { n, p, usePm ->
        Bot(n, p, usePm)
    }
}
