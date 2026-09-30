package chatango.bot

import chatango.connection.ChatRoom
import chatango.manager.SessionManager
import chatango.model.Message
import chatango.model.User

class ChatBot(name: String?, password: String?, usePm: Boolean) : SessionManager(name, password, usePm) {

    override fun onInit() {
        println("Bot started")
    }

    override fun onConnect(room: ChatRoom) {
        println("Connected to ${room.name}")
    }

    override fun onDisconnect(room: ChatRoom) {
        println("Disconnected from ${room.name}")
    }

    override fun onMessage(room: ChatRoom, user: User, message: Message) {
        println("[${room.name}] ${user.name}: ${message.body}")
        if (user == this.user) return
        if (message.body.trim().lowercase() == "!ping") {
            room.message("pong")
        }
    }
}

typealias ChatangoBot = ChatBot
typealias Bot = ChatBot
