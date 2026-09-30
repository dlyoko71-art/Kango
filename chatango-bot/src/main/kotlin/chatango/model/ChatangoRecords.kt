package chatango.model

data class RoomParticipant(val user: User, val time: Double, val puid: String, val name: String, val anon: String)

data class UserBanRecord(val unid: String, val ip: String, val target: User, val time: Double, val src: User)

data class PrivateMessageStatus(var lastOn: Double, var state: String, var idle: Double)

typealias Participant = RoomParticipant
typealias BanRecord = UserBanRecord
typealias PMStatus = PrivateMessageStatus
