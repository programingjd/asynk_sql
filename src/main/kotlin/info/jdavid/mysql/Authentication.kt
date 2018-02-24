package info.jdavid.mysql

import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.SocketAddress
import java.security.MessageDigest

object Authentication {

  internal suspend fun authenticate(connection: MysqlConnection,
                                    database: String,
                                    credentials: MysqlCredentials) {
    val handshake = connection.receive(Packet.Handshake::class.java)
    when (handshake.auth) {
      null -> throw RuntimeException("Unsupported authentication method: null")
      "mysql_old_password", "mysql_clear_password", "dialog" -> throw Exception(
        "Unsupported authentication method: ${handshake.auth}"
      )
      "auth_gssapi_client" -> throw Exception("Incompatible credentials.")
      "mysql_native_password" -> {
        if (credentials !is MysqlCredentials.PasswordCredentials) throw Exception("Incompatible credentials.")
        val authResponse = nativePassword(credentials.password, handshake.scramble)
        connection.send(Packet.HandshakeResponse(database, credentials.username, authResponse, handshake))
        val ok = connection.receive(Packet.OK::class.java)
        assert (ok.sequenceId == 2.toByte())
      }
      else -> throw Exception("Unsupported authentication method: ${handshake.auth}")
    }
  }

  private fun nativePassword(password: String, salt: ByteArray): ByteArray {
    val sha1 = MessageDigest.getInstance("SHA1")
    val s = sha1.digest(password.toByteArray())
    val ss = sha1.digest(s)
    sha1.update(salt)
    val xor = sha1.digest(ss)
    return ByteArray(s.size, { i ->
      (s[i].toInt() xor xor[i].toInt()).toByte()
    })
  }

  class Exception(message: String): RuntimeException(message)

  sealed class MysqlCredentials(internal val username: String) {

    class UnsecuredCredentials(username: String = "root"): MysqlCredentials(username)
    class PasswordCredentials(username: String = "root",
                              internal val password: String = ""): MysqlCredentials(username)

    suspend fun connectTo(
      database: String,
      address: SocketAddress = InetSocketAddress(InetAddress.getLoopbackAddress (), 3306)
    ): MysqlConnection {
      return MysqlConnection.to(database, this, address)
    }

  }

}
