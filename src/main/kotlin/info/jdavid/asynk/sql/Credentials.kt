package info.jdavid.asynk.sql

import java.net.InetAddress
import java.net.SocketAddress

interface Credentials<C: Connection<C>> {
  suspend fun connectTo(database: String): C
  suspend fun connectTo(database: String, bufferSize: Int): C
  suspend fun connectTo(database: String, address: InetAddress): C
  suspend fun connectTo(database: String, address: InetAddress, bufferSize: Int): C
  suspend fun connectTo(database: String, address: SocketAddress): C
  suspend fun connectTo(database: String, address: SocketAddress, bufferSize: Int): C
}
