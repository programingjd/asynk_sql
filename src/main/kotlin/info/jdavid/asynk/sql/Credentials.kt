package info.jdavid.asynk.sql

import java.net.InetAddress
import java.net.SocketAddress

/**
 * Common interface for SQL database credentials, with helper methods to connect to the database and get a
 * [Connection<*>] back.
 */
interface Credentials<C: Connection<C>> {
  /**
   * Connects to the database with the specified name on localhost with the default port,
   * and the default buffer size.
   * @param database the database name.
   * @return a database [Connection<*>].
   */
  suspend fun connectTo(database: String): C
  /**
   * Connects to the database with the specified name on localhost with the default port.
   * @param database the database name.
   * @param bufferSize the buffer size.
   * @return a database [Connection<*>].
   */
  suspend fun connectTo(database: String, bufferSize: Int): C
  /**
   * Connects to the database with the specified name with the default port and buffer size,
   * @param database the database name.
   * @param address the address of the database server.
   * @return a database [Connection<*>].
   */
  suspend fun connectTo(database: String, address: InetAddress): C
  /**
   * Connects to the database with the specified name with the default port.
   * @param database the database name.
   * @param address the address of the database server.
   * @param bufferSize the buffer size.
   * @return a database [Connection<*>].
   */
  suspend fun connectTo(database: String, address: InetAddress, bufferSize: Int): C
  /**
   * Connects to the database with the specified name with the default buffer size,
   * @param database the database name.
   * @param address the address and port of the database server.
   * @return a database [Connection<*>].
   */
  suspend fun connectTo(database: String, address: SocketAddress): C
  /**
   * Connects to the database with the specified name.
   * @param database the database name.
   * @param address the address and port of the database server.
   * @param bufferSize the buffer size.
   * @return a database [Connection<*>].
   */
  suspend fun connectTo(database: String, address: SocketAddress, bufferSize: Int): C
}
