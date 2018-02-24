package info.jdavid.mysql

import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.channels.toList
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.nio.aConnect
import kotlinx.coroutines.experimental.nio.aRead
import kotlinx.coroutines.experimental.nio.aWrite
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.SocketAddress
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.AsynchronousSocketChannel
import java.util.concurrent.TimeUnit
import kotlin.coroutines.experimental.EmptyCoroutineContext

class MysqlConnection internal constructor(private val channel: AsynchronousSocketChannel,
                                           private val buffer: ByteBuffer): Closeable {
  private var statementCounter = 0

  override fun close() = channel.close()

  suspend fun affectedRows(sqlStatement: String, params: Iterable<Any?> = emptyList()): Int {
    val statement = prepare(sqlStatement, true)
    return affectedRows(statement, params)
  }

  suspend fun affectedRows(preparedStatement: PreparedStatement,
                           params: Iterable<Any?> = emptyList()): Int {
    TODO()
  }

  suspend fun rows(sqlStatement: String,
                   params: Iterable<Any?> = emptyList()): ResultSet {
    val statement = prepare(sqlStatement, true)
    return rows(statement, params)
  }

  suspend fun rows(preparedStatement: PreparedStatement,
                   params: Iterable<Any?> = emptyList()): ResultSet {
    send(Packet.StatementExecute(preparedStatement.id, params))
    val rs = receive(Packet.BinaryResultSet::class.java)
    val n = rs.columnCount
    val cols = ArrayList<Packet.ColumnDefinition>(n)
    for (i in 0 until n) {
      cols.add(receive(Packet.ColumnDefinition::class.java))
    }

    val channel = Channel<Map<String, Any?>>()
    launch(EmptyCoroutineContext) {
      while (true) {
        val row = receive(Packet.Row::class.java)
        if (row.bytes == null) break
        println(row)
        channel.send(row.decode(cols))
      }
      if (preparedStatement.temporary) {
        send(Packet.StatementReset(preparedStatement.id))
        val ok = receive(Packet.OK::class.java)
      }
      channel.close()
    }
    return ResultSet(channel)
  }

  suspend fun close(preparedStatement: PreparedStatement) {
    send(Packet.StatementClose(preparedStatement.id))
  }

  suspend fun prepare(sqlStatement: String): PreparedStatement {
    return prepare(sqlStatement, false)
  }

  private suspend fun prepare(sqlStatement: String, temporary: Boolean): PreparedStatement {
    send(Packet.StatementPrepare(sqlStatement))
    val prepareOK = receive(Packet.StatementPrepareOK::class.java)
    for (i in 1..prepareOK.columnCount) receive(Packet.ColumnDefinition::class.java)
    for (i in 1..prepareOK.paramCount) receive(Packet.ColumnDefinition::class.java)
    return PreparedStatement(prepareOK.statementId, temporary)
  }

  internal suspend fun send(packet: Packet.FromClient) {
    assert(buffer.limit() == buffer.position())
    packet.writeTo(buffer.clear() as ByteBuffer)
    channel.aWrite(buffer.flip() as ByteBuffer, 5000L, TimeUnit.MILLISECONDS)
    buffer.clear().flip()
  }

  internal suspend fun <T: Packet.FromServer> receive(type: Class<T>): T {
    if (buffer.remaining() > 0) return Packet.fromBytes(buffer, type)
    buffer.compact()
    val left = buffer.capacity() - buffer.position()
    val n = channel.aRead(buffer, 5000L, TimeUnit.MILLISECONDS)
    if (n == left) throw RuntimeException("Connection buffer too small.")
    buffer.flip()
    return Packet.fromBytes(buffer, type)
  }

  inner class PreparedStatement internal constructor(internal val id: Int,
                                                     internal val temporary: Boolean) {
    suspend fun rows(params: Iterable<Any?> = emptyList()) = this@MysqlConnection.rows(this, params)
    suspend fun affectedRows(params: Iterable<Any?> = emptyList()) = this@MysqlConnection.affectedRows(this, params)
    suspend fun close() = this@MysqlConnection.close(this)
  }

  class ResultSet(private val channel: Channel<Map<String, Any?>>): Closeable {
    operator fun iterator() = channel.iterator()
    override fun close() { channel.cancel() }
    suspend fun toList() = channel.toList()
  }

  companion object {
    suspend fun to(
      database: String,
      credentials: Authentication.MysqlCredentials = Authentication.MysqlCredentials.UnsecuredCredentials(),
      address: SocketAddress = InetSocketAddress(InetAddress.getLoopbackAddress(), 5432)
    ): MysqlConnection {
      val channel = AsynchronousSocketChannel.open()
      try {
        channel.aConnect(address)
        val buffer = ByteBuffer.allocateDirect(4194304)// needs to hold any RowData message
        buffer.order(ByteOrder.LITTLE_ENDIAN).flip()
        val connection = MysqlConnection(channel, buffer)
        Authentication.authenticate(connection, database, credentials)
        return connection
      }
      catch (e: Exception) {
        channel.close()
        throw e
      }
    }
    private val logger = LoggerFactory.getLogger(MysqlConnection::class.java)
    private fun warn(message: String) = logger.warn(message)
    private fun err(message: String) = logger.error(message)

    internal fun hex(bytes: ByteArray): String {
      val chars = CharArray(bytes.size * 2)
      var i = 0
      for (b in bytes) {
        chars[i++] = Character.forDigit(b.toInt().shr(4).and(0xf), 16)
        chars[i++] = Character.forDigit(b.toInt().and(0xf), 16)
      }
      return String(chars)
    }
  }

}
