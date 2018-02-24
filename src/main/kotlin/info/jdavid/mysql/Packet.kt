package info.jdavid.mysql

import java.nio.ByteBuffer
import java.nio.ByteOrder

sealed class Packet {

  internal interface FromServer
  internal interface FromClient {
    fun writeTo(buffer: ByteBuffer)
  }

  class HandshakeResponse(private val database: String,
                          private val username: String, private val authResponse: ByteArray,
                          private val handshake: Handshake): FromClient, Packet() {

    override fun writeTo(buffer: ByteBuffer) {
      val start = buffer.position()
      buffer.putInt(0)
      buffer.putInt(Capabilities.clientCapabilities())
      buffer.putInt(4092)
      buffer.put(Collations.UTF8)
      buffer.put(ByteArray(23))
      buffer.put(username.toByteArray())
      buffer.put(0.toByte())
      buffer.put(authResponse.size.toByte())
      buffer.put(authResponse)
      buffer.put(database.toByteArray())
      buffer.put(0.toByte())
      buffer.put(handshake.auth?.toByteArray())
      buffer.put(0.toByte())
      buffer.putInt(start, buffer.position() - start - 4)
      buffer.put(start + 3, 1.toByte())
    }
  }

  class StatementPrepare(private val query: String): FromClient, Packet() {
    override fun toString() = "StatementPrepare(): ${query}"
    override fun writeTo(buffer: ByteBuffer) {
      val start = buffer.position()
      buffer.putInt(0)
      buffer.put(0x16.toByte())
      buffer.put(query.toByteArray())
      buffer.putInt(start, buffer.position() - start - 4)
    }
  }

  class StatementClose(private val statementId: Int): FromClient, Packet() {
    override fun toString() = "StatementClose()"
    override fun writeTo(buffer: ByteBuffer) {
      val start = buffer.position()
      buffer.putInt(0)
      buffer.put(0x19.toByte())
      buffer.putInt(statementId)
      buffer.putInt(start, buffer.position() - start - 4)
    }
  }

  class StatementReset(private val statementId: Int): FromClient, Packet() {
    override fun toString() = "StatementClose()"
    override fun writeTo(buffer: ByteBuffer) {
      val start = buffer.position()
      buffer.putInt(0)
      buffer.put(0x1a.toByte())
      buffer.putInt(statementId)
      buffer.putInt(start, buffer.position() - start - 4)
    }
  }

  class StatementExecute(private val statementId: Int, private val params: Iterable<Any?>): FromClient, Packet() {
    override fun toString() = "StatementClose()"
    override fun writeTo(buffer: ByteBuffer) {
      val list = params.toList()
      val start = buffer.position()
      buffer.putInt(0)
      buffer.put(0x17.toByte())
      buffer.putInt(statementId)
      buffer.putInt(0) //
      val bitmap = Bitmap(list.size)
      list.forEachIndexed { index, any -> if (list[index] == null) bitmap.set(index, true) }
      buffer.put(bitmap.bytes)
      buffer.put(0)
      buffer.putInt(start, buffer.position() - start - 4)
    }
  }


  //-----------------------------------------------------------------------------------------------


  class OK(internal val sequenceId: Byte, private val info: String): FromServer, Packet() {
    override fun toString() = "GenericOK(){\n${info}\n}"
  }

  class StatementPrepareOK(internal val sequenceId: Byte,
                           internal val statementId: Int,
                           internal val columnCount: Short,
                           internal val paramCount: Short): FromServer, Packet() {
    override fun toString() = "StatementPrepareOK(${statementId})"
  }

  class ColumnDefinition(internal val name: String, internal val table: String,
                         internal val type: Byte, internal val length: Int,
                         internal val unsigned: Boolean): FromServer, Packet() {
    override fun toString() = "ColumnDefinition(${table}.${name})"
  }

  class BinaryResultSet(internal val columnCount: Int): FromServer, Packet() {
    override fun toString() = "ResultSet(${columnCount})"
  }

  class Row(internal val bytes: ByteArray?): FromServer, Packet() {
    override fun toString(): String {
      return if (bytes == null) {
        "EOF()"
      }
      else {
        "Row(${MysqlConnection.hex(bytes)})"
      }
    }
    internal fun decode(cols: List<ColumnDefinition>): Map<String,Any?> {
      val n = cols.size
      if (n == 0) return emptyMap()
      val buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
      val bitmap =  Bitmap(n).set(buffer)
      val map = LinkedHashMap<String,Any?>(n)
      for (i in 0 until n) {
        val col = cols[i]
        if (bitmap.get(i)) {
          map[col.name] = null
        }
        else {
          map[col.name] = BinaryFormat.parse(col.type, col.length, col.unsigned, buffer)
        }
      }
      return map
    }
  }

  class EOF : FromServer, Packet() {
    override fun toString() = "EOF()"
  }

  class Handshake(internal val connectionId: Int,
                  internal val scramble: ByteArray,
                  internal val auth: String?): FromServer, Packet() {
    override fun toString() = "HANDSHAKE(auth: ${auth ?: "null"})"
  }

  companion object {

    @Suppress("UsePropertyAccessSyntax", "UNCHECKED_CAST")
    internal fun <T: FromServer> fromBytes(buffer: ByteBuffer, expected: Class<T>?): T {
      val length = BinaryFormat.threeByteInteger(buffer)
      val sequenceId = buffer.get()
      if (length > buffer.remaining()) throw RuntimeException("Connection buffer too small.")
      val start = buffer.position()
      val first = buffer.get()
      if (first == 0xff.toByte()) {
        val errorCode = buffer.getShort()
        val sqlState = ByteArray(5).let {
          buffer.get(it)
          String(it)
        }
        val message = ByteArray(start + length - buffer.position()).let {
          buffer.get(it)
          String(it)
        }
        assert(start + length == buffer.position())
        throw Exception("Error code: ${errorCode}, SQLState: ${sqlState}\n${message}")
      }
      return when (expected) {
        OK::class.java -> {
          println("OK")
          assert(first == 0x00.toByte() || first == 0xfe.toByte())
          /*val affectedRows =*/ BinaryFormat.getLengthEncodedInteger(buffer)
          /*val lastInsertId =*/ BinaryFormat.getLengthEncodedInteger(buffer)
          /*val status =*/ ByteArray(2).apply { buffer.get(this) }
          /*val warningCount =*/ buffer.getShort()
          val info = ByteArray(start + length - buffer.position()).let {
            buffer.get(it)
            String(it)
          }
          assert(start + length == buffer.position())
          OK(sequenceId, info) as T
        }
        EOF::class.java -> {
          println("EOF")
          assert(first == 0xfe.toByte())
          // CLIENT_DEPRECATE_EOF is set -> EOF is replaced with OK
          /* val warningCount =*/ buffer.getShort()
          /*val status =*/ ByteArray(2).apply { buffer.get(this) }
          assert(start + length == buffer.position())
          return EOF() as T
        }
        StatementPrepareOK::class.java -> {
          println("STMT_PREPARE_OK")
          assert(first == 0x00.toByte())
          val statementId = buffer.getInt()
          val columnCount = buffer.getShort()
          val paramCount = buffer.getShort()
          /*val filler =*/ buffer.get()
          /*val warningCount =*/ buffer.getShort()
          assert(start + length == buffer.position())
          StatementPrepareOK(sequenceId, statementId, columnCount, paramCount) as T
        }
        ColumnDefinition::class.java -> {
          println("COLUMN_DEFINITION")
          assert(first == 3.toByte())
          ByteArray(first.toInt()).apply { buffer.get(this) }//.apply { assert(String(this) == "def") }
          /*val schema =*/ BinaryFormat.getLengthEncodedString(buffer)
          val table = BinaryFormat.getLengthEncodedString(buffer)
          /*val tableOrg =*/ BinaryFormat.getLengthEncodedString(buffer)
          val name = BinaryFormat.getLengthEncodedString(buffer)
          /*val nameOrg =*/ BinaryFormat.getLengthEncodedString(buffer)
          /*val n =*/ BinaryFormat.getLengthEncodedInteger(buffer)
          /*val collation =*/ buffer.getShort()
          val columnLength = buffer.getInt()
          val columnType = buffer.get()
          val flags = Bitmap(16).set(buffer)
          val unsigned = flags.get(5)
          /*val maxDigits =*/ buffer.get()
          /*val filler =*/ ByteArray(2).apply { buffer.get(this) }
          assert(start + length == buffer.position())
          return ColumnDefinition(name, table, columnType, columnLength, unsigned) as T
        }
        BinaryResultSet::class.java -> {
          println("RESULTSET")
          assert(start + length == buffer.position())
          return BinaryResultSet(first.toInt()) as T
        }
        Row::class.java -> {
          println("ROW")
          if (first == 0xfe.toByte()) {
            /*val affectedRows =*/ BinaryFormat.getLengthEncodedInteger(buffer)
            /*val lastInsertId =*/ BinaryFormat.getLengthEncodedInteger(buffer)
            /*val status =*/ ByteArray(2).apply { buffer.get(this) }
            /*val warningCount =*/ buffer.getShort()
            val info = ByteArray(start + length - buffer.position()).let {
              buffer.get(it)
              String(it)
            }
            assert(start + length == buffer.position())
            return Row(null) as T
          }
          else {
            assert(first == 0x00.toByte())
            val bytes = ByteArray(start + length - buffer.position()).apply { buffer.get(this) }
            return Row(bytes) as T
          }
        }
        Handshake::class.java -> {
          println("HANDSHAKE")
          assert(first == 0x0a.toByte())
          /*val serverVersion =*/ BinaryFormat.getNullTerminatedString(buffer)
          val connectionId = buffer.getInt()
          var scramble = ByteArray(8).apply { buffer.get(this) }
          val filler1 = buffer.get()
          assert(filler1 == 0.toByte())
          val capabilitiesBytes = ByteArray(4).apply { buffer.get(this, 2, 2) }
          if (start + length > buffer.position()) {
            /*val collation =*/ buffer.get()
            /*val status =*/ ByteArray(2).apply { buffer.get(this) }
            buffer.get(capabilitiesBytes, 0, 2)
            val n = Math.max(12, buffer.get() - 9)
            buffer.get(ByteArray(10))
            scramble = ByteArray(scramble.size + n).apply {
              System.arraycopy(scramble, 0, this, 0, scramble.size)
              buffer.get(this, scramble.size, n)
            }
            /*val filler2 =*/ buffer.get()
            //assert(filler2 == 0.toByte())
          }
          val auth = BinaryFormat.getNullTerminatedString(buffer, start + length)
          assert(start + length == buffer.position())
          return Handshake(connectionId, scramble, auth) as T
        }
        else -> throw IllegalArgumentException()
      }
    }

  }

  class Exception(message: String): RuntimeException(message)

}
