package info.jdavid.sql

import kotlinx.coroutines.experimental.channels.ChannelIterator
import java.io.Closeable

interface Connection<C: Connection<C>>: Closeable {

  suspend fun rows(params: Iterable<Any?>): ResultSet
  suspend fun rows(preparedStatement: PreparedStatement<C>,
                   params: Iterable<Any?>): ResultSet

  suspend fun affectedRows(params: Iterable<Any?>): Int
  suspend fun affectedRows(preparedStatement: PreparedStatement<C>,
                           params: Iterable<Any?>): Int

  interface PreparedStatement<C: Connection<C>> {
    suspend fun rows(params: Iterable<Any?>): ResultSet
    suspend fun affectedRows(params: Iterable<Any?>): Int
    suspend fun close()
  }

  interface ResultSet: Closeable {
    operator fun iterator(): ChannelIterator<Map<String, Any?>>
    suspend fun toList(): List<Map<String, Any?>>
  }

}
