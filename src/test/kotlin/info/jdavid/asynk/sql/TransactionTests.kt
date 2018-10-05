package info.jdavid.asynk.sql

import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

class TransactionTests {

  @Test
  fun testCommit() {
    runBlocking {
      TestConn().use {
        assertFalse(it.transaction)
        it.withTransaction {
          assertTrue(it.transaction)
          assertFalse(it.rollback)
        }
        assertFalse(it.transaction)
        assertFalse(it.rollback)
      }
    }
  }

  @Test
  fun testRollback() {
    runBlocking {
      TestConn().use {
        assertFalse(it.transaction)
        try {
          it.withTransaction {
            assertTrue(it.transaction)
            assertFalse(it.rollback)
            throw RuntimeException()
          }
        }
        catch (ignore: RuntimeException) {
          assertFalse(it.transaction)
          assertTrue(it.rollback)
        }
      }
    }
  }

  private class TestConn: Connection<TestConn> {
    var transaction: Boolean = false
    var rollback: Boolean = false
    override suspend fun startTransaction() {
      transaction = true
      rollback = false
    }
    override suspend fun commitTransaction() {
      transaction = false
      rollback = false
    }
    override suspend fun rollbackTransaction() {
      transaction = false
      rollback = true
    }
    override suspend fun prepare(sqlStatement: String) = TODO()
    override suspend fun rows(sqlStatement: String) = TODO()
    override suspend fun rows(sqlStatement: String, params: Iterable<Any?>) = TODO()
    override suspend fun rows(preparedStatement: Connection.PreparedStatement<TestConn>) = TODO()
    override suspend fun rows(preparedStatement: Connection.PreparedStatement<TestConn>,
                              params: Iterable<Any?>) = TODO()
    override suspend fun <T> values(sqlStatement: String, columnNameOrAlias: String) = TODO()
    override suspend fun <T> values(sqlStatement: String, params: Iterable<Any?>,
                                    columnNameOrAlias: String) = TODO()
    override suspend fun <T> values(preparedStatement: Connection.PreparedStatement<TestConn>,
                                    columnNameOrAlias: String) = TODO()
    override suspend fun <T> values(preparedStatement: Connection.PreparedStatement<TestConn>,
                                    params: Iterable<Any?>, columnNameOrAlias: String) = TODO()
    override suspend fun <K, V> entries(sqlStatement: String, keyColumnNameOrAlias: String,
                                        valueColumnNameOrAlias: String) = TODO()
    override suspend fun <K, V> entries(sqlStatement: String, params: Iterable<Any?>,
                                        keyColumnNameOrAlias: String,
                                        valueColumnNameOrAlias: String) = TODO()
    override suspend fun <K, V> entries(preparedStatement: Connection.PreparedStatement<TestConn>,
                                        keyColumnNameOrAlias: String,
                                        valueColumnNameOrAlias: String) = TODO()
    override suspend fun <K, V> entries(preparedStatement: Connection.PreparedStatement<TestConn>,
                                        params: Iterable<Any?>, keyColumnNameOrAlias: String,
                                        valueColumnNameOrAlias: String) = TODO()
    override suspend fun affectedRows(sqlStatement: String) = TODO()
    override suspend fun affectedRows(sqlStatement: String, params: Iterable<Any?>) = TODO()
    override suspend fun affectedRows(preparedStatement: Connection.PreparedStatement<TestConn>) = TODO()
    override suspend fun affectedRows(preparedStatement: Connection.PreparedStatement<TestConn>,
                                      params: Iterable<Any?>) = TODO()
    override suspend fun close() {}
  }

}
