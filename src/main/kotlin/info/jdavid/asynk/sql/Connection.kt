package info.jdavid.asynk.sql

import kotlinx.coroutines.experimental.channels.ChannelIterator
import java.io.Closeable

/**
 * Common interface for an SQL database connection.
 */
interface Connection<C: Connection<C>>: ACloseable {

  /**
   * Starts an SQL transaction.
   */
  suspend fun startTransaction()

  /**
   * Commits an SQL transaction.
   */
  suspend fun commitTransaction()

  /**
   * Rolls back an SQL transaction.
   */
  suspend fun rollbackTransaction()

  /**
   * Executes the given [block] function inside a transaction scope. The transaction is either committed or
   * rolled back if the block execution threw an exception.
   * @param block a function to execute in a new transaction context.
   * @return the result of [block] function.
   */
  suspend fun <R> withTransaction(block: suspend () -> R): R {
    var throwable: Throwable? = null
    try {
      startTransaction()
      try {
        return block()
      }
      catch (e: Throwable) {
        throwable = e
        throw e
      }
    }
    finally {
      if (throwable == null) {
        commitTransaction()
      }
      else {
        rollbackTransaction()
      }
    }
  }

  /**
   * Creates a prepared statement from the specified SQL statement. The "?" syntax should be used for binding
   * parameters.
   * @param sqlStatement the SQL statement.
   * @return the prepared statement.
   */
  suspend fun prepare(sqlStatement: String): PreparedStatement<C>

  /**
   * Executes a row query SQL statement.
   * @param sqlStatement the SQL query statement.
   * @return the result set.
   */
  suspend fun rows(sqlStatement: String): ResultSet
  /**
   * Executes a row query SQL statement. Query parameters (the "?" syntax should be used) are bound to
   * the specified values.
   * @param sqlStatement the SQL query statement.
   * @param params the values to bind to the query parameters.
   * @return the result set.
   */
  suspend fun rows(sqlStatement: String, params: Iterable<Any?>): ResultSet
  /**
   * Executes a row query prepared statement.
   * @param preparedStatement the SQL prepared statement.
   * @return the result set.
   */
  suspend fun rows(preparedStatement: PreparedStatement<C>): ResultSet
  /**
   * Executes a row query prepared statement. Query parameters (the "?" syntax should be used) are bound to
   * the specified values.
   * @param preparedStatement the SQL prepared statement.
   * @param params the values to bind to the query parameters.
   * @return the result set.
   */
  suspend fun rows(preparedStatement: PreparedStatement<C>,
                   params: Iterable<Any?>): ResultSet

  /**
   * Executes a row insert/update/delete statement.
   * @param sqlStatement the SQL statement.
   * @return the number of rows affected (inserted, updated or deleted, depending on the type of query).
   */
  suspend fun affectedRows(sqlStatement: String): Int
  /**
   * Executes a row insert/update/delete statement. Query parameters (the "?" syntax should be used) are
   * bound to the specified values.
   * @param sqlStatement the SQL statement.
   * @param params the values to bind to the statement parameters.
   * @return the number of rows affected (inserted, updated or deleted, depending on the type of query).
   */
  suspend fun affectedRows(sqlStatement: String, params: Iterable<Any?>): Int
  /**
   * Executes a row insert/update/delete prepared statement.
   * @param preparedStatement the SQL prepared statement.
   * @return the number of rows affected (inserted, updated or deleted, depending on the type of query).
   */
  suspend fun affectedRows(preparedStatement: PreparedStatement<C>): Int
  /**
   * Executes a row insert/update/delete prepared statement. Query parameters (the "?" syntax should be used)
   * are bound to the specified values.
   * @param preparedStatement the SQL prepared statement.
   * @param params the values to bind to the statement parameters.
   * @return the number of rows affected (inserted, updated or deleted, depending on the type of query).
   */
  suspend fun affectedRows(preparedStatement: PreparedStatement<C>,
                           params: Iterable<Any?>): Int

  /**
   * Common interface for SQL Prepared Statements. The "?" syntax should be used for binding parameters.
   */
  interface PreparedStatement<C: Connection<C>>: ACloseable {
    /**
     * Executes this row query prepared statement.
     * @return the result set.
     */
    suspend fun rows(): ResultSet
    /**
     * Executes this row query prepared statement. Query parameters (the "?" syntax should be used) are bound
     * to the specified values.
     * @param params the values to bind to the query parameters.
     * @return the result set.
     */
    suspend fun rows(params: Iterable<Any?>): ResultSet
    /**
     * Executes this row insert/update/delete prepared statement.
     * @return the number of rows affected (inserted, updated or deleted, depending on the type of query).
     */
    suspend fun affectedRows(): Int
    /**
     * Executes this row insert/update/delete prepared statement. Query parameters (the "?" syntax should be
     * used) are bound to the specified values.
     * @param params the values to bind to the statement parameters.
     * @return the number of rows affected (inserted, updated or deleted, depending on the type of query).
     */
    suspend fun affectedRows(params: Iterable<Any?>): Int
  }

  /**
   * Common interface for row result sets.
   */
  interface ResultSet: Closeable {
    operator fun iterator(): ChannelIterator<Map<String, Any?>>
    /**
     * Fetches all the rows (represented as a Map<String, Any?> of name/value pairs) and returns them
     * as a list.
     * @return a list with all the rows in the result set.
     */
    suspend fun toList(): List<Map<String, Any?>>
  }

}
