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
   * Executes a row query SQL statement and returns each row as a Map<String,Any?> where the key is the column
   * name or alias and the value is the column value for that row.
   * @param sqlStatement the SQL query statement.
   * @return the result set.
   */
  suspend fun rows(sqlStatement: String): ResultSet<Map<String,Any?>>
  /**
   * Executes a row query SQL statement and returns each row as a Map<String,Any?> where the key is the column
   * name or alias and the value is the column value for that row.<br>
   * Query parameters (the "?" syntax should be used) are bound to the specified values.
   * @param sqlStatement the SQL query statement.
   * @param params the values to bind to the query parameters.
   * @return the result set.
   */
  suspend fun rows(sqlStatement: String, params: Iterable<Any?>): ResultSet<Map<String,Any?>>
  /**
   * Executes a row query prepared statement and returns each row as Map<String,Any?> where the key is the
   * column name or alias and the value is the column value for that row.
   * @param preparedStatement the SQL prepared statement.
   * @return the result set.
   */
  suspend fun rows(preparedStatement: PreparedStatement<C>): ResultSet<Map<String,Any?>>
  /**
   * Executes a row query prepared statement and returns each row as a Map<String,Any?> where the key is the
   * column name or alias and the value is the column value for that row.<br>
   * Query parameters (the "?" syntax should be used) are bound to the specified values.
   * @param preparedStatement the SQL prepared statement.
   * @param params the values to bind to the query parameters.
   * @return the result set.
   */
  suspend fun rows(preparedStatement: PreparedStatement<C>,
                   params: Iterable<Any?>): ResultSet<Map<String,Any?>>

  /**
   * Executes a row query SQL statement and returns the value of the specified column for each row.
   * @param sqlStatement the SQL query statement.
   * @param columnNameOrAlias the name or alias of the column to get the value of.
   * @param T the value type.
   * @return the result set.
   */
  suspend fun <T> values(sqlStatement: String, columnNameOrAlias: String): ResultSet<T>
  /**
   * Executes a row query SQL statement and returns the value of the specified column for each row.<br>
   * Query parameters (the "?" syntax should be used) are bound to the specified values.
   * @param sqlStatement the SQL query statement.
   * @param params the values to bind to the query parameters.
   * @param columnNameOrAlias the name or alias of the column to get the value of.
   * @param T the value type.
   * @return the result set.
   */
  suspend fun <T> values(sqlStatement: String, params: Iterable<Any?>,
                         columnNameOrAlias: String): ResultSet<T>
  /**
   * Executes a row query prepared statement and returns the value of the specified column for each row.
   * @param preparedStatement the SQL prepared statement.
   * @param columnNameOrAlias the name or alias of the column to get the value of.
   * @param T the value type.
   * @return the result set.
   */
  suspend fun <T> values(preparedStatement: PreparedStatement<C>, columnNameOrAlias: String): ResultSet<T>
  /**
   * Executes a row query prepared statement and returns the value of the specified column for each row.<br>.
   * Query parameters (the "?" syntax should be used) are bound to the specified values.
   * @param preparedStatement the SQL prepared statement.
   * @param params the values to bind to the query parameters.
   * @param columnNameOrAlias the name or alias of the column to get the value of.
   * @param T the value type.
   * @return the result set.
   */
  suspend fun <T> values(preparedStatement: PreparedStatement<C>,
                   params: Iterable<Any?>, columnNameOrAlias: String): ResultSet<T>

  /**
   * Executes a row query SQL statement and returns a key/value pair of the specified columns for each row.
   * @param sqlStatement the SQL query statement.
   * @param keyColumnNameOrAlias the name or alias of the column to get the key value of.
   * @param valueColumnNameOrAlias the name or alias of the column to get the value of.
   * @param K the key type.
   * @param V the value type.
   * @return the result set.
   */
  suspend fun <K,V> entries(sqlStatement: String,
                            keyColumnNameOrAlias: String, valueColumnNameOrAlias: String): ResultMap<K,V>
  /**
   * Executes a row query SQL statement and returns a key/value pair of the specified columns for
   * each row.<br>
   * Query parameters (the "?" syntax should be used) are bound to the specified values.
   * @param sqlStatement the SQL query statement.
   * @param params the values to bind to the query parameters.
   * @param keyColumnNameOrAlias the name or alias of the column to get the key value of.
   * @param valueColumnNameOrAlias the name or alias of the column to get the value of.
   * @param K the key type.
   * @param V the value type.
   * @return the result set.
   */
  suspend fun <K,V> entries(sqlStatement: String, params: Iterable<Any?>,
                            keyColumnNameOrAlias: String, valueColumnNameOrAlias: String): ResultMap<K,V>
  /**
   * Executes a row query prepared statement and returns a key/value pair of the specified columns for
   * each row.
   * @param preparedStatement the SQL prepared statement.
   * @param keyColumnNameOrAlias the name or alias of the column to get the key value of.
   * @param valueColumnNameOrAlias the name or alias of the column to get the value of.
   * @param K the key type.
   * @param V the value type.
   * @return the result set.
   */
  suspend fun <K,V> entries(preparedStatement: PreparedStatement<C>,
                            keyColumnNameOrAlias: String, valueColumnNameOrAlias: String): ResultMap<K,V>
  /**
   * Executes a row query prepared statement and returns a key/value pair of the specified columns for
   * each row.<br>
   * Query parameters (the "?" syntax should be used) are bound to the specified values.
   * @param preparedStatement the SQL prepared statement.
   * @param params the values to bind to the query parameters.
   * @param keyColumnNameOrAlias the name or alias of the column to get the key value of.
   * @param valueColumnNameOrAlias the name or alias of the column to get the value of.
   * @param K the key type.
   * @param V the value type.
   * @return the result set.
   */
  suspend fun <K,V> entries(preparedStatement: PreparedStatement<C>,
                            params: Iterable<Any?>,
                            keyColumnNameOrAlias: String, valueColumnNameOrAlias: String): ResultMap<K,V>


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
     * Executes this row query prepared statement and returns each row as a Map<String,Any?> where the key
     * is the column name or alias and the value is the column value for that row.
     * @return the result set.
     */
    suspend fun rows(): ResultSet<Map<String,Any?>>
    /**
     * Executes this row query prepared statement and returns each row as a Map<String,Any?> where the key
     * is the column name or alias and the value is the column value for that row.<br>
     * Query parameters (the "?" syntax should be used) are bound to the specified values.
     * @param params the values to bind to the query parameters.
     * @return the result set.
     */
    suspend fun rows(params: Iterable<Any?>): ResultSet<Map<String,Any?>>
    /**
     * Executes this row query prepared statement and returns the value of the specified column for each row.
     * @param columnNameOrAlias the name or alias of the column to get the value of.
     * @param T the value type.
     * @return the result set.
     */
    suspend fun <T> values(columnNameOrAlias: String): ResultSet<T>
    /**
     * Executes this row query prepared statement and returns the value of the specified column for each row.
     * <br>Query parameters (the "?" syntax should be used) are bound to the specified values.
     * @param params the values to bind to the query parameters.
     * @param columnNameOrAlias the name or alias of the column to get the value of.
     * @param T the value type.
     * @return the result set.
     */
    suspend fun <T> values(params: Iterable<Any?>, columnNameOrAlias: String): ResultSet<T>
    /**
     * Executes this row query prepared statement and returns a key/value pair of the specified columns for
     * each row.
     * @param keyColumnNameOrAlias the name or alias of the column to get the key value of.
     * @param valueColumnNameOrAlias the name or alias of the column to get the value of.
     * @param K the key type.
     * @param V the value type.
     * @return the result set.
     */
    suspend fun <K,V> entries(keyColumnNameOrAlias: String, valueColumnNameOrAlias: String): ResultMap<K,V>
    /**
     * Executes this row query prepared statement and returns a key/value pair of the specified columns for
     * each row.<br>
     * Query parameters (the "?" syntax should be used) are bound to the specified values.
     * @param params the values to bind to the query parameters.
     * @param keyColumnNameOrAlias the name or alias of the column to get the key value of.
     * @param valueColumnNameOrAlias the name or alias of the column to get the value of.
     * @param K the key type.
     * @param V the value type.
     * @return the result set.
     */
    suspend fun <K,V> entries(params: Iterable<Any?>,
                              keyColumnNameOrAlias: String,
                              valueColumnNameOrAlias: String): ResultMap<K,V>
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
   * @param T the data type returned for each row result.
   */
  interface ResultSet<T>: Closeable {
    operator fun iterator(): ChannelIterator<T>
    /**
     * Fetches all the rows and returns their values as a list.
     * @return a list with all the row values in the result set.
     */
    suspend fun toList(): List<T>
    /**
     * Fetches all the rows and adds their values to the supplied collection.
     * @param destination the target collection.
     * @param C the collection type.
     * @return the collection with all the row values in the result set added to it.
     */
    suspend fun <C: MutableCollection<in T>> toCollection(destination: C): C
  }

  /**
   * Result set whose values are pairs of key,values.
   * @param K the data type of the key returned for each row.
   * @param V the data type of the value returned for each row.
   */
  interface ResultMap<K,V>: ResultSet<Pair<K,V>> {
    /**
     * Fetches the key/value pairs for all the rows and returns them as a map. If the same key is returned
     * more than once, then the last value is retained.
     * @return a map of the row values in the result set.
     */
    suspend fun toMap(): Map<K,V>
    /**
     * Fetches the key/value pairs for all the rows and adds them to the supplied map.
     * @param destination the target map.
     * @param map the map type.
     * @return the map with all the row key/value pairs in the result set added to it.
     */
    suspend fun <M: MutableMap<in K, in V>> toMap(destination: M): M
  }

}
