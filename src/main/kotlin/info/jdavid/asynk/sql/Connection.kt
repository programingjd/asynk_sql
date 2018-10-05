package info.jdavid.asynk.sql

import info.jdavid.asynk.core.AsyncCloseable
import info.jdavid.asynk.core.internal.use
import kotlinx.coroutines.channels.ChannelIterator
import java.io.Closeable

/**
 * Common interface for an SQL database connection.
 */
interface Connection<C: Connection<C>>: AsyncCloseable {

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
  @Suppress("TooGenericExceptionCaught")
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
  interface PreparedStatement<C: Connection<C>>: AsyncCloseable {
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

/**
 * Appends all elements yielded from results of [transform] function being invoked on each element of
 * the result set, to the given [destination].
 *
 * The operation is _terminal_.
 */
suspend inline fun <T,R,C: MutableCollection<in R>> Connection.ResultSet<T>.flatMapTo(
  destination: C, transform: (T) -> Sequence<R>
): C {
  for (element in this) {
    val list = transform(element)
    destination.addAll(list)
  }
  return destination
}

/**
 * Returns a [Map] containing key-value pairs provided by [transform] function applied to elements of
 * the result set.
 *
 * If any of two pairs would have the same key the last one gets added to the map.
 *
 * The returned map preserves the entry iteration order of the result set.
 *
 * The operation is _terminal_.
 */
suspend inline fun <T,K,V> Connection.ResultSet<T>.associate(transform: (T) -> Pair<K,V>) =
  associateTo(LinkedHashMap(), transform)

/**
 * Populates and returns the [destination] mutable map with key-value pairs provided by [transform]
 * function applied to each element of the result set.
 *
 * If any of two pairs would have the same key the last one gets added to the map.
 *
 * The operation is _terminal_.
 */
suspend inline fun <T,K,V,M: MutableMap<in K, in V>> Connection.ResultSet<T>.associateTo(
  destination: M, transform: (T) -> Pair<K,V>
): M {
  for (element in this) {
    destination += transform(element)
  }
  return destination
}

/**
 * Returns a [Map] containing the elements from the result set indexed by the key
 * returned from [keySelector] function applied to each element.
 *
 * If any two elements would have the same key returned by [keySelector] the last one gets added to
 * the map.
 *
 * The returned map preserves the entry iteration order of the result set.
 *
 * The operation is _terminal_.
 */
suspend inline fun <T,K> Connection.ResultSet<T>.associateBy(keySelector: (T) -> K) =
  associateByTo(LinkedHashMap(), keySelector)

/**
 * Populates and returns the [destination] mutable map with key-value pairs,
 * where key is provided by the [keySelector] function applied to each element of the result set and
 * value is the element itself.
 *
 * If any two elements would have the same key returned by [keySelector] the last one gets added to
 * the map.
 *
 * The operation is _terminal_.
 */
suspend inline fun <T,K,M: MutableMap<in K, in T>> Connection.ResultSet<T>.associateByTo(
  destination: M, keySelector: (T) -> K
): M {
  for (element in this) {
    destination.put(keySelector(element), element)
  }
  return destination
}

/**
 * Returns a [Map] containing the values provided by [valueTransform] and indexed by [keySelector]
 * functions applied to elements of the result set.
 *
 * If any two elements would have the same key returned by [keySelector] the last one gets added to
 * the map.
 *
 * The returned map preserves the entry iteration order of the result set.
 *
 * The operation is _terminal_.
 */
suspend inline fun <T,K,V> Connection.ResultSet<T>.associateBy(
  keySelector: (T) -> K, valueTransform: (T) -> V
) = associateByTo(LinkedHashMap(), keySelector, valueTransform)

/**
 * Populates and returns the [destination] mutable map with key-value pairs, where key is provided
 * by the [keySelector] function and value is provided by the [valueTransform] function applied to
 * elements of the result set.
 *
 * If any two elements would have the same key returned by [keySelector] the last one gets added to
 * the map.
 *
 * The operation is _terminal_.
 */
suspend inline fun <T,K,V,M : MutableMap<in K, in V>> Connection.ResultSet<T>.associateByTo(
  destination: M, keySelector: (T) -> K, valueTransform: (T) -> V
): M {
  for (element in this) {
    destination.put(keySelector(element), valueTransform(element))
  }
  return destination
}

/**
 * Accumulates value starting with [initial] value and applying [operation] from left to right to
 * current accumulator value and each element.
 *
 * The operation is _terminal_.
 */
suspend inline fun <T,R> Connection.ResultSet<T>.fold(initial: R, operation: (acc: R, T) -> R): R {
  var accumulator = initial
  for (element in this) accumulator = operation(accumulator, element)
  return accumulator
}

/**
 * Accumulates value starting with [initial] value and applying [operation] from left to right
 * to current accumulator value and each element with its index in the result set.
 * @param [operation] function that takes the index of an element, current accumulator value
 * and the element itself, and calculates the next accumulator value.
 *
 * The operation is _terminal_.
 */
suspend inline fun <T,R> Connection.ResultSet<T>.foldIndexed(
  initial: R, operation: (index: Int, acc: R, T) -> R
): R {
  var index = 0
  var accumulator = initial
  for (element in this) accumulator = operation(index++, accumulator, element)
  return accumulator
}

/**
 * Performs the given [action] on each element.
 *
 * The operation is _terminal_.
 */
suspend inline fun <T> Connection.ResultSet<T>.forEach(action: (T) -> Unit) {
  for (element in this) action(element)
}

/**
 * Performs the given [action] on each element, providing sequential index with the element.
 * @param [action] function that takes the index of an element and the element itself
 * and performs the desired action on the element.
 *
 * The operation is _terminal_.
 */
suspend inline fun <T> Connection.ResultSet<T>.forEachIndexed(action: (index: Int, T) -> Unit) {
  var index = 0
  for (item in this) action(index++, item)
}

/**
 * Groups elements of the result set by the key returned by the given [keySelector] function applied to each
 * element and returns a map where each group key is associated with a list of corresponding elements.
 *
 * The returned map preserves the entry iteration order of the keys produced from the result set.
 *
 * The operation is _terminal_.
 */
suspend inline fun <T,K> Connection.ResultSet<T>.groupBy(keySelector: (T) -> K) =
  groupByTo(LinkedHashMap(), keySelector)

/**
 * Groups values returned by the [valueTransform] function applied to each element of the result set
 * by the key returned by the given [keySelector] function applied to the element
 * and returns a map where each group key is associated with a list of corresponding values.
 *
 * The returned map preserves the entry iteration order of the keys produced from the result set.
 *
 * The operation is _terminal_.
 */
suspend inline fun <T,K,V> Connection.ResultSet<T>.groupBy(keySelector: (T) -> K, valueTransform: (T) -> V) =
  groupByTo(LinkedHashMap(), keySelector, valueTransform)

/**
 * Groups elements of the result set by the key returned by the given [keySelector] function applied to each
 * element and puts to the [destination] map each group key associated with a list of corresponding elements.
 *
 * @return The [destination] map.
 *
 * The operation is _terminal_.
 */
suspend inline fun <T,K,M: MutableMap<in K, MutableList<T>>> Connection.ResultSet<T>.groupByTo(
  destination: M, keySelector: (T) -> K
): M {
  for (element in this) {
    val key = keySelector(element)
    val list = destination.getOrPut(key) { ArrayList<T>() }
    list.add(element)
  }
  return destination
}

/**
 * Groups values returned by the [valueTransform] function applied to each element of the result set by the
 * key returned by the given [keySelector] function applied to the element and puts to the [destination]
 * map each group key associated with a list of corresponding values.
 *
 * @return The [destination] map.
 *
 * The operation is _terminal_.
 */
suspend inline fun <T,K,V,M: MutableMap<in K, MutableList<V>>> Connection.ResultSet<T>.groupByTo(
  destination: M, keySelector: (T) -> K, valueTransform: (T) -> V
): M {
  for (element in this) {
    val key = keySelector(element)
    val list = destination.getOrPut(key) { ArrayList() }
    list.add(valueTransform(element))
  }
  return destination
}

/**
 * Applies the given [transform] function to each element of the result set and appends the results to the
 * given [destination].
 *
 * The operation is _terminal_.
 */
suspend inline fun <T,R,C: MutableCollection<in R>> Connection.ResultSet<T>.mapTo(
  destination: C, transform: (T) -> R
): C {
  for (item in this)
    destination.add(transform(item))
  return destination
}

/**
 * Applies the given [transform] function to each element and its index in the result set and appends the
 * results to the given [destination].
 * @param [transform] function that takes the index of an element and the element itself
 * and returns the result of the transform applied to the element.
 *
 * The operation is _terminal_.
 */
suspend inline fun <T,R,C: MutableCollection<in R>> Connection.ResultSet<T>.mapIndexedTo(
  destination: C, transform: (index: Int, T) -> R
): C {
  var index = 0
  for (item in this)
    destination.add(transform(index++, item))
  return destination
}

/**
 * Accumulates value starting with the first element and applying [operation] from left to right to current
 * accumulator value and each element.
 *
 * The operation is _terminal_.
 */
suspend inline fun <S,T:S> Connection.ResultSet<T>.reduce(operation: (acc: S, T) -> S): S {
  val iterator = this.iterator()
  if (!iterator.hasNext()) throw UnsupportedOperationException("Empty sequence can't be reduced.")
  var accumulator: S = iterator.next()
  while (iterator.hasNext()) {
    accumulator = operation(accumulator, iterator.next())
  }
  return accumulator
}

/**
 * Accumulates value starting with the first element and applying [operation] from left to right to current
 * accumulator value and each element with its index in the result set.
 * @param [operation] function that takes the index of an element, current accumulator value
 * and the element itself and calculates the next accumulator value.
 *
 * The operation is _terminal_.
 */
suspend inline fun <S,T:S> Connection.ResultSet<T>.reduceIndexed(operation: (index: Int, acc: S, T) -> S): S {
  val iterator = this.iterator()
  if (!iterator.hasNext()) throw UnsupportedOperationException("Empty sequence can't be reduced.")
  var index = 1
  var accumulator: S = iterator.next()
  while (iterator.hasNext()) {
    accumulator = operation(index++, accumulator, iterator.next())
  }
  return accumulator
}

suspend inline fun <T : Connection<*>?, R> T.use(block: (T) -> R): R {
  return use(this) { block(this) }
}

suspend inline fun <T : Connection.PreparedStatement<*>?, R> T.use(block: (T) -> R): R {
  return use(this) { block(this) }
}
