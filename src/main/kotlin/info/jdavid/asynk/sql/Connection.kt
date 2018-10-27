@file:Suppress("unused")
package info.jdavid.asynk.sql

import info.jdavid.asynk.core.AsyncCloseable
import info.jdavid.asynk.core.internal.use
import kotlinx.coroutines.channels.ChannelIterator

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
   * @param batchSize the fetch batch size.
   * @return the result set.
   */
  suspend fun rows(sqlStatement: String,
                   batchSize: Int = 1024): ResultSet<Map<String,Any?>>
  /**
   * Executes a row query SQL statement and returns each row as a Map<String,Any?> where the key is the column
   * name or alias and the value is the column value for that row.<br>
   * Query parameters (the "?" syntax should be used) are bound to the specified values.
   * @param sqlStatement the SQL query statement.
   * @param params the values to bind to the query parameters.
   * @param batchSize the fetch batch size.
   * @return the result set.
   */
  suspend fun rows(sqlStatement: String, params: Iterable<Any?>,
                   batchSize: Int = 1024): ResultSet<Map<String,Any?>>
  /**
   * Executes a row query prepared statement and returns each row as Map<String,Any?> where the key is the
   * column name or alias and the value is the column value for that row.
   * @param preparedStatement the SQL prepared statement.
   * @param batchSize the fetch batch size.
   * @return the result set.
   */
  suspend fun rows(preparedStatement: PreparedStatement<C>,
                   batchSize: Int = 1024): ResultSet<Map<String,Any?>>
  /**
   * Executes a row query prepared statement and returns each row as a Map<String,Any?> where the key is the
   * column name or alias and the value is the column value for that row.<br>
   * Query parameters (the "?" syntax should be used) are bound to the specified values.
   * @param preparedStatement the SQL prepared statement.
   * @param params the values to bind to the query parameters.
   * @param batchSize the fetch batch size.
   * @return the result set.
   */
  suspend fun rows(preparedStatement: PreparedStatement<C>,
                   params: Iterable<Any?>,
                   batchSize: Int = 1024): ResultSet<Map<String,Any?>>

  /**
   * Executes a row query SQL statement and returns the value of the specified column for each row.
   * @param sqlStatement the SQL query statement.
   * @param columnNameOrAlias the name or alias of the column to get the value of.
   * @param batchSize the fetch batch size.
   * @param T the value type.
   * @return the result set.
   */
  suspend fun <T> values(sqlStatement: String,
                         columnNameOrAlias: String,
                         batchSize: Int = 1024): ResultSet<T>
  /**
   * Executes a row query SQL statement and returns the value of the specified column for each row.<br>
   * Query parameters (the "?" syntax should be used) are bound to the specified values.
   * @param sqlStatement the SQL query statement.
   * @param params the values to bind to the query parameters.
   * @param columnNameOrAlias the name or alias of the column to get the value of.
   * @param batchSize the fetch batch size.
   * @param T the value type.
   * @return the result set.
   */
  suspend fun <T> values(sqlStatement: String, params: Iterable<Any?>,
                         columnNameOrAlias: String,
                         batchSize: Int = 1024): ResultSet<T>
  /**
   * Executes a row query prepared statement and returns the value of the specified column for each row.
   * @param preparedStatement the SQL prepared statement.
   * @param columnNameOrAlias the name or alias of the column to get the value of.
   * @param batchSize the fetch batch size.
   * @param T the value type.
   * @return the result set.
   */
  suspend fun <T> values(preparedStatement: PreparedStatement<C>,
                         columnNameOrAlias: String,
                         batchSize: Int = 1024): ResultSet<T>
  /**
   * Executes a row query prepared statement and returns the value of the specified column for each row.<br>.
   * Query parameters (the "?" syntax should be used) are bound to the specified values.
   * @param preparedStatement the SQL prepared statement.
   * @param params the values to bind to the query parameters.
   * @param columnNameOrAlias the name or alias of the column to get the value of.
   * @param batchSize the fetch batch size.
   * @param T the value type.
   * @return the result set.
   */
  suspend fun <T> values(preparedStatement: PreparedStatement<C>,
                         params: Iterable<Any?>,
                         columnNameOrAlias: String,
                         batchSize: Int = 1024): ResultSet<T>

  /**
   * Executes a row query SQL statement and returns a key/value pair of the specified columns for each row.
   * @param sqlStatement the SQL query statement.
   * @param keyColumnNameOrAlias the name or alias of the column to get the key value of.
   * @param valueColumnNameOrAlias the name or alias of the column to get the value of.
   * @param batchSize the fetch batch size.
   * @param K the key type.
   * @param V the value type.
   * @return the result set.
   */
  suspend fun <K,V> entries(sqlStatement: String,
                            keyColumnNameOrAlias: String,
                            valueColumnNameOrAlias: String,
                            batchSize: Int = 1024): ResultMap<K,V>
  /**
   * Executes a row query SQL statement and returns a key/value pair of the specified columns for
   * each row.<br>
   * Query parameters (the "?" syntax should be used) are bound to the specified values.
   * @param sqlStatement the SQL query statement.
   * @param params the values to bind to the query parameters.
   * @param keyColumnNameOrAlias the name or alias of the column to get the key value of.
   * @param valueColumnNameOrAlias the name or alias of the column to get the value of.
   * @param batchSize the fetch batch size.
   * @param K the key type.
   * @param V the value type.
   * @return the result set.
   */
  suspend fun <K,V> entries(sqlStatement: String,
                            params: Iterable<Any?>,
                            keyColumnNameOrAlias: String,
                            valueColumnNameOrAlias: String,
                            batchSize: Int = 1024): ResultMap<K,V>
  /**
   * Executes a row query prepared statement and returns a key/value pair of the specified columns for
   * each row.
   * @param preparedStatement the SQL prepared statement.
   * @param keyColumnNameOrAlias the name or alias of the column to get the key value of.
   * @param valueColumnNameOrAlias the name or alias of the column to get the value of.
   * @param batchSize the fetch batch size.
   * @param K the key type.
   * @param V the value type.
   * @return the result set.
   */
  suspend fun <K,V> entries(preparedStatement: PreparedStatement<C>,
                            keyColumnNameOrAlias: String,
                            valueColumnNameOrAlias: String,
                            batchSize: Int = 1024): ResultMap<K,V>
  /**
   * Executes a row query prepared statement and returns a key/value pair of the specified columns for
   * each row.<br>
   * Query parameters (the "?" syntax should be used) are bound to the specified values.
   * @param preparedStatement the SQL prepared statement.
   * @param params the values to bind to the query parameters.
   * @param keyColumnNameOrAlias the name or alias of the column to get the key value of.
   * @param valueColumnNameOrAlias the name or alias of the column to get the value of.
   * @param batchSize the fetch batch size.
   * @param K the key type.
   * @param V the value type.
   * @return the result set.
   */
  suspend fun <K,V> entries(preparedStatement: PreparedStatement<C>,
                            params: Iterable<Any?>,
                            keyColumnNameOrAlias: String,
                            valueColumnNameOrAlias: String,
                            batchSize: Int = 1024): ResultMap<K,V>


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
  suspend fun affectedRows(preparedStatement: PreparedStatement<C>, params: Iterable<Any?>): Int

  /**
   * Common interface for SQL Prepared Statements. The "?" syntax should be used for binding parameters.
   */
  interface PreparedStatement<C: Connection<C>>: AsyncCloseable {
    /**
     * Executes this row query prepared statement and returns each row as a Map<String,Any?> where the key
     * is the column name or alias and the value is the column value for that row.
     * @param batchSize the fetch batch size.
     * @return the result set.
     */
    suspend fun rows(batchSize: Int = 1024): ResultSet<Map<String,Any?>>
    /**
     * Executes this row query prepared statement and returns each row as a Map<String,Any?> where the key
     * is the column name or alias and the value is the column value for that row.<br>
     * Query parameters (the "?" syntax should be used) are bound to the specified values.
     * @param params the values to bind to the query parameters.
     * @param batchSize the fetch batch size.
     * @return the result set.
     */
    suspend fun rows(params: Iterable<Any?>, batchSize: Int = 1024): ResultSet<Map<String,Any?>>
    /**
     * Executes this row query prepared statement and returns the value of the specified column for each row.
     * @param columnNameOrAlias the name or alias of the column to get the value of.
     * @param batchSize the fetch batch size.
     * @param T the value type.
     * @return the result set.
     */
    suspend fun <T> values(columnNameOrAlias: String, batchSize: Int = 1024): ResultSet<T>
    /**
     * Executes this row query prepared statement and returns the value of the specified column for each row.
     * <br>Query parameters (the "?" syntax should be used) are bound to the specified values.
     * @param params the values to bind to the query parameters.
     * @param columnNameOrAlias the name or alias of the column to get the value of.
     * @param batchSize the fetch batch size.
     * @param T the value type.
     * @return the result set.
     */
    suspend fun <T> values(params: Iterable<Any?>,
                           columnNameOrAlias: String,
                           batchSize: Int = 1024): ResultSet<T>
    /**
     * Executes this row query prepared statement and returns a key/value pair of the specified columns for
     * each row.
     * @param keyColumnNameOrAlias the name or alias of the column to get the key value of.
     * @param valueColumnNameOrAlias the name or alias of the column to get the value of.
     * @param batchSize the fetch batch size.
     * @param K the key type.
     * @param V the value type.
     * @return the result set.
     */
    suspend fun <K,V> entries(keyColumnNameOrAlias: String,
                              valueColumnNameOrAlias: String,
                              batchSize: Int = 1024): ResultMap<K,V>
    /**
     * Executes this row query prepared statement and returns a key/value pair of the specified columns for
     * each row.<br>
     * Query parameters (the "?" syntax should be used) are bound to the specified values.
     * @param params the values to bind to the query parameters.
     * @param keyColumnNameOrAlias the name or alias of the column to get the key value of.
     * @param valueColumnNameOrAlias the name or alias of the column to get the value of.
     * @param batchSize the fetch batch size.
     * @param K the key type.
     * @param V the value type.
     * @return the result set.
     */
    suspend fun <K,V> entries(params: Iterable<Any?>,
                              keyColumnNameOrAlias: String,
                              valueColumnNameOrAlias: String,
                              batchSize: Int = 1024): ResultMap<K,V>
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
  interface ResultSet<T>: AsyncCloseable {
    /**
     * Executes the specified lambda on an iterator and then closes the result set.
     */
    suspend fun <R> iterate(block: suspend (ChannelIterator<T>) -> R): R

  }

  /**
   * Result set whose values are pairs of key,values.
   * @param K the data type of the key returned for each row.
   * @param V the data type of the value returned for each row.
   */
  interface ResultMap<K,V>: ResultSet<Pair<K,V>>

}

/**
 * Fetches all the rows and returns their values as a list, and then closes the result set.
 * @return a list with all the row values in the result set.
 */
suspend inline fun <T> Connection.ResultSet<T>.toList(): List<T> {
  return toMutableList()
}

/**
 * Fetches all the rows and returns their values as a mutable list, and then closes the result set.
 * @return a mutable list with all the row values in the result set.
 */
suspend inline fun <T> Connection.ResultSet<T>.toMutableList(): MutableList<T> {
  return toCollection(ArrayList())
}

/**
 * Fetches all the rows and adds their values to the supplied collection, and then closes the result set.
 * @param destination the target collection.
 * @param C the collection type.
 * @return the collection with all the row values in the result set added to it.
 */
suspend inline fun <T,C: MutableCollection<in T>> Connection.ResultSet<T>.toCollection(destination: C): C {
  return iterate {
    while (it.hasNext()) destination.add(it.next())
    destination
  }
}

/**
 * Appends all elements yielded from results of [transform] function being invoked on each element of
 * the result set, to the given [destination], and then closes the result set.
 *
 * The operation is _terminal_.
 */
suspend inline fun <T,R,C: MutableCollection<in R>> Connection.ResultSet<T>.flatMapTo(
  destination: C, crossinline transform: (T) -> Sequence<R>
): C {
  return iterate {
    while(it.hasNext()) {
      val list = transform(it.next())
      destination.addAll(list)
    }
    destination
  }
}

/**
 * Returns a [Map] containing key-value pairs provided by [transform] function applied to elements of
 * the result set, and then closes the result set.
 *
 * If any of two pairs would have the same key the last one gets added to the map.
 *
 * The returned map preserves the entry iteration order of the result set.
 *
 * The operation is _terminal_.
 */
suspend inline fun <T,K,V> Connection.ResultSet<T>.associate(crossinline transform: (T) -> Pair<K,V>) =
  associateTo(LinkedHashMap(), transform)

/**
 * Populates and returns the [destination] mutable map with key-value pairs provided by [transform]
 * function applied to each element of the result set, and then closes the result set.
 *
 * If any of two pairs would have the same key the last one gets added to the map.
 *
 * The operation is _terminal_.
 */
suspend inline fun <T,K,V,M: MutableMap<in K, in V>> Connection.ResultSet<T>.associateTo(
  destination: M, crossinline transform: (T) -> Pair<K,V>
): M {
  return iterate {
    while (it.hasNext()) {
      destination += transform(it.next())
    }
    destination
  }
}

/**
 * Returns a [Map] containing the elements from the result set indexed by the key
 * returned from [keySelector] function applied to each element, and then closes the result set.
 *
 * If any two elements would have the same key returned by [keySelector], the last one gets added to
 * the map.
 *
 * The returned map preserves the entry iteration order of the result set.
 *
 * The operation is _terminal_.
 */
suspend inline fun <T,K> Connection.ResultSet<T>.associateBy(crossinline keySelector: (T) -> K) =
  associateByTo(LinkedHashMap(), keySelector)

/**
 * Populates and returns the [destination] mutable map with key-value pairs and then closes the result set.
 * The key is provided by the [keySelector] function applied to each element of the result set and
 * value is the element itself.
 *
 * If any two elements would have the same key returned by [keySelector], the last one gets added to
 * the map.
 *
 * The operation is _terminal_.
 */
suspend inline fun <T,K,M: MutableMap<in K, in T>> Connection.ResultSet<T>.associateByTo(
  destination: M, crossinline keySelector: (T) -> K
): M {
  return iterate {
    while (it.hasNext()) {
      val element = it.next()
      destination.put(keySelector(element), element)
    }
    destination
  }
}

/**
 * Returns a [Map] containing the values provided by [valueTransform] and indexed by [keySelector]
 * functions applied to elements of the result set, and then closes the result set.
 *
 * If any two elements would have the same key returned by [keySelector] the last one gets added to
 * the map.
 *
 * The returned map preserves the entry iteration order of the result set.
 *
 * The operation is _terminal_.
 */
suspend inline fun <T,K,V> Connection.ResultSet<T>.associateBy(
  crossinline keySelector: (T) -> K, crossinline valueTransform: (T) -> V
) = associateByTo(LinkedHashMap(), keySelector, valueTransform)

/**
 * Populates and returns the [destination] mutable map with key-value pairs and then closes the result set.
 * The key is provided by the [keySelector] function and value is provided by the [valueTransform] function
 * applied to elements of the result set.
 *
 * If any two elements would have the same key returned by [keySelector] the last one gets added to
 * the map.
 *
 * The operation is _terminal_.
 */
suspend inline fun <T,K,V,M : MutableMap<in K, in V>> Connection.ResultSet<T>.associateByTo(
  destination: M, crossinline keySelector: (T) -> K, crossinline valueTransform: (T) -> V
): M {
  return iterate {
    while (it.hasNext()) {
      val element = it.next()
      destination.put(keySelector(element), valueTransform(element))
    }
    destination
  }
}

/**
 * Accumulates value starting with [initial] value and applying [operation] from left to right to
 * current accumulator value and each element, and then closes the result set.
 *
 * The operation is _terminal_.
 */
suspend inline fun <T,R> Connection.ResultSet<T>.fold(initial: R, crossinline operation: (acc: R,T) -> R): R {
  return iterate {
    var accumulator = initial
    while (it.hasNext()) {
      val element = it.next()
      accumulator = operation(accumulator, element)
    }
    accumulator
  }
}

/**
 * Accumulates value starting with [initial] value and applying [operation] from left to right
 * to current accumulator value and each element with its index in the result set, and then closes the
 * result set.
 * @param [operation] function that takes the index of an element, current accumulator value
 * and the element itself, and calculates the next accumulator value.
 *
 * The operation is _terminal_.
 */
suspend inline fun <T,R> Connection.ResultSet<T>.foldIndexed(
  initial: R, crossinline operation: (index: Int, acc: R, T) -> R
): R {
  return iterate {
    var index = 0
    var accumulator = initial
    while (it.hasNext()) {
      accumulator = operation(index++, accumulator, it.next())
    }
    accumulator
  }
}

/**
 * Performs the given [action] on each element, and then closes the result set.
 *
 * The operation is _terminal_.
 */
suspend inline fun <T> Connection.ResultSet<T>.forEach(crossinline action: (T) -> Unit) {
  iterate { while(it.hasNext()) action(it.next()) }
}

/**
 * Performs the given [action] on each element, providing sequential index with the element, and then
 * closes the result set.
 * @param [action] function that takes the index of an element and the element itself
 * and performs the desired action on the element.
 *
 * The operation is _terminal_.
 */
suspend inline fun <T> Connection.ResultSet<T>.forEachIndexed(crossinline action: (index: Int, T) -> Unit) {
  iterate {
    var index = 0
    iterate { while(it.hasNext()) action(index++, it.next()) }
  }
}

/**
 * Groups elements of the result set by the key returned by the given [keySelector] function applied to each
 * element and returns a map where each group key is associated with a list of corresponding elements, and
 * then closes the result set.
 *
 * The returned map preserves the entry iteration order of the keys produced from the result set.
 *
 * The operation is _terminal_.
 */
suspend inline fun <T,K> Connection.ResultSet<T>.groupBy(crossinline keySelector: (T) -> K) =
  groupByTo(LinkedHashMap(), keySelector)

/**
 * Groups values returned by the [valueTransform] function applied to each element of the result set
 * by the key returned by the given [keySelector] function applied to the element
 * and returns a map where each group key is associated with a list of corresponding values, and then closes
 * the result set.
 *
 * The returned map preserves the entry iteration order of the keys produced from the result set.
 *
 * The operation is _terminal_.
 */
suspend inline fun <T,K,V> Connection.ResultSet<T>.groupBy(
  crossinline keySelector: (T) -> K, crossinline valueTransform: (T) -> V
) = groupByTo(LinkedHashMap(), keySelector, valueTransform)

/**
 * Groups elements of the result set by the key returned by the given [keySelector] function applied to each
 * element and puts to the [destination] map each group key associated with a list of corresponding elements,
 * and then closes the result set.
 *
 * @return The [destination] map.
 *
 * The operation is _terminal_.
 */
suspend inline fun <T,K,M: MutableMap<in K, MutableList<T>>> Connection.ResultSet<T>.groupByTo(
  destination: M, crossinline keySelector: (T) -> K
): M {
  return iterate {
    while (it.hasNext()) {
      val element = it.next()
      val key = keySelector(element)
      val list = destination.getOrPut(key) { ArrayList() }
      list.add(element)
    }
    destination
  }
}

/**
 * Groups values returned by the [valueTransform] function applied to each element of the result set by the
 * key returned by the given [keySelector] function applied to the element and puts to the [destination]
 * map each group key associated with a list of corresponding values, and then closes the result set.
 *
 * @return The [destination] map.
 *
 * The operation is _terminal_.
 */
suspend inline fun <T,K,V,M: MutableMap<in K, MutableList<V>>> Connection.ResultSet<T>.groupByTo(
  destination: M, crossinline keySelector: (T) -> K, crossinline valueTransform: (T) -> V
): M {
  return iterate {
    while (it.hasNext()) {
      val element = it.next()
      val key = keySelector(element)
      val list = destination.getOrPut(key) { ArrayList() }
      list.add(valueTransform(element))
    }
    destination
  }
}

/**
 * Applies the given [transform] function to each element of the result set and appends the results to the
 * given [destination], and then closes the result set.
 *
 * The operation is _terminal_.
 */
suspend inline fun <T,R,C: MutableCollection<in R>> Connection.ResultSet<T>.mapTo(
  destination: C, crossinline transform: (T) -> R
): C {
  return iterate {
    while (it.hasNext()) destination.add(transform(it.next()))
    destination
  }
}

/**
 * Applies the given [transform] function to each element and its index in the result set and appends the
 * results to the given [destination], and then closes the result set.
 * @param [transform] function that takes the index of an element and the element itself
 * and returns the result of the transform applied to the element.
 *
 * The operation is _terminal_.
 */
suspend inline fun <T,R,C: MutableCollection<in R>> Connection.ResultSet<T>.mapIndexedTo(
  destination: C, crossinline transform: (index: Int, T) -> R
): C {
  return iterate {
    var index = 0
    while (it.hasNext()) destination.add(transform(index++, it.next()))
    destination
  }
}

/**
 * Accumulates value starting with the first element and applying [operation] from left to right to current
 * accumulator value and each element, and then closes the result set.
 *
 * The operation is _terminal_.
 */
suspend inline fun <S,T:S> Connection.ResultSet<T>.reduce(crossinline operation: (acc: S, T) -> S): S {
  return iterate {
    if (!it.hasNext()) throw UnsupportedOperationException("Empty sequence can't be reduced.")
    var accumulator: S = it.next()
    while (it.hasNext()) {
      accumulator = operation(accumulator, it.next())
    }
    accumulator
  }
}

/**
 * Accumulates value starting with the first element and applying [operation] from left to right to current
 * accumulator value and each element with its index in the result set, and then closes the result set.
 * @param [operation] function that takes the index of an element, current accumulator value
 * and the element itself and calculates the next accumulator value.
 *
 * The operation is _terminal_.
 */
suspend inline fun <S,T:S> Connection.ResultSet<T>.reduceIndexed(
  crossinline operation: (index: Int, acc: S, T) -> S
): S {
  return iterate {
    if (!it.hasNext()) throw UnsupportedOperationException("Empty sequence can't be reduced.")
    var index = 1
    var accumulator: S = it.next()
    while (it.hasNext()) {
      accumulator = operation(index++, accumulator, it.next())
    }
    accumulator
  }
}

/**
 * Fetches the key/value pairs for all the rows and returns them as a map, and then closes the result set.
 * If the same key is returned more than once, then the last value is retained.
 * @return a map of the row values in the result set.
 */
suspend fun <K,V> Connection.ResultMap<K, V>.toMap(): Map<K,V> {
  return toMutableMap()
}

/**
 * Fetches the key/value pairs for all the rows and returns them as a mutable map, and then closes the
 * result set. If the same key is returned more than once, then the last value is retained.
 * @return a map of the row values in the result set.
 */
suspend fun <K,V> Connection.ResultMap<K, V>.toMutableMap(): MutableMap<K,V> {
  return toMap(LinkedHashMap())
}

/**
 * Fetches the key/value pairs for all the rows and adds them to the supplied map, and then closes the result
 * set.
 * @param destination the target map.
 * @param M the map type.
 * @return the map with all the row key/value pairs in the result set added to it.
 */
suspend fun <K,V,M: MutableMap<in K,in V>> Connection.ResultMap<K, V>.toMap(destination: M): M {
  return iterate {
    while (it.hasNext()) {
      val element = it.next()
      destination.put(element.first, element.second)
    }
    destination
  }
}

suspend inline fun <T : Connection<*>?, R> T.use(block: (T) -> R): R {
  return use(this) { block(this) }
}

suspend inline fun <T : Connection.PreparedStatement<*>?, R> T.use(block: (T) -> R): R {
  return use(this) { block(this) }
}

suspend inline fun <T : Connection.ResultSet<*>?, R> T.use(block: (T) -> R): R {
  return use(this) { block(this) }
}
