package info.jdavid.asynk.sql

/**
 * An ACloseable (Async Closeable) is a source or destination of data that can be closed.
 * The suspending aClose method is invoked to release resources that the object is holding.
 */
interface ACloseable {
  suspend fun aClose()
}

/**
 * Executes the given [block] function on this resource and then closes it down correctly whether an exception
 * is thrown or not.
 * @param block a function to process this [ACloseable] resource.
 * @return the result of [block] function invoked on this resource.
 */
suspend inline fun <T : ACloseable?, R> T.use(block: (T) -> R): R {
  var exception: Throwable? = null
  try {
    return block(this)
  } catch (e: Throwable) {
    exception = e
    throw e
  } finally {
    when {
      this == null -> {}
      exception == null -> aClose()
      else ->
        try {
          aClose()
        } catch (closeException: Throwable) {
          exception.addSuppressed(closeException)
        }
    }
  }
}
