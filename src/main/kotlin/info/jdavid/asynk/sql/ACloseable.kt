package info.jdavid.asynk.sql

interface ACloseable {
  suspend fun aClose()
}

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
