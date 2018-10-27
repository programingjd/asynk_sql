package info.jdavid.asynk.sql

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ChannelIterator
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

class ResultSetTests {


  @Test
  fun testToCollection() {
    runBlocking {
      TestResultSet(channel(sequenceOf("a", "b", "c"))).apply {
        assertEquals("a,b,c", toList().joinToString(","))
        assertFalse(iterate { it.hasNext() })
      }
      TestResultSet(channel(sequenceOf("a", "b", "a", "c"))).apply {
        assertEquals("a,b,c", toCollection(LinkedHashSet()).joinToString(","))
        assertFalse(iterate { it.hasNext() })
      }
    }
  }

  @Test
  fun testAssociate() {
    runBlocking {
      TestResultSet(channel(sequenceOf("a", "b", "c"))).apply {
        assertEquals(
          "ka->va,kb->vb,kc->vc",
          associate { "k$it" to "v$it" }.map { "${it.key}->${it.value}" }.joinToString(",")
        )
        assertFalse(iterate { it.hasNext() })
      }
      TestResultSet(channel(sequenceOf("a", "b", "a", "c"))).apply {
        assertEquals(
          "A->a,B->b,C->c",
          associateBy { it.toUpperCase() }.map { "${it.key}->${it.value}" }.joinToString(",")
        )
        assertFalse(iterate { it.hasNext() })
      }
      TestResultSet(channel(sequenceOf("a", "b", "a", "c"))).apply {
        assertEquals(
          "ka->va,kb->vb,kc->vc",
          associateBy({ "k$it" }, { "v$it" }).map { "${it.key}->${it.value}" }.joinToString(",")
        )
        assertFalse(iterate { it.hasNext() })
      }
    }
  }

  @Test
  fun testFlapMap() {
    runBlocking {
      TestResultSet(channel(sequenceOf("abc", "def"))).let { rs ->
        assertEquals(
          "a,b,c,d,e,f",
          mutableListOf<String>().apply {
            rs.flatMapTo(this) { it.chunked(1).asSequence() }
          }.joinToString(",")
        )
        assertFalse(rs.iterate { it.hasNext() })
      }
    }
  }

  @Test
  fun testFold() {
    runBlocking {
      TestResultSet(channel(sequenceOf("abc", "def"))).apply {
        assertEquals(
          "abcdef",
          fold(StringBuilder()) { sb, it -> sb.append(it) }.toString()
        )
        assertFalse(iterate { it.hasNext() })
      }
      TestResultSet(channel(sequenceOf("a", "b", "c", "d", "e", "f"))).apply {
        assertEquals(
          "1a2b3c4d5e6f",
          foldIndexed(StringBuilder()) { i, sb, it -> sb.append(i+1).append(it) }.toString()
        )
        assertFalse(iterate { it.hasNext() })
      }
    }
  }

  @Test
  fun testForEach() {
    runBlocking {
      TestResultSet(channel(sequenceOf("abc", "def"))).apply {
        val sb = StringBuilder()
        forEach { sb.append(it) }
        assertEquals(
          "abcdef",
          sb.toString()
        )
        assertFalse(iterate { it.hasNext() })
      }
      TestResultSet(channel(sequenceOf("a", "b", "c", "d", "e", "f"))).apply {
        val sb = StringBuilder()
        forEachIndexed { i, it -> sb.append(i+1).append(it) }
        assertEquals(
          "1a2b3c4d5e6f",
          sb.toString()
        )
        assertFalse(iterate { it.hasNext() })
      }
    }
  }

  @Test
  fun testGroupBy() {
    runBlocking {
      TestResultSet(channel(sequenceOf("a1", "b2", "a3", "c4"))).apply {
        val groups = groupBy { it.chunked(1).first() }
        assertEquals(3, groups.size)
        groups["a"].let {
          assertNotNull(it)
          assertEquals(2, it?.size)
          assertEquals("a1", it?.first())
          assertEquals("a3", it?.last())
        }
        groups["b"].let {
          assertNotNull(it)
          assertEquals(1, it?.size)
          assertEquals("b2", it?.first())
        }
        groups["c"].let {
          assertNotNull(it)
          assertEquals(1, it?.size)
          assertEquals("c4", it?.first())
        }
        assertFalse(iterate { it.hasNext() })
      }
      TestResultSet(channel(sequenceOf("a1", "b2", "a3", "c4"))).apply {
        val groups = groupBy({ it.chunked(1).first() }, { it.chunked(1).last() })
        assertEquals(3, groups.size)
        groups["a"].let {
          assertNotNull(it)
          assertEquals(2, it?.size)
          assertEquals("1", it?.first())
          assertEquals("3", it?.last())
        }
        groups["b"].let {
          assertNotNull(it)
          assertEquals(1, it?.size)
          assertEquals("2", it?.first())
        }
        groups["c"].let {
          assertNotNull(it)
          assertEquals(1, it?.size)
          assertEquals("4", it?.first())
        }
        assertFalse(iterate { it.hasNext() })
      }
    }
  }

  @Test
  fun testMap() {
    runBlocking {
      TestResultSet(channel(sequenceOf("abc", "def"))).apply {
        assertEquals(
          "ABCDEF",
          mapTo(mutableListOf()) { it.toUpperCase() }.joinToString("")
        )
        assertFalse(iterate { it.hasNext() })
      }
      TestResultSet(channel(sequenceOf("a", "b", "c", "d", "e", "f"))).apply {
        assertEquals(
          "1a2b3c4d5e6f",
          mapIndexedTo(mutableListOf()) { i, it -> "${i+1}$it" }.joinToString("")
        )
        assertFalse(iterate { it.hasNext() })
      }
    }
  }

  @Test
  fun testReduce() {
    runBlocking {
      TestResultSet(channel(sequenceOf("abc", "def"))).apply {
        assertEquals(
          "abcdef",
          reduce{ a, b -> a + b }
        )
        assertFalse(iterate { it.hasNext() })
      }
      TestResultSet(channel(sequenceOf("a", "b", "c", "d", "e", "f"))).apply {
        assertEquals(
          "a2b3c4d5e6f",
          reduceIndexed { i, a, b -> a + (i+1) + b }
        )
        assertFalse(iterate { it.hasNext() })
      }
    }
  }

  private suspend fun <T> channel(data: Sequence<T>): ReceiveChannel<T> = Channel<T>(Channel.UNLIMITED).apply {
    coroutineScope {
      data.forEach {
        send(it)
        delay(100)
      }
      close()
    }
  }

  class TestResultSet<T>(private val channel: ReceiveChannel<T>): Connection.ResultSet<T> {
    override suspend fun <R> iterate(block: suspend (ChannelIterator<T>) -> R): R {
      return block(channel.iterator())
    }
    override suspend fun close() {}
  }


}
