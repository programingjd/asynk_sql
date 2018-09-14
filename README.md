![jcenter](https://img.shields.io/badge/_jcenter_-0.0.0.13-6688ff.png?style=flat)
# Asynk SQL
Interfaces for SQL async client with suspend functions for kotlin coroutines.


## Usage ##

__Fetching rows__

```kotlin
val total: Int = 
  connection.values("SELECT COUNT(*) as count FROM table;", "count").use {
    it.iterator().next() // only one row
  }

val max = 123
val count: Int =
  connection.values(
    "SELECT COUNT(*) as count FROM table WHERE id > ?;",
    listOf(max),
    "count"
  ).use {
    it.iterator().next() // only one value
  }

val ids = ArrayList<Int>(count)
connection.values("SELECT * FROM table WHERE id > ?", listOf(max), "id").use { it.toList(ids) }

val names = Map<Int,String?>(count)
connection.entries("SELECT id, name FROM table", "id", "name").use { it.toMap(map) }

val json = com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(
  connection.rows("SELECT * FROM table").use { it.toList(ids) }
)
```

__Updating rows__

```kotlin
println(
  connection.affectedRows("UPDATE table SET key = ? WHERE id = ?", listOf("a", 1))
)

println(
  connection.prepare("DELETE FROM table WHERE id = ?").use { statement ->
    listOf(1, 3, 4, 6, 9).count { id ->
      statement.affectedRows(listOf(id)) == 1
    }
  }
)
```                                                                                             
