package info.jdavid.sql

import java.net.SocketAddress

interface Credentials<C: Connection<C>> {
  suspend fun connectTo(database: String,
                        address: SocketAddress): C
}
