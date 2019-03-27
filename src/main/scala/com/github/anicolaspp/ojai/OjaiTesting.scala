package com.github.anicolaspp.ojai

import com.mapr.ojai.store.impl.InMemoryDriver
import org.ojai.store.{Connection, DocumentStore, DriverManager}

trait OjaiTesting {

  lazy val connection: Connection = {
    DriverManager.registerDriver(InMemoryDriver)

    DriverManager.getConnection("ojai:anicolaspp:mem")
  }

  def storeHandler(storeName: String): DocumentStore = connection.getStore(storeName)
}