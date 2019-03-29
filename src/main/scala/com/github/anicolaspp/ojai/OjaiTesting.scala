package com.github.anicolaspp.ojai

import com.mapr.ojai.store.impl.InMemoryDriver
import org.ojai.store.{Connection, DocumentStore, DriverManager}

trait OjaiTesting {

  def connection: Connection = {
    DriverManager.registerDriver(InMemoryDriver)

    DriverManager.getConnection("ojai:anicolaspp:mem")
  }

  def documentStore(storeName: String): DocumentStore = connection.getStore(storeName)
}