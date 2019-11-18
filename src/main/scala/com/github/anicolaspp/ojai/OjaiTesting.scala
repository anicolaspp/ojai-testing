package com.github.anicolaspp.ojai

import com.mapr.ojai.store.impl.InMemoryDriver
import org.ojai.store.{Connection, DocumentStore, DriverManager}

trait OjaiTesting {

  def getConnection(): Connection

  def documentStore(storeName: String): DocumentStore
}

trait ScalaOjaiTesting extends OjaiTesting {

  private lazy val connection: Connection = {
    DriverManager.registerDriver(InMemoryDriver)

    DriverManager.getConnection("ojai:anicolaspp:mem")
  }

  override def getConnection(): Connection = connection

  override def documentStore(storeName: String): DocumentStore = connection.getStore(storeName)
}