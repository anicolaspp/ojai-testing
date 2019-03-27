package com.mapr.ojai.store.impl

import java.util

import org.ojai.store._
import org.ojai.{Document, DocumentBuilder}

class InMemoryConnection(driver: Driver) extends Connection {
  override def getValueBuilder: ValueBuilder = driver.getValueBuilder

  override def newDocument(): Document = driver.newDocument()

  override def newDocument(jsonString: String): Document = driver.newDocument(jsonString)

  override def newDocument(map: util.Map[String, AnyRef]): Document = driver.newDocument(map)

  override def newDocument(bean: Any): Document = driver.newDocument(bean)

  override def newDocumentBuilder(): DocumentBuilder = driver.newDocumentBuilder()

  override def newMutation(): DocumentMutation = driver.newMutation()

  override def newCondition(): QueryCondition = driver.newCondition()

  override def newQuery(): Query = driver.newQuery()

  override def newQuery(queryJson: String): Query = driver.newQuery(queryJson)

  override def getDriver: Driver = driver

  override def close(): Unit = {}

  override def getStore(storeName: String): DocumentStore = {
    if (!storeName.startsWith("anicolaspp")){
      throw new IllegalArgumentException("storeName should be anicolaspp")
    }

    val store = new InMemoryStore(storeName)

    storeRegistry.putStore(storeName, store)

    storeRegistry.getStore(storeName).get
  }

  private val storeRegistry = StoreRegistry()
}




