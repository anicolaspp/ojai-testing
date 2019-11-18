package com.github.anicolaspp.ojai

import java.util

import com.mapr.db.impl.InMemoryStore
import org.ojai.store._
import org.ojai.{Document, DocumentBuilder}

class InMemoryConnection(driver: Driver, options: Document) extends Connection {
  private val clearStoreOnClose =
    Option(options.getBooleanObj(ConnectionOptions.clearStoreOnCloseOption))
      .fold(true)(shouldClose => shouldClose)

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

  override def close(): Unit = {
    storeRegistry.clean()
  }

  override def getStore(storeName: String): DocumentStore = {

    val store = new InMemoryStore(storeName, this, clearStoreOnClose)

    storeRegistry.putStore(storeName, store)

    storeRegistry.getStore(storeName).get
  }

  override def createStore(storeName: String): DocumentStore = getStore(storeName)

  override def storeExists(storeName: String): Boolean = storeRegistry.getStore(storeName).isDefined

  override def deleteStore(storeName: String): Boolean = storeRegistry.removeStore(storeName)

  private lazy val storeRegistry = StoreRegistry()
}




