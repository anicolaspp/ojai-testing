package com.mapr.ojai.store.impl

import java.{lang, util}

import com.mapr.db.impl.MapRDBImpl
import org.ojai.json.impl.{JsonDocumentStream, JsonStreamDocumentReader, JsonValueBuilder}
import org.ojai.store._
//import org.ojai.store._
import org.ojai.{Document, DocumentBuilder, DocumentListener, DocumentReader, DocumentStream, FieldPath, Value}

object InMemDriver extends Driver {
  override def getName = "InMemDriver"

  override def getValueBuilder: ValueBuilder = JsonValueBuilder.INSTANCE

  override def newDocument(): Document = MapRDBImpl.newDocument()

  override def newDocument(documentJson: String): Document = MapRDBImpl.newDocument(documentJson)

  override def newDocument(map: util.Map[String, AnyRef]): Document = MapRDBImpl.newDocument(map)

  override def newDocument(bean: Any): Document = MapRDBImpl.newDocument(bean)

  override def newDocumentBuilder(): DocumentBuilder = MapRDBImpl.newDocumentBuilder()

  override def newMutation(): DocumentMutation = MapRDBImpl.newMutation()

  override def newCondition(): QueryCondition = MapRDBImpl.newCondition()

  override def newQuery(): Query = new OjaiQuery()

  override def newQuery(queryJson: String): Query = {
    val queryParser = new QueryParser()

    queryParser.parseQuery(queryJson)
  }

  override def accepts(url: String) = url == "ojai:anicolaspp:mem"

  override def connect(url: String, options: Document): Connection = new InMemConnection(this)
}

class InMemConnection(driver: Driver) extends Connection {
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

  override def getStore(storeName: String): DocumentStore = new InMemoryStore()
}