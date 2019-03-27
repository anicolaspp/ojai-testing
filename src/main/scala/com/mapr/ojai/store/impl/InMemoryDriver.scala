package com.mapr.ojai.store.impl

import java.util

import com.mapr.db.impl.MapRDBImpl
import org.ojai.json.impl.JsonValueBuilder
import org.ojai.store._
import org.ojai.{Document, DocumentBuilder}

object InMemoryDriver extends Driver {
  
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

  override def connect(url: String, options: Document): Connection = new InMemoryConnection(this)
}

