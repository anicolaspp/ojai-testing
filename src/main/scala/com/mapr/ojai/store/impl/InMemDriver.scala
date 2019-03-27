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

  override def accepts(url: String) = url == "ojai:mem:@"

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

//object InMemStore extends DocumentStore {
//
//  import scala.collection.JavaConverters._
//
//  private val documents = scala.collection.mutable.ListBuffer.empty[Document]
//
//
//  override def isReadOnly: Boolean = false
//
//  override def flush(): Unit = throw new UnsupportedOperationException
//
//  override def beginTrackingWrites(): Unit = throw new UnsupportedOperationException
//
//  override def beginTrackingWrites(previousWritesContext: String): Unit = throw new UnsupportedOperationException
//
//  override def endTrackingWrites(): String = throw new UnsupportedOperationException
//
//  override def clearTrackedWrites(): Unit = throw new UnsupportedOperationException
//
//  override def findById(_id: String): Document = documents.find(_.getIdString == _id).orNull
//
//  override def findById(_id: Value): Document = documents.find(_.getId == _id).orNull
//
//  private def project(document: Document, fields: String*): Document =
//    fields.foldLeft(MapRDBImpl.newDocument())((acc, field) => acc.set(field, document.getValue(field)))
//
//  override def findById(_id: String, fieldPaths: String*): Document = {
//    val maybeDoc = documents.find(_.getIdString == _id)
//
//    maybeDoc
//      .map(document => project(document, fieldPaths: _*))
//      .orNull
//  }
//
//  override def findById(_id: String, fieldPaths: FieldPath*): Document = findById(_id,  fieldPaths.map(_.getName): _*)
//
//  override def findById(_id: Value, fieldPaths: String*): Document = findById(_id.getString, fieldPaths: _*)
//
//  override def findById(_id: Value, fieldPaths: FieldPath*): Document = findById(_id.getString, fieldPaths.map(_.getName): _*)
//
//  override def findById(_id: String, condition: QueryCondition): Document = throw new UnsupportedOperationException
//
//  override def findById(_id: Value, condition: QueryCondition): Document = throw new UnsupportedOperationException
//
//  override def findById(_id: String, condition: QueryCondition, fieldPaths: String*): Document = throw new UnsupportedOperationException
//
//  override def findById(_id: String, condition: QueryCondition, fieldPaths: FieldPath*): Document = throw new UnsupportedOperationException
//
//  override def findById(_id: Value, condition: QueryCondition, fieldPaths: String*): Document = throw new UnsupportedOperationException
//
//  override def findById(_id: Value, condition: QueryCondition, fieldPaths: FieldPath*): Document = throw new UnsupportedOperationException
//
//  override def find(query: Query): QueryResult = ???
//
//  override def find(): DocumentStream = new DocumentStream {
//
//    import scala.collection.JavaConverters._
//
//    override def streamTo(listener: DocumentListener): Unit = documents.foreach(d => listener.documentArrived(d))
//
//    override def iterator(): util.Iterator[Document] = documents.iterator.asJava
//
//    override def documentReaders(): lang.Iterable[DocumentReader] = throw new UnsupportedOperationException
//
//    override def close(): Unit = {}
//  }
//
//  override def findQuery(query: Query): DocumentStream = ???
//
//  override def findQuery(queryJSON: String): DocumentStream = ???
//
//  override def find(fieldPaths: String*): DocumentStream = ???
//
//  override def find(fieldPaths: FieldPath*): DocumentStream = ???
//
//  override def find(condition: QueryCondition): DocumentStream = ???
//
//  override def find(condition: QueryCondition, fieldPaths: String*): DocumentStream = ???
//
//  override def find(condition: QueryCondition, fieldPaths: FieldPath*): DocumentStream = ???
//
//  override def insertOrReplace(doc: Document): Unit = insertOrReplace(doc.getIdString, doc)
//
//  override def insertOrReplace(_id: String, r: Document): Unit = {
//    val idx = documents.indexWhere(_.getIdString == _id)
//
//    if (idx > 0) {
//      documents.remove(idx)
//    }
//
//    documents.append(r)
//  }
//
//  override def insertOrReplace(_id: Value, doc: Document): Unit = insertOrReplace(_id.getString, doc)
//
//  override def insertOrReplace(doc: Document, fieldAsKey: FieldPath): Unit = insertOrReplace(doc.getString(fieldAsKey), doc)
//
//  override def insertOrReplace(doc: Document, fieldAsKey: String): Unit = insertOrReplace(doc.getString(fieldAsKey), doc)
//
//  override def insertOrReplace(stream: DocumentStream): Unit = stream.asScala.foreach(doc => insertOrReplace(doc))
//
//  override def insertOrReplace(stream: DocumentStream, fieldAsKey: FieldPath): Unit =
//    stream.asScala.foreach(doc => insertOrReplace(doc, fieldAsKey))
//
//  override def insertOrReplace(stream: DocumentStream, fieldAsKey: String): Unit =
//    stream.asScala.foreach(doc => insertOrReplace(doc, fieldAsKey))
//
//  override def update(_id: String, mutation: DocumentMutation): Unit = ???
//
//  override def update(_id: Value, mutation: DocumentMutation): Unit = ???
//
//  override def delete(_id: String): Unit = ???
//
//  override def delete(_id: Value): Unit = ???
//
//  override def delete(doc: Document): Unit = ???
//
//  override def delete(doc: Document, fieldAsKey: FieldPath): Unit = ???
//
//  override def delete(doc: Document, fieldAsKey: String): Unit = ???
//
//  override def delete(stream: DocumentStream): Unit = ???
//
//  override def delete(stream: DocumentStream, fieldAsKey: FieldPath): Unit = ???
//
//  override def delete(stream: DocumentStream, fieldAsKey: String): Unit = ???
//
//  override def insert(_id: String, doc: Document): Unit = ???
//
//  override def insert(_id: Value, doc: Document): Unit = ???
//
//  override def insert(doc: Document): Unit = ???
//
//  override def insert(doc: Document, fieldAsKey: FieldPath): Unit = ???
//
//  override def insert(doc: Document, fieldAsKey: String): Unit = ???
//
//  override def insert(stream: DocumentStream): Unit = ???
//
//  override def insert(stream: DocumentStream, fieldAsKey: FieldPath): Unit = ???
//
//  override def insert(stream: DocumentStream, fieldAsKey: String): Unit = ???
//
//  override def replace(_id: String, doc: Document): Unit = ???
//
//  override def replace(_id: Value, doc: Document): Unit = ???
//
//  override def replace(doc: Document): Unit = ???
//
//  override def replace(doc: Document, fieldAsKey: FieldPath): Unit = ???
//
//  override def replace(doc: Document, fieldAsKey: String): Unit = ???
//
//  override def replace(stream: DocumentStream): Unit = ???
//
//  override def replace(stream: DocumentStream, fieldAsKey: FieldPath): Unit = ???
//
//  override def replace(stream: DocumentStream, fieldAsKey: String): Unit = ???
//
//  override def increment(_id: String, field: String, inc: Byte): Unit = ???
//
//  override def increment(_id: String, field: String, inc: Short): Unit = ???
//
//  override def increment(_id: String, field: String, inc: Int): Unit = ???
//
//  override def increment(_id: String, field: String, inc: Long): Unit = ???
//
//  override def increment(_id: String, field: String, inc: Float): Unit = ???
//
//  override def increment(_id: String, field: String, inc: Double): Unit = ???
//
//  override def increment(_id: String, field: String, inc: java.math.BigDecimal): Unit = ???
//
//  override def increment(_id: Value, field: String, inc: Byte): Unit = ???
//
//  override def increment(_id: Value, field: String, inc: Short): Unit = ???
//
//  override def increment(_id: Value, field: String, inc: Int): Unit = ???
//
//  override def increment(_id: Value, field: String, inc: Long): Unit = ???
//
//  override def increment(_id: Value, field: String, inc: Float): Unit = ???
//
//  override def increment(_id: Value, field: String, inc: Double): Unit = ???
//
//  override def increment(_id: Value, field: String, inc: java.math.BigDecimal): Unit = ???
//
//  override def checkAndMutate(_id: String, condition: QueryCondition, mutation: DocumentMutation): Boolean = ???
//
//  override def checkAndMutate(_id: Value, condition: QueryCondition, mutation: DocumentMutation): Boolean = ???
//
//  override def checkAndDelete(_id: String, condition: QueryCondition): Boolean = ???
//
//  override def checkAndDelete(_id: Value, condition: QueryCondition): Boolean = ???
//
//  override def checkAndReplace(_id: String, condition: QueryCondition, doc: Document): Boolean = ???
//
//  override def checkAndReplace(_id: Value, condition: QueryCondition, doc: Document): Boolean = ???
//
//  override def close(): Unit = {}
//}