package com.github.anicolaspp.ojai

import java.{lang, util}

import scala.collection.JavaConverters._
import org.ojai.{Document, DocumentListener, DocumentReader}
import org.ojai.store.{Connection, QueryResult}

import scala.util.{Failure, Success, Try}

class ResultDocumentStream private(documents: Iterable[Document], connection: Connection) extends QueryResult {
  private var isOpen = true

  override def getQueryPlan: Document = runIfOpen(connection.newDocument().set("type", "In memory"))

  override def streamTo(documentListener: DocumentListener): Unit = runIfOpen(documents.foreach(documentListener.documentArrived))

  override def iterator(): util.Iterator[Document] = runIfOpen(documents.toIterator.asJava)

  override def documentReaders(): lang.Iterable[DocumentReader] = runIfOpen(documents.map(_.asReader()).asJava)

  override def close(): Unit = {
    isOpen = false
  }

  private def runIfOpen[A](fn: => A): A = Try {
    if (isOpen) fn else throw new Exception("DocumentStream is closed")
  } match {
    case Failure(exception) => throw exception
    case Success(value)     => value
  }
}

object ResultDocumentStream {
  def apply(documents: java.util.stream.Stream[Document], connection: Connection): ResultDocumentStream =
    new ResultDocumentStream(documents.iterator().asScala.toIterable, connection)
}
