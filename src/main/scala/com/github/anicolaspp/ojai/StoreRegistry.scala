package com.github.anicolaspp.ojai

import org.ojai.store.DocumentStore

trait StoreRegistry {

  def putStore(storeName: String, store: DocumentStore): Unit

  def getStore(storeName: String): Option[DocumentStore]

  def removeStore(storeName: String): Boolean

  def clean(): Unit
}

object StoreRegistry {

  def apply(): StoreRegistry = new StoreRegistry {

    private val stores = scala.collection.mutable.HashMap.empty[String, DocumentStore]

    override def putStore(storeName: String, store: DocumentStore): Unit = {
      if (stores.get(storeName).isEmpty){
        stores.put(storeName, store)
      }
    }

    override def getStore(storeName: String): Option[DocumentStore] = stores.get(storeName)

    override def removeStore(storeName: String): Boolean = stores.remove(storeName).isDefined

    override def clean(): Unit = stores.clear()
  }
}