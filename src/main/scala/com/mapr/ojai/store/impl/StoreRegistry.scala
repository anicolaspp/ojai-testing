package com.mapr.ojai.store.impl

import org.ojai.store.DocumentStore

trait StoreRegistry {

  def putStore(storeName: String, store: DocumentStore)

  def getStore(storeName: String): Option[DocumentStore]
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
  }

}