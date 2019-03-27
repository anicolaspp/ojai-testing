package com.github.anicolaspp.ojai

import com.mapr.ojai.store.impl.InMemoryDriver
import org.ojai.store._

import scala.collection.JavaConverters._

object App {
  def main(args: Array[String]): Unit = {

    DriverManager.registerDriver(InMemoryDriver)

    val connection = DriverManager.getConnection("ojai:anicolaspp:mem")

    val doc = connection.newDocument().set("name", "nico").set("age", 30).set("_id", "1")

    val store = connection.getStore("anicolaspp/mem")

    store.insert(doc)

    assert(store.find().asScala.toList.length == 1)

    store.insert(connection.newDocument().set("name", "nico").set("age", 30).set("_id", "2"))

    assert(store.find().asScala.toList.length == 2)

    store.delete("1")

    assert(store.find().asScala.toList.length == 1)

    store.increment("2", "age", 2)
    store.increment("a", "asd", 2)


    store
      .find()
      .asScala
      .foreach(println)




    //
    //        connection
    //          .newQuery()
    //          .where(connection
    //            .newCondition()
    //            .and()
    //            .condition(connection.newCondition().is("_id", QueryCondition.Op.EQUAL, "me").build())
    //            .condition(connection.newCondition().is("age", QueryCondition.Op.EQUAL, 30).build())
    //            .close()
    //            .build())
    //          .build()
    //      )

  }
}



trait OjaiTesting {
  
  lazy val connection: Connection = {
    OjaiTesting.registerDriver

    DriverManager.getConnection("ojai:anicolaspp:mem")
  }

  def storeHandler(): DocumentStore = connection.getStore("anicolaspp/mem")
}

object OjaiTesting {


  def registerDriver = DriverManager.registerDriver(InMemoryDriver)

}




