package com.github.anicolaspp.ojai

//package com.mapr.ojai.store.impl._

import com.mapr.ojai.store.impl.InMemDriver
import org.ojai.store._

import scala.collection.JavaConverters._

object App {
  def main(args: Array[String]): Unit = {
    println("HELLO IN MEM OJAI")

    DriverManager.registerDriver(InMemDriver)

    val connection = DriverManager.getConnection("ojai:mem:@")

    val doc = connection.newDocument().set("name", "nico").set("age", 30)

    val store = connection.getStore("table")

    store.delete("-1")

    val result = store
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

    println(doc.asJsonString())
  }
}







