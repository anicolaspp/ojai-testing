package com.github.anicolaspp.ojai
//package com.mapr.ojai.store.impl._

import com.mapr.ojai.store.impl.InMemDriver
import org.ojai.store._


object App {
  def main(args: Array[String]): Unit = {
    println("HELLO IN MEM OJAI")
    
    DriverManager.registerDriver(InMemDriver)

    val connection = DriverManager.getConnection("ojai:mem:@")

    val doc = connection.newDocument().set("name", "nico").set("age", 30)

    val store = connection.getStore("table")

    println(doc.asJsonString())
  }
}







