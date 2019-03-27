package tests

import com.mapr.ojai.store.impl.{InMemoryDriver, InMemoryStore}
import org.ojai.store.DriverManager
import org.scalatest.{FlatSpec, Matchers}

class ConnectionTest extends FlatSpec with Matchers {

  DriverManager.registerDriver(InMemoryDriver)

  "Connection" should "create document" in {

    val connection = DriverManager.getConnection("ojai:anicolaspp:mem")

    connection.newDocument().asJsonString() should be("{}")
  }

  it should "create document with fields" in {

    val connection = DriverManager.getConnection("ojai:anicolaspp:mem")

    connection
      .newDocument()
      .set("_id", "1")
      .set("name", "nico")
      .set("age", 30)
      .asJsonString() should be("{\"_id\":\"1\",\"name\":\"nico\",\"age\":30}")
  }

  it should "use InMemoryStore" in {
    val connection = DriverManager.getConnection("ojai:anicolaspp:mem")

    connection.getStore("anicolaspp/mem").isInstanceOf[InMemoryStore] should be (true)

  }

}
