package tests

import com.github.anicolaspp.ojai.{InMemoryStore, OjaiTesting}
import com.mapr.ojai.store.impl.InMemoryDriver
import org.ojai.store.DriverManager
import org.scalatest.{FlatSpec, Matchers}

class ConnectionTest extends FlatSpec with OjaiTesting with Matchers {

  "Connection" should "create document" in {

    connection.newDocument().asJsonString() should be("{}")
  }

  it should "create document with fields" in {

    connection
      .newDocument()
      .set("_id", "1")
      .set("name", "nico")
      .set("age", 30)
      .asJsonString() should be("{\"_id\":\"1\",\"name\":\"nico\",\"age\":30}")
  }

  it should "use InMemoryStore" in {

    connection.getStore("anicolaspp/mem").isInstanceOf[InMemoryStore] should be(true)

  }

  it should "mutation set" in {
    val store = documentStore("anicolaspp/update")

    val mutation = connection.newMutation().set("name", "nico")

    store.update("1", mutation)

    val doc = store.findById("1")

    doc.getIdString should be ("1")
    doc.getString("name") should be ("nico")
  }
}


