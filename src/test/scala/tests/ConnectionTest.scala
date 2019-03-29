package tests

import com.github.anicolaspp.ojai.OjaiTesting
import com.mapr.db.impl.InMemoryStore
import org.ojai.FieldPath
import org.ojai.store.QueryCondition
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

import scala.util.{Failure, Try}

class ConnectionTest extends FlatSpec with OjaiTesting with Matchers with BeforeAndAfterEach {

  override def beforeEach(): Unit = connection.close()


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

  it should "mutation set not doc" in {
    val store = documentStore("anicolaspp/mem")

    val mutation = connection.newMutation().set("name", "nico")

    store.update("1", mutation)

    val doc = store.findById("1")

    doc.getIdString should be("1")
    doc.getString("name") should be("nico")
  }

  it should "mutation set existing doc" in {
    val store = documentStore("anicolaspp/mem")

    store.insert(connection.newDocument().set("_id", "1").set("name", "pepe").set("age", 20))

    val mutation = connection.newMutation().set("name", "nico")

    store.update("1", mutation)

    val doc = store.findById("1")

    doc.getIdString should be("1")
    doc.getString("name") should be("nico")
    doc.getInt("age") should be(20)
  }


  it should "mutation set or replace" in {

    val store = documentStore("anicolaspp/mem")

    val mutation = connection.newMutation().setOrReplace("name", "nico")

    store.update("1", mutation)

    val doc = store.findById("1")

    doc.getIdString should be("1")
    doc.getString("name") should be("nico")
  }

  it should "mutation increment" in {

    val store = documentStore("anicolaspp/mem")

    val mutation = connection.newMutation()
      .increment("count", 5)
      .set("name", "pepe")
      .set("age", 29)
      .increment("age", 1)

    store.update("1", mutation)

    val doc = store.findById("1")

    doc.getIdString should be("1")
    doc.getInt("count") should be(5)

    store.update("1", mutation)

    store.findById("1").getInt("count") should be(10)
  }

  it should "delete" in {
    val store = documentStore("anicolaspp/mem")

    val mutation = connection.newMutation()
      .increment("count", 5)
      .set("name", "pepe")

    store.update("1", mutation)

    store.update("1", mutation.delete("count"))

    Try {
      store.findById("1").getInt("count")
    }.isFailure should be(true)
  }

  it should "decrement" in {
    val store = documentStore("anicolaspp/mem")

    val mutation = connection.newMutation()
      .increment("count", 5)
      .set("name", "pepe")
      .decrement(FieldPath.parseFrom("count"), 1)

    store.update("1", mutation)

    store.findById("1").getInt("count") should be(4)
  }

  import scala.collection.JavaConverters._

  it should "find with query non-nested" in {
    val store = documentStore("anicolaspp/mem")

    val mutation = connection.newMutation()
      .increment("count", 5)
      .set("name", "pepe")
      .decrement(FieldPath.parseFrom("count"), 1)

    store.update("1", mutation)

    val cond = connection.newCondition().is("name", QueryCondition.Op.EQUAL, "pepe").build()

    val query = connection.newQuery()
      .where(cond)
      .select("count")
      .limit(10)
      .build()

    val result = store.find(query)

    result.asScala.toList.length should be(1)
  }

  it should "find with query nested" in {
    val store = documentStore("anicolaspp/mem")

    val mutation = connection.newMutation()
      .increment("count", 5)
      .set("name", "pepe")
      .decrement(FieldPath.parseFrom("count"), 1)

    store.update("1", mutation)

    val cond = connection.newCondition()
      .and()
      .condition(connection.newCondition().is("name", QueryCondition.Op.EQUAL, "pepe").build())
      .condition(connection.newCondition().is("count", QueryCondition.Op.EQUAL, 4).build())
      .close()
      .build()

    val query = connection.newQuery()
      .where(cond)
      .select("count")
      .limit(10)
      .build()

    val result = store.find(query)

    result.asScala.toList.length should be(1)
  }
}


