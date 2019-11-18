package tests

import java.nio.ByteBuffer

import com.github.anicolaspp.ojai.{ConnectionOptions, ScalaOjaiTesting}
import com.mapr.db.impl.InMemoryStore
import com.mapr.ojai.store.impl.Values
import org.ojai
import org.ojai.FieldPath
import org.ojai.json.Json
import org.ojai.store.{DriverManager, QueryCondition}
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

import scala.collection.JavaConversions._
import scala.util.Try

class ConnectionTest extends FlatSpec
  with ScalaOjaiTesting
  with Matchers
  with BeforeAndAfterEach {

  override def beforeEach(): Unit = getConnection().close()


  "Connection" should "create document" in {

    getConnection().newDocument().asJsonString() should be("{}")
  }

  it should "create document with fields" in {

    getConnection()
      .newDocument()
      .set("_id", "1")
      .set("name", "nico")
      .set("age", 30)
      .asJsonString() should be("{\"_id\":\"1\",\"name\":\"nico\",\"age\":30}")
  }

  it should "use InMemoryStore" in {

    getConnection().getStore("anicolaspp/mem").isInstanceOf[InMemoryStore] should be(true)

  }

  it should "mutation set not doc" in {
    val store = documentStore("anicolaspp/mem")

    val mutation = getConnection().newMutation().set("name", "nico")

    store.update("1", mutation)

    val doc = store.findById("1")

    doc.getIdString should be("1")
    doc.getString("name") should be("nico")
  }

  it should "mutation set existing doc" in {
    val store = documentStore("anicolaspp/mem")

    store.insert(getConnection().newDocument().set("_id", "1").set("name", "pepe").set("age", 20))

    val mutation = getConnection().newMutation().set("name", "nico")

    store.update("1", mutation)

    val doc = store.findById("1")

    doc.getIdString should be("1")
    doc.getString("name") should be("nico")
    doc.getInt("age") should be(20)
  }


  it should "mutation set or replace" in {

    val store = documentStore("anicolaspp/mem")

    val mutation = getConnection().newMutation().setOrReplace("name", "nico")

    store.update("1", mutation)

    val doc = store.findById("1")

    doc.getIdString should be("1")
    doc.getString("name") should be("nico")
  }

  it should "mutation increment" in {

    val store = documentStore("anicolaspp/mem")

    val mutation = getConnection().newMutation()
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

  it should "append in list" in {

    val store = documentStore("anicolaspp/mem")

    val doc = getConnection().newDocument()
      .set("name", "nico")
      .set("values", List.empty[Int])

    store.insert("1", doc)

    val appendMutation = getConnection().newMutation()
      .append("values", List(1, 2, 3, 4, 5))

    store.update("1", appendMutation)

    val result = store.findById("1")

    result.getIdString should be("1")
    result.getList("values").length should be(5)
  }

  it should "append in byte array" in {

    val store = documentStore("anicolaspp/mem")

    val doc = getConnection().newDocument()
      .set("name", "nico")
      .set("values", "testing".getBytes())

    store.insert("1", doc)

    val appendMutation = getConnection().newMutation()
      .append("values", List("hello", "word").flatMap(_.getBytes()).toArray)

    store.update("1", appendMutation)

    val result = store.findById("1")

    result.getIdString should be("1")

    result.getList("values").length should be(List("testing", "hello", "word").flatMap(_.getBytes()).length)
  }

  it should "append in String" in {

    val store = documentStore("anicolaspp/mem")

    val doc = getConnection().newDocument()
      .set("name", "nico")
      .set("values", "testing")

    store.insert("1", doc)

    val appendMutation = getConnection().newMutation()
      .append("values", " append string")

    store.update("1", appendMutation)

    val result = store.findById("1")

    result.getIdString should be("1")
    result.getString("values") should be("testing append string")
  }

  it should "delete" in {
    val store = documentStore("anicolaspp/mem")

    val mutation = getConnection().newMutation()
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

    val mutation = getConnection().newMutation()
      .increment("count", 5)
      .set("name", "pepe")
      .decrement(FieldPath.parseFrom("count"), 1)

    store.update("1", mutation)

    store.findById("1").getInt("count") should be(4)
  }

  import scala.collection.JavaConverters._

  it should "find with query non-nested" in {
    val store = documentStore("anicolaspp/aaa")

    val mutation = getConnection().newMutation()
      .increment("count", 5)
      .set("name", "pepe")
      .decrement(FieldPath.parseFrom("count"), 1)

    store.update("1", mutation)

    val cond = getConnection().newCondition().is("name", QueryCondition.Op.EQUAL, "pepe").build()

    val query = getConnection().newQuery()
      .where(cond)
      .select("count")
      .limit(10)
      .build()

    val result = store.find(query)

    result.asScala.toList.length should be(1)
  }

  it should "find with query nested" in {
    val store = documentStore("anicolaspp/mem")

    val mutation = getConnection().newMutation()
      .increment("count", 5)
      .set("name", "pepe")
      .decrement(FieldPath.parseFrom("count"), 1)

    store.update("1", mutation)

    val cond = getConnection().newCondition()
      .and()
      .condition(getConnection().newCondition().is("name", QueryCondition.Op.EQUAL, "pepe").build())
      .condition(getConnection().newCondition().is("count", QueryCondition.Op.EQUAL, 4).build())
      .close()
      .build()

    val query = getConnection().newQuery()
      .where(cond)
      .select("count")
      .limit(10)
      .build()

    val result = store.find(query)

    result.asScala.toList.length should be(1)
  }

  it should "create query from json" in {

    val query = getConnection()
      .newQuery()
      .where(getConnection()
        .newCondition()
        .is("name", QueryCondition.Op.EQUAL, "pepe")
        .build())
      .select("a", "b", "c")
      .build()

    getConnection().newQuery(query.asJsonString()).asJsonString() should be(query.asJsonString())
  }

  it should "create document from json" in {

    val document = getConnection()
      .newDocument()
      .set("name", "pepe")
      .set("value", 5)

    getConnection().newDocument(document.asJsonString()).asJsonString() should be(document.asJsonString())
  }

  import com.mapr.ojai.store.impl.InMemoryDriver

  it should "use the InMemoryDriver" in {

    InMemoryDriver.getClass should be(getConnection().getDriver.getClass)
  }

  it should "work with any store protocol" in {

    Try {
      getConnection().getStore("asfasdf")
    }.isFailure should be(false)
    Try {
      getConnection().getStore("anicolaspp")
    }.isSuccess should be(true)

  }

  def parseC(doc: ojai.Document) = {
    if (doc.getMap("$ge") != null) {
      val ge = getConnection().newDocument(doc.getMap("$ge"))

      getConnection().newDocument()
        .set("_id", new String(ge.getBinary("$$row_key").array()))
    } else if (doc.getMap("$lt") != null) {
      val ge = getConnection().newDocument(doc.getMap("$lt"))

      getConnection().newDocument()
        .set("_id", new String(ge.getBinary("$$row_key").array()))
    } else getConnection().newDocument()
  }

  import scala.collection.JavaConversions._

  it should "get doc" in {

    val d = "{\"$and\":[{\"$ge\":{\"$$row_key\":{\"$binary\":\"AzE5NjY0MzYwNjI=\"}}}, {\"$lt\":{\"$$row_key\":{\"$binary\":\"AzUyOTI5NzI2Nw==\"}}}]}"


    val doc: ojai.Document = getConnection().newDocument(d)

    val and = doc.getList("$and")

    and.map(x => getConnection().newDocument(x))
      .map(d => parseC(d).asJsonString())
      .foreach(println)
  }

  it should "project maps" in {
    val existing = "{\n" +
      "\"_id\": \"1\",\n" +
      "\"customer_no\":1,\n" +
      "\"brand_id\":1,\n" +
      "\"emails\":[{\"type\":25, \"address\": \"mpereira@mapr.com\"}, {\"type\":52}]\n" +
      "}\n"

    val doc = getConnection().newDocument(existing)

    val store = documentStore("anicolaspp/test1")
    store.insert(doc)
    store.insert(getConnection().newDocument().set("_id", "2").set("address", Map[String, String]("line1" -> "15501")))


    store.insert(

      getConnection().newDocument(

        "{\n" +
          "\"_id\": \"3\",\n" +
          "\"customer_no\":1,\n" +
          "\"brand_id\":1,\n" +
          "\"emails\":[{\"t\": [{\"x\":7}, {\"m\":8}]}, {\"type\":26, \"address\": \"nperez@mapr.com\"}, {\"type\":30}]\n" +
          "}\n"

      )

    )

    val query = getConnection().newQuery().select("_id", "address.line1", "address", "emails[].type", "emails[].t[].x", "emails[].t[].m")

    println(query.asJsonString())

    val result = store.find(query).asScala.toList


    //    result.length should be(3)

    result.foreach(println)
  }

  it should "find in container fields" in {
    val first = getConnection().newDocument("{\n  \"_id\": \"001\",\n  \"tasks\": [\n    {\"a\": \"t001\"},\n    {\"a\": \"t002\"}\n, {\"b\": \"t002\"}\n  ]\n}")
    val second = getConnection().newDocument("{\n  \"_id\": \"002\",\n  \"tasks\": [\n    {\"a\": \"t003\"}\n  ]\n}")

    val store = documentStore("anicolaspp/containerfields")

    store.insert(first)
    store.insert(second)

    val condition = getConnection().newCondition().is("tasks[].a", QueryCondition.Op.EQUAL, "t002").build()

    val query = getConnection().newQuery().where(condition).build()

    val result = store.find(query).asScala.toList

    result.size should be(1)
  }

  it should "find in container field and ignored embedded" in {
    val first = getConnection().newDocument("{\n  \"_id\": \"001\",\n  \"tasks\": [\n    {\"b\": \"t001\", \"a\": {\"a\": \"t002\"}\n},\n    {\"a\": \"t003\"}\n, {\"b\": \"t002\"}\n  ]\n}")

    val store = documentStore("anicolaspp/containerfields2")

    store.insert(first)

    val condition = getConnection().newCondition().is("tasks[].a", QueryCondition.Op.EQUAL, "t002").build()

    val query = getConnection().newQuery().where(condition).build()

    val result = store.find(query).asScala.toList

    result.size should be(0)
  }

  it should "insert binary id" in {
    val store = documentStore("anicolaspp/binary")

    store.insert(new Values.BinaryValue(ByteBuffer.wrap("value".getBytes())), getConnection().newDocument().set("age", 30))

    store.find().asScala.toList.head.getInt("age") should be(30)
  }

  it should "closing the connection means cleaning all stores - ignore store options" in {
    DriverManager.registerDriver(InMemoryDriver)
    val options = Json.newDocument().set(ConnectionOptions.clearStoreOnCloseOption, false)

    val connection = DriverManager.getConnection("ojai:anicolaspp:mem", options)

    val store1 = connection.getStore("store_1")
    store1.insert(connection.newDocument().set("_id", "1"))

    val store2 = connection.getStore("store_2")
    store2.insert(connection.newDocument().set("_id", "1"))

    store1.close()
    store2.close()

    connection.getStore("store_1").find().iterator().hasNext should be (true)
    connection.getStore("store_2").find().iterator().hasNext should be (true)

    connection.close()

    connection.getStore("store_1").find().iterator().hasNext should be (false)
    connection.getStore("store_2").find().iterator().hasNext should be (false)
  }
}