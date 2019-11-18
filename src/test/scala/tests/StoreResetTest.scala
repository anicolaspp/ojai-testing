package tests

import com.github.anicolaspp.ojai.{ConnectionOptions, ScalaOjaiTesting}
import com.mapr.ojai.store.impl.InMemoryDriver
import org.ojai.json.Json
import org.ojai.store.DriverManager
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

class StoreResetTest extends FlatSpec
  with ScalaOjaiTesting
  with Matchers
  with BeforeAndAfterEach {

  it should "insert clean up on reset" in {
    val store = documentStore("someStore")
    store.insert(getConnection().newDocument().setId("hello"))


    store.find().iterator().hasNext should be (true)

    val sameStore = documentStore("someStore")
    sameStore.find().iterator().hasNext should be (true)

    sameStore.close()

    val emptyStore = documentStore("someStore")
    emptyStore.find().iterator().hasNext should be (false)
  }

  it should "not clear store if option is provided" in {
    DriverManager.registerDriver(InMemoryDriver)
    val options = Json.newDocument().set(ConnectionOptions.clearStoreOnCloseOption, false)
    val connection = DriverManager.getConnection("ojai:anicolaspp:mem", options)

    val store = connection.getStore("anicolaspp/non-clearing")
    store.insert(connection.newDocument().set("_id", "1"))
    store.close()

    val store2 = connection.getStore("anicolaspp/non-clearing")
    store2.find().iterator().hasNext should be (true)
  }
}
