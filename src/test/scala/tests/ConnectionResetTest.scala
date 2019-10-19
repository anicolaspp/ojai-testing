package tests

import com.github.anicolaspp.ojai.OjaiTesting
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

class ConnectionResetTest extends FlatSpec
  with OjaiTesting
  with Matchers
  with BeforeAndAfterEach {

  it should "insert clean up on reset" in {
    val store = documentStore("someStore")
    store.insert(connection.newDocument().setId("hello"))


    store.find().iterator().hasNext should be (true)

    val sameStore = documentStore("someStore")
    sameStore.find().iterator().hasNext should be (true)

    sameStore.close()

    val emptyStore = documentStore("someStore")
    emptyStore.find().iterator().hasNext should be (false)
  }

}
