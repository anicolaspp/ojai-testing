package tests

import com.github.anicolaspp.ojai.{InMemoryConnection, OjaiTesting}
import org.scalatest.{FlatSpec, Matchers}

class AutoRegisterForTest extends FlatSpec with OjaiTesting with Matchers {

  it should "be ready" in {

    connection.isInstanceOf[InMemoryConnection] should be (true)

    connection.getStore("anicolaspp/mem") should be (documentStore("anicolaspp/mem"))
  }

}
