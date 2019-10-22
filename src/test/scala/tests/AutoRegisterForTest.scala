package tests

import com.github.anicolaspp.ojai.{InMemoryConnection, OjaiTesting, ScalaOjaiTesting}
import org.scalatest.{FlatSpec, Matchers}

class AutoRegisterForTest extends FlatSpec with ScalaOjaiTesting with Matchers {

  it should "be ready" in {

    getConnection().isInstanceOf[InMemoryConnection] should be (true)

    getConnection().getStore("anicolaspp/mem") should be (documentStore("anicolaspp/mem"))
  }

}
