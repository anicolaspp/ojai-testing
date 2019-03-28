package tests

import com.github.anicolaspp.ojai.InMemoryConnection
import com.mapr.ojai.store.impl.InMemoryDriver
import org.ojai.store.DriverManager
import org.scalatest.{FlatSpec, Matchers}

class DriverManagerTest extends FlatSpec with Matchers {

  "DriverManger" should "register InMemoryDriver" in {

    DriverManager.registerDriver(InMemoryDriver)

    DriverManager.getConnection("ojai:anicolaspp:mem").isInstanceOf[InMemoryConnection] should be(true)
  }
}
