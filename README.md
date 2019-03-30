# ojai-testing
[![Build Status](https://travis-ci.org/anicolaspp/ojai-testing.svg?branch=master)](https://travis-ci.org/anicolaspp/ojai-testing)[![codecov](https://codecov.io/gh/anicolaspp/ojai-testing/branch/master/graph/badge.svg)](https://codecov.io/gh/anicolaspp/ojai-testing)

Embeded In-Memory Implementation of OJAI Driver to be used in testing

The aim of this project is to provide to a way of running an OJAI Document Store in memory that can be used for mocking 
MapR Database when running test suits.

In order to use our in memory store we should first register the correct driver as follow.

```scala
 DriverManager.registerDriver(InMemDriver)
```

Alternatively, you can mix in the `OjaiTesting` trait and it will automatically register our driver.

```scala
class AutoRegisterForTest extends FlatSpec with OjaiTesting with Matchers {

  it should "be ready" in {

    connection.isInstanceOf[InMemoryConnection] should be (true)

    connection.getStore("anicolaspp/mem") should be (storeHandler("anicolaspp/mem"))
  }

}
```

Then we can get a `Connection` and a `DocumentStore` as we normally do when connecting to a MapR Database table.

```scala
val connection = DriverManager.getConnection("ojai:anicolaspp:mem")

val store = connection.getStore("anicolaspp/mem")
```
The `storeName` being passed must start with `anicolaspp`.

There will be only a single instance per store name so the following holds.

```scala
class SomeTests extends FlatSpec with OjaiTesting with Matchers {

  it should "keep track of stores" in {
    val store = connection.getStore("anicolaspp/my_store")

    store should be (connection.getStore("anicolaspp/my_store"))
    
    storeHandler("anicolaspp/a_store") should be (connection.getStore("anicolaspp/a_store"))
    
    store should not be (storeHandler("anicolaspp/a_store"))
  }
}
```
Notice we are mixing in the `OjaiTesting` trait to auto register the correct driver and to have access to `connection` and `storeHandler`.

After we have gained access to the `DocumentStore` we should be able to run OJAI queries on it. 

```scala

object Run extends OjaiTesting {

 def run() = {
  store = storeHandler

  val doc = connection.newDocument().set("name", "nico").set("age", 30).set("_id", "1")

  store.insert(doc)
  assert(store.find().asScala.toList.length == 1)

  store.insert(connection.newDocument().set("name", "nico").set("age", 30).set("_id", "2"))
  assert(store.find().asScala.toList.length == 2)

  store.delete("1")
  assert(store.find().asScala.toList.length == 1)

  store.increment("2", "age", 2)
  store.increment("a", "asd", 2)

  store
   .find()
   .asScala
   .foreach(println)
 } 
 
}
```

## Using Java

If you want to use this library from Java, that can be done without drawbacks since we have the same testing facilities ready to be used. 

```java
public class JavaTesting implements JavaOjaiTesting {
    
    @Test
    public void testGetConnection() {
        assert connection() instanceof InMemoryConnection;
    }
    
    @Test
    public void testGetStore() {
        assert documentStore("anicolaspp/java_store") instanceof InMemoryStore;
    }
}
```

Notice that by implementing `JavaOjaiTesting` we gain access to the same constructs, `connection()` and `documentStore()` and from here we just write our normal Java tests using the testing framework of your choice. 

## Cross Build

This project is cross built for Scala `2.11.8` and `2.12.8`. Make sure you select the right version for your Scala.
