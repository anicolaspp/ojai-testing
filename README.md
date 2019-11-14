# ojai-testing
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.anicolaspp/ojai-testing_2.11/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.anicolaspp/ojai-testing_2.11)
[![Build Status](https://travis-ci.org/anicolaspp/ojai-testing.svg?branch=master)](https://travis-ci.org/anicolaspp/ojai-testing)
[![codecov](https://codecov.io/gh/anicolaspp/ojai-testing/branch/master/graph/badge.svg)](https://codecov.io/gh/anicolaspp/ojai-testing)

Embeded In-Memory Implementation of OJAI Driver to be used in testing

The aim of this project is to provide to a way of running an OJAI Document Store in memory that can be used for mocking 
MapR Database when running test suits.

In order to use our in memory store we should first register the correct driver as follow.

```scala
 DriverManager.registerDriver(InMemoryDriver)
```

Alternatively, you can mix in the `OjaiTesting` trait and it will automatically register our driver.

```scala
class AutoRegisterForTest extends FlatSpec with ScalaOjaiTesting with Matchers {

  it should "be ready" in {

    getConnection().isInstanceOf[InMemoryConnection] should be (true)

    getConnection().getStore("anicolaspp/mem") should be (documentStore("anicolaspp/mem"))
  }

}
```

Then we can get a `Connection` and a `DocumentStore` as we normally do when connecting to a MapR Database table.

```scala
val connection = DriverManager.getConnection("ojai:anicolaspp:mem")

val store = connection.getStore("anicolaspp/mem")
```
The `storeName` can be any name you want. 

There will be only a single instance per store name so the following holds.

```scala
class SomeTests extends FlatSpec with ScalaOjaiTesting with Matchers {

  it should "keep track of stores" in {
    val store = getConnection().getStore("anicolaspp/my_store")

    store should be (getConnection().getStore("anicolaspp/my_store"))
    
    documentStore("anicolaspp/a_store") should be (getConnection().getStore("anicolaspp/a_store"))
    
    store should not be (documentStore("anicolaspp/a_store"))
  }
}
```
When running multiple tests, you can share the same store instance across all of them. 

```scala
class ShareStoreTests extends FlatSpec with ScalaOjaiTesting with Matchers {

  it should "keep values on store" in {
    val store = getConnection().getStore("my_store")
 
    store.insert(getConnection().newDocument().setId("5"))
    
    store.find().iterator.hasNext() should be (true)
  }
  
   it should "has values on it" in {
    val store = getConnection().getStore("my_store")

    val it = store.find().iterator
    
    it.hasNext should be (true)
    it.next.getId().toString() should be ("5")
  }

}
```

However, this behavior might not desirable under certain conditions. In those case, you can clean the stores by calling `.close()` on the store.

```scala
class ConnectionResetTest extends FlatSpec
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

}
```

In order to clean after each test, you can use `override def beforeEach(): Unit = getConnection().close()`


Notice we are mixing in the `ScalaOjaiTesting` trait to auto register the correct driver and to have access to 
`getConnection` and `documentStore`.

After we have gained access to the `DocumentStore` we should be able to run OJAI queries on it. 

```scala
object Run extends OjaiTesting {

 def run() = {
  store = documentStore("someStore")

  val doc = getConnection().newDocument().set("name", "nico").set("age", 30).set("_id", "1")

  store.insert(doc)
  assert(store.find().asScala.toList.length == 1)

  store.insert(getConnection().newDocument().set("name", "nico").set("age", 30).set("_id", "2"))
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
public class JavaTesting extends JavaOjaiTesting {
    
    @Test
    public void testGetConnection() {
        assert getConnection() instanceof InMemoryConnection;
    }
    
    @Test
    public void testGetStore() {
        assert documentStore("anicolaspp/java_store") instanceof InMemoryStore;
    }
}
```

Notice that by implementing `JavaOjaiTesting` we gain access to the same constructs, `getConnection()` and 
`documentStore()` and from here we just write our normal Java tests using the testing framework of your choice.

If extending the test class with `JavaOjaiTesting` is not possible, it is also possible to use a composition.
```java
public class JavaTesting extends SomeOtherBaseClass {
    private JavaOjaiTesting testObject = new JavaOjaiTesting();
    
    @Test
    public void testGetConnection() {
        assert testObject.getConnection() instanceof InMemoryConnection;
    }
    
    @Test
    public void testGetStore() {
        assert testObject.documentStore("anicolaspp/java_store") instanceof InMemoryStore;
    }
}
```

## Connection options
In some scenarios you can specify further options for the in memory ojai connection. These options are available:

| Option | Description | Default |
| ------ | ----------- | ------- |
| `ojai.in-memory.store.clear-store-on-close` | As default the data in an in memory store will be cleared on closing the store. Set this option to `false` to preserve the state of the store after closing. | `true` |

The options can be passed to `DriverManager.getConnection(url, options)`.

`com.github.anicolaspp.ojai.ConnectionOptions` defines a constant for each option.

```scala
DriverManager.registerDriver(InMemoryDriver)
val options = Json.newDocument().set(ConnectionOptions.clearStoreOnCloseOption, false)
val connection = DriverManager.getConnection("ojai:anicolaspp:mem", options)
```

In Java `JavaOjaiTesting` also provides a easy access for creating connections with options.
```Java
public class Test extends JavaOjaiTesting {
    public Test() {
        super(false);
    }

    @Test
    public void testGetConnection() {
        Connection connection = getConnection()
        // ...
    }
}
```

## Cross Build

This project is cross built for Scala `2.11.8` and `2.12.8`. Make sure you select the right version for your Scala.

## Linking

### Maven

```xml
<dependency>
  <groupId>com.github.anicolaspp</groupId>
  <artifactId>ojai-testing_2.X</artifactId>
  <version>1.0.11</version>
</dependency>
```

### SBT

```sbt
libraryDependencies += "com.github.anicolaspp" % "ojai-testing_2.X" % "1.0.11"
```
