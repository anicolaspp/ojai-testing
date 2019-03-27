# ojai-testing
Embeded In-Memory Implementation of OJAI Driver to be used in testing

The aim of this project is to provide to a way of running an OJAI Document Store in memory that can be used for mocking 
MapR Database when running test suits.

In order to use our in memory store we should first register the correct driver as follow.

```scala
 DriverManager.registerDriver(InMemDriver)
```

Then we can get a `Connection` and a `DocumentStore` as we normally do when connecting to a MapR Database table.

```scala
val connection = DriverManager.getConnection("ojai:anicolaspp:mem")

val store = connection.getStore("anicolaspp/mem")
```
The `storeName` being passed is must be `anicolaspp/mem`.

After we have gained access to the `DocumentStore` we should be able to run OJAI queries on it. 

```scala
store
  .find()
  .asScala
  .foreach(println)
```
