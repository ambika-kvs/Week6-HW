package Week6

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Exercise1d {
  case class Record(key: Int, value: String)

  def main(args: Array[String]): Unit = {
    // $example on:init_session$
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val spark = SparkSession
      .builder
      .master("local[2]")
      .appName("Spark Examples")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    // Importing the SparkSession gives access to all the SQL functions and implicit conversions.
    import spark.implicits._
    // $example off:init_session$

    val df = spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))

    // Any RDD containing case classes can be used to create a temporary view.  The schema of the
    // view is automatically inferred using scala reflection.

    df.createOrReplaceTempView("records")
    // Once tables have been registered, you can run SQL queries over them.

    println("Result of SELECT *:")
    spark.sql("SELECT * FROM records").collect().foreach(println)

    // Aggregation queries are also supported.
    val count = spark.sql("SELECT COUNT(*) FROM records").collect().head.getLong(0)
    println(s"COUNT(*): $count")

    // The results of SQL queries are themselves RDDs and support all normal RDD functions. The
    // items in the RDD are of type Row, which allows you to access each column by ordinal.
    val rddFromSql = spark.sql("SELECT key, value FROM records WHERE key < 10")

    println("Result of RDD.map:")
    rddFromSql.rdd.map(row => s"Key: ${row(0)}, Value: ${row(1)}").collect().foreach(println)

    // Queries can also be written using a LINQ-like Scala DSL.
    df.where($"key" === 1).orderBy($"value".asc).select($"value").collect().foreach(println)

    df.where($"key" === 1).orderBy($"value".asc).select($"key").collect().foreach(println)

    spark.stop()
  }
}

