package Week6

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

//
// Create Datasets of primitive type and tuple type ands show simple operations.
//
object Exercise1c {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val spark =
      SparkSession
        .builder()
        .master("local[2]")
        .appName("Dataset-Basic")
        .getOrCreate()
    import spark.implicits._
    // Create a tiny Dataset of integers
    val s = Seq(10, 11, 12, 13, 14, 15)
    val ds = s.toDS()

    println("*** only one column, and it always has the same name")
    ds.columns.foreach(println(_))

    println("*** column types")
    ds.dtypes.foreach(println(_))

    println("*** schema as if it was a DataFrame")
    ds.printSchema()

    println("*** values > 12")
    ds.where($"value" > 12).show()

    // This seems to be the best way to get a range that's actually a Seq and
    // thus easy to convert to a Dataset, rather than a Range, which isn't.
    val s2 = Seq.range(1, 100)

    println("*** size of the range")
    println(s2.size)

    val tuples = Seq((1, "one", "un"), (2, "two", "deux"), (3, "three", "trois"))
    val tupleDS = tuples.toDS()

    println("*** Tuple Dataset types")
    tupleDS.dtypes.foreach(println(_))

    // the tuple columns have unfriendly names, but you can use them to query
    println("*** filter by one column and fetch another")
    tupleDS.where($"_3"==="un").select($"_1", $"_2").show()
    tupleDS.where("_3='un'").select($"_1", $"_2").show()
  }
}

