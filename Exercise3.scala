package Week6

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Exercise3 {

  case class Number(i: Int, english: String, french: String)

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark =
      SparkSession.builder()
        .appName("Dataset-CaseClass")
        .master("local[4]")
        .getOrCreate()
    import spark.implicits._
    val numbers = Seq(
      Number(1, "one", "un"),
      Number(2, "two", "deux"),
      Number(3, "three", "trois"))
    val numberDS = numbers.toDS()

    println("Dataset Types")
    numberDS.dtypes.foreach(println(_))
    println()

    println("filter dataset where i>1")
    numberDS.filter($"i" > "1").show()

    println("select the number with English column and display")
    numberDS.select($"i", $"english").show()

    println("select the number with English column and filter for i>1")
    numberDS.select($"i",$"english").where($"i" > "1").show()

    println("sparkSession dataset")
    val anotherDS = spark.createDataset(numbers)
    anotherDS.show()

    println("Spark Dataset Types")
    anotherDS.dtypes.foreach(println(_))
    spark.stop()

  }
}
