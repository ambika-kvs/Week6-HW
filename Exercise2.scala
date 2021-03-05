package Week6

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Exercise2 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val session = SparkSession
      .builder
      .master("local[4]")
      .appName("ReadCSV")
      .getOrCreate()

    import session.implicits._


    // 1.To read the CSV file
    println("1. Reading insurance.csv file")
    val insurance = session.read.option("header", value = true).
      csv("./src/main/insurance.csv")
    insurance.show(10)

    //2.To print the size of the dataset
    println("2. Size of the insurance dataset = " + insurance.count()+" rows X "+insurance.columns.length+" columns")
    println()

    //3.Print sex and count of sex
    println("3. Printing the sex and its count")
    insurance.groupBy("sex").count().show()

    //4.Filter smoker=yes and print again the sex,count of sex
    println("4. Printing the smoker's sex and its count")
    insurance.where($"smoker" === "yes").groupBy("sex").count().show()

    //5. To print the records in descending order based on region and sum of the charges by each region
    println("5. Printing sum of the charges for each region in descending order")
    insurance.createOrReplaceTempView("InsuranceView")
    session.sql("SELECT region as Region, sum(charges) as TotalCharge FROM InsuranceView group by region " +
      "order by sum(charges) desc").show()

    session.stop()

  }
}
