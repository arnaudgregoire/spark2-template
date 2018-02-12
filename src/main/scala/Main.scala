import spark_helpers.SparkSessionHelper

object
Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionHelper.buildSession()
    val sparkVersion = spark.version
    sql_practice.examples.exec1()
    println(s"Spark Version: $sparkVersion")
  }
}
