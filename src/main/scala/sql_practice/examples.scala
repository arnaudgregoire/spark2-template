package sql_practice

import org.apache.spark
import org.apache.spark.sql.functions._
import spark_helpers.SparkSessionHelper

object examples {
  def exec1(): Unit = {
    val spark = SparkSessionHelper.getSparkSession()
    val tourDF = spark.read.option("multiline", true).option("mode", "PERMISSIVE").json("/home/prof/Bureau/learning-redis/tours.json")
    spark.sparkContext.setLogLevel("WARN")
    tourDF.show()
    tourDF.printSchema()
    println("The number of tours is :" + tourDF.count())
    val counted = tourDF.groupBy("tourDifficulty").count()
    println("The number of difficulty is : " + counted.count())

    // Aggregate to have max / min /mean of all tables
    tourDF.agg(max("tourPrice"),
      min("tourPrice"),
      mean("tourPrice"))
      .show()

    // Agregate like question before, but grouped by tourDifficulty, sorted by ascending order of min price
    tourDF.groupBy("tourDifficulty")
      .agg(max("tourPrice"),
        min("tourPrice"),
        mean("tourPrice"))
      .sort("min(tourPrice)")
      .show()

    // min max avg for tourlength and tour duration sort by ascending avg tour length grouped by tourDifficulty
    tourDF.groupBy("tourDifficulty")
      .agg(max("tourPrice")
        , min("tourPrice")
        , mean("tourPrice")
        , min("tourLength")
        , max("tourLength")
        , mean("tourLength"))
      .sort("avg(tourLength)")
      .show()

  // top ten of most used tags
    tourDF
      .select(explode(tourDF("tourTags")))
      .groupBy("col")
      .count()
      .sort(desc("count"))
      .show(10)

    // Numbers of unique tags
    println(tourDF
      .select(explode(tourDF("tourTags")))
      .groupBy("col")
      .count()
      .sort(desc("count"))
      .count()
    )

  // Relationship between top 10 tourTags and tourDifficulty
    tourDF
      .select(explode(tourDF("tourTags")),tourDF("tourDifficulty"))
      .groupBy("tourDifficulty","col")
      .count()
      .sort(desc("count"))
      .show(10)

    // min max abg of price in tour tags and tourDifficuty relationship sort by avg
    tourDF
      .select(explode(tourDF("tourTags")),tourDF("tourDifficulty"),tourDF("tourPrice"))
      .groupBy("tourDifficulty","col")
      .agg(max("tourPrice"), min("tourPrice"), mean("tourPrice"))
        .sort(desc("avg(tourPrice)"))
      .show(10)
  }
}
