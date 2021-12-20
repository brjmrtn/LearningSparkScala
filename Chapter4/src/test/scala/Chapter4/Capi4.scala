package Chapter4

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


/**
 * @author ${user.name}
 */

object Capi4 {

  def main(arg : Array[String]): Unit = {

    val spark:SparkSession = SparkSession.builder().master("local[1]")
      .appName("Chapter4")
      .getOrCreate()
    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

    val csvPath = "data/departuredelays.csv"

    val df_departure_delays = spark.read.format("csv")
      .option("InferSchema", "true")
      .option("header", "true")
      .load(csvPath)

    df_departure_delays.createOrReplaceTempView("us_delay_table")

    //spark.sql libro
    spark.sql(
      """SELECT distance, origin, destination FROM us_delay_table WHERE distance > 1000 ORDER by distance DESC""").show(10)

    spark.sql(
      """SELECT date, delay, origin, destination
        |FROM us_delay_table
        |WHERE delay > 120 AND origin = 'SFO' AND DESTINATION = 'ORD'
        |ORDER by delay DESC """.stripMargin).show(10)

    spark.sql(
      """SELECT delay, origin, destination,
        |CASE
        | WHEN delay > 360 THEN 'Very Long Delays'
        | WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
        | WHEN delay > 60 AND delay < 120 THEN 'Short Delays'
        | WHEN delay > 0 AND delay < 0 THEN 'Tolerable Delays'
        | WHEN delay = 0 THEN 'No Delays'
        | ELSE 'Early'
        |END AS Flight_Delays
        |FROM us_delay_table
        |ORDER BY origin, delay DESC""".stripMargin).show(10)

    //Spark DF
    df_departure_delays.select(col("distance"), col("origin"), col("destination"))
      .where(col("distance") > 1000)
      .orderBy(desc("delay")).show(10)

    df_departure_delays.select(col("date"), col("delay"), col("origin"), col("destination"))
      .where(col("delay") > 120)
      .where(col("origin") === "SFO" && col("destination") === "ORD")
      .orderBy(desc("delay")).show(10)

    df_departure_delays.withColumn("Flight Delays", when(col("delay") > 360, "Very Long Delay")
      .when(col("delay") <= 360 && col("delay") > 120, "Long Delay")
      .when(col("delay") <= 120 && col("delay") > 60, "Short Delay")
      .when(col("delay") <= 60 && col("delay") > 0, "Tolerable Delay")
      .otherwise("No Delay"))
      .orderBy(desc("delay")).show(false)

    // Create DataBase and use

    spark.sql("CREATE DATABASE learn_spark_db")
    spark.sql("USE learn_spark_db")

    // Create Table with departuredelay
    spark.sql("""CREATE TABLE flightdelays(date STRING, delay INT,
                         distance INT, origin STRING, destination STRING)
                         USING csv OPTIONS (PATH
                         'data/departuredelays.csv')""")

    //Create table using DF API
    //(df_departure_delays
    // .write
    // .option("path", "/tmp/data/us_flights_delay")
    // .saveAsTable("us_delay_flights_tbl")

    // Creating a managed table SQL
    //spark.sql("CREATE TABLE managed_us_delay_flights_tbl(date STRING, delay INT, distance INT, origin STRING, destination STRING)")

    //Save

    val dfSave = spark.sql("SELECT origin FROM us_delay_table")
    var pathCsv= "file:///Users/borja.martin/Desktop/data/csv"
    var pathParquet = "file:///Users/borja.martin/Desktop/data/parquet"
    var pathAvro = "file:///Users/borja.martin/Desktop/data/avro"

    dfSave.write.mode("overwrite").format("csv").option("header", "true").save(pathCsv)
    dfSave.write.mode("overwrite").format("parquet").option("header", "true").save(pathParquet)
    dfSave.write.mode("overwrite").format("avro").option("header", "true").save(pathAvro)


  }

}
