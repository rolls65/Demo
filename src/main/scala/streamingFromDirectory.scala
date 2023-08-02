package org.demo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object streamingFromDirectory extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder()
    .master("local[2]")
    .appName("My Streaming Application")
    .config("spark.sql.shuffle.partitions", 3)
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.streaming.schemaInference", "true").getOrCreate()

  val stockDf = spark.readStream
    .format("json")
    .option("path", "input").load()


  stockDf.createOrReplaceTempView("stockdata")
  val companylocation = spark.sql("select * from stockdata where headquarters = 'London'")


  val query = companylocation.writeStream
    .format("json")
    .outputMode("append")
    .option("path", "output")
    .option("checkpointLocation", "checkpoint-location7")
    .trigger(Trigger.ProcessingTime("30 seconds"))
    .start()

  query.awaitTermination()

}
