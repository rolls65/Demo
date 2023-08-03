package org.demo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}
import org.apache.spark.sql.functions.{col, from_json}

object streamingFromDirectory extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder()
    .master("local[2]")
    .appName("Data Streaming Stock Data")
    .config("spark.sql.shuffle.partitions", 3)
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .enableHiveSupport()
    .config("spark.sql.streaming.schemaInference", "true").getOrCreate()
  val dfstockcompanies = spark.read.option("header", true).
    option("inferSchema", true).csv("C:\\Users\\rolls\\Downloads\\BigDataProject\\stockcompanies.csv")
  val dfstockprice = spark.read.option("header", true).
    option("inferSchema", true).csv("C:\\Users\\rolls\\Downloads\\BigDataProject\\stockprice.csv")
  //create stockdata

  var stockcompanyView = dfstockcompanies.createTempView("stock_company")
  var stockpriceView = dfstockprice.createTempView("stock_price")
  //store stock data with trading year and month
  val query = "select trading_year, trading_month, sc.code as company_code,name as company_name,trim(SUBSTRING_INDEX(Headquarter,';',1)) as headquarters, sector, sub_industry,open as opening, close as closing, low, high, volume from stock_company sc " +
    "inner join (select code,EXTRACT(YEAR FROM to_date(trading_date,'dd/MM/yyyy')) as trading_year,  EXTRACT(MONTH FROM to_date(trading_date,'dd/MM/yyyy')) as trading_month,round(avg(open),2) open, round(avg(close),2) close, round(avg(low),2) low, round(avg(high),2) high,round(avg(volume),2) volume from stock_price group by code,trading_year,trading_month) sp on sc.code=sp.code"
  val dfstockdatacsv = spark.sql(query)
  val stockdatacsvView = dfstockdatacsv.createTempView("stock_datacsv")

  val stockschema = new StructType()
    .add("trading_year", IntegerType, true)
    .add("trading_month", IntegerType, true)
    .add("company_code", StringType, true)
    .add("company_name", StringType, true)
    .add("headquarters", StringType, true)
    .add("sector", StringType, true)
    .add("sub_industry", StringType, true)
    .add("opening", DoubleType, true)
    .add("closing", DoubleType, true)
    .add("low", DoubleType, true)
    .add("high", DoubleType, true)
    .add("volume", DoubleType, true)

  val stockstreamDf = spark.readStream
    .schema(stockschema)
    .format("json")
    .option("MaxFilesPerTrigger","2")
    .option("path", "input")
    .load()

val stockstreamView = stockstreamDf.createOrReplaceTempView("stockstreamdata")

  //val dfstockdata = stockstreamDf.toDF().union(dfstockdatacsv.toDF())
  //val left_df = stockstreamDf.join(dfstockcompanies, (stockstreamDf("company_code") === dfstockcompanies("code")), "full_outer")
  val querydf = spark.sql("select company_name,company_code,trading_year,trading_month,(((closing - opening)/opening)*100.00) as growthrate  " +
       "from stockstreamdata")

  writeStreamData(querydf)

  stockstreamDf.repartition(numPartitions = 1).write.mode(SaveMode.Overwrite).saveAsTable("stockstreameddata")

  def writeStreamData(dataFrame: DataFrame): Unit = {

    dataFrame.writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("15 Seconds"))
      .start()
      .awaitTermination()
  }
}
