
package com.github.akshay.spark.teikametrics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import scala.io.Source

object TeikametricsProcessor {

  def main(args: Array[String]) =
  {
    val spark = SparkSession.builder.appName("Teikametrics").getOrCreate()
    val configMap=Source.fromFile("application.properties").getLines().filter(line => line.contains("=")).map{ line => val tkns=line.split("=")
      if(tkns.size==1){
        (tkns(0) -> "" )
      }else{
        (tkns(0) ->  tkns(1))
      }
    }.toMap

    val env = Environment.DEV
    val fileService = FileServiceFactory.instance(env)
    if (fileService.isEmpty) throw new NullPointerException("Instance of File Service not found")
    val loader = new TeikametricsLoader()

    println("\n\n>>>>> START OF PROGRAM <<<<<\n\n");


    //Loading the raw data into Parquet Format
    val ad_reportDF = loader.createInputDF(spark, configMap("ad_report"),fileService.get)
    loader.writeDFParquet(spark,configMap("storageDataPath"),ad_reportDF,configMap("ad_report_tlb"))

    val keyword_reportDF = loader.createInputDF(spark, configMap("keyword_report"),fileService.get)
    loader.writeDFParquet(spark,configMap("storageDataPath"),keyword_reportDF,configMap("keyword_report_tlb"))

    val product_reportDF = loader.createInputDF(spark, configMap("product_report"),fileService.get)
    loader.writeDFParquet(spark,configMap("storageDataPath"),product_reportDF,configMap("product_report_tlb"))

    //Processing the data

    val ad_reportDFAggDF = ad_reportDF.withColumn("Week",weekofyear(col("date"))).select(col("asin"),col("date"),col("sales"),col("ad_spend"), col("clicks"),col("impressions"),col("week")).groupBy("week","asin").agg((sum("Sales")).alias("Weekly_Sale"),(sum("sales")/sum("ad_spend")).alias("Sales/Ad-spend_ratios"), (sum("clicks")/sum("impressions")).alias("clicks/impressions_ratios"),count("asin").alias("Total_Volume")).orderBy(asc("week"),desc("Weekly_Sale"),desc("Total_Volume"))

    val WeeklySales = ad_reportDFAggDF.join(product_reportDF,ad_reportDFAggDF("asin")===product_reportDF("asin")).drop(product_reportDF("asin")).drop(ad_reportDFAggDF("asin"))

    //Saving the WeeklySales processed Data
    loader.ProcessedDF(spark,configMap("processedDataPath"),WeeklySales,configMap("weeklysales_tlb"))

    //Saving the WeeklyDataWithHighestSales processed Data
    import spark.implicits._
    val WeeklyDataWithHighestSales = WeeklySales.withColumn("rank", row_number().over(Window.partitionBy($"week").orderBy($"Weekly_Sale".desc))).select(col("*")).where(col("rank")===1).orderBy(asc("week"))
    loader.ProcessedDF(spark,configMap("processedDataPath"),WeeklyDataWithHighestSales,configMap("WeeklyDataWithHighestSales_tlb"))

    //Weekly sales with keywords intermeediate
    val weekDF =  ad_reportDF.withColumn("Week",weekofyear(col("date"))).select(col("asin"),col("date"),col("sales"),col("ad_spend"),col("clicks"),col("impressions"),col("week"),col("keyword_id")).groupBy("week","asin","keyword_id").agg((sum("Sales")).alias("Weekly_Sale"),(count("keyword_id")).alias("totalKeyword")).orderBy(asc("week"))
    val weekProdDF = weekDF.join(product_reportDF,ad_reportDFAggDF("asin")===product_reportDF("asin")).drop(product_reportDF("asin")).drop(ad_reportDFAggDF("asin"))
    val KeywordLedMostSales = weekProdDF.join(keyword_reportDF,Seq("keyword_id"), "left").drop(keyword_reportDF("keyword_id")).drop(keyword_reportDF("ad_group"))

    //Saving the KeywordLedMostSales processed Data
    loader.ProcessedDF(spark,configMap("processedDataPath"),KeywordLedMostSales,configMap("KeywordLedMostSales_tlb"))


    println("\n\n>>>>> END OF PROGRAM <<<<<\n\n")
  }


}

