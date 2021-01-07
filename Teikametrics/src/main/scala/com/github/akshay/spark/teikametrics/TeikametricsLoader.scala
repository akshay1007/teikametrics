package com.github.akshay.spark.teikametrics

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.{DataFrame, SparkSession}

class TeikametricsLoader {

  def createInputDF(spark: SparkSession, filePath: String,fileService: FileService): DataFrame = {
    val inputDF = fileService.read(spark,filePath)
    inputDF
  }

  def writeDFParquet(spark: SparkSession, filePath: String,df:DataFrame,tableName:String):Unit = {
    //Get the current timestamp
    val format = "yyyyMMdd_HHmmss"
    val dtf = DateTimeFormatter.ofPattern(format)
    val ldt = LocalDateTime.now().format(dtf)

    df.write.format("parquet").save(filePath+tableName+"/"+ldt+"/")
  }

  def ProcessedDF(spark: SparkSession, filePath: String,df:DataFrame,tableName:String):Unit = {
    println("/////////////// Inside the ProcessedDF /////////////////////////////////////")
    df.coalesce(1).write.mode("Overwrite").format("parquet").save(filePath+tableName)
    df.coalesce(1).write.mode("Overwrite").format("csv").save(filePath+"/CSV/"+tableName)

  }


}
