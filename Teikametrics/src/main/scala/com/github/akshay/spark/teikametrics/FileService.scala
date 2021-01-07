package com.github.akshay.spark.teikametrics

import org.apache.spark.sql. {
  DataFrame,
  SparkSession
}

trait ServiceFactory[T] {
  protected def map(): Map[Int, T];
  def instance(env: Int): Option[T]=map().get(env);
}
trait FileService {
  def read(spark: SparkSession, filePath: String): DataFrame;
  def write(spark: SparkSession, filePath: String, df: DataFrame);
}

object FileServiceFactory extends ServiceFactory[FileService] {
  override protected def map(): Map[Int, FileService]=Map(
    Environment.DEV -> FileServiceCSV,
    Environment.LOCAL -> FileServiceDummy,
    Environment.PROD -> FileServiceS3Parquet)
}

object FileServiceS3Parquet extends FileService {
  override def read(spark: SparkSession, filePath: String): DataFrame= {
    spark.read.parquet(filePath)
  }
  override def write(spark: SparkSession, filePath: String, df: DataFrame): Unit= {
    df.write.parquet(filePath)
  }
}

object FileServiceCSV extends FileService {
  override def read(spark: SparkSession, filePath: String): DataFrame= {
    spark.read.format("csv").option("header",true).option("inferSchema",true).load(filePath)
  }
  override def write(spark: SparkSession, filePath: String, df: DataFrame): Unit= {
    df.write.csv(filePath)
  }
}

object FileServiceDummy extends FileService {
  override def read(spark: SparkSession, filePath: String): DataFrame= {
    import spark.implicits._
    Seq( ("1", "Tom"), ("2", "Dick"), ("3", "Harry")).toDF("id", "name")
  }
  override def write(spark: SparkSession, filePath: String, df: DataFrame): Unit= {
    df.show(false)
  }
}