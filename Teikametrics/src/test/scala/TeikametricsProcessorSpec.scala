package com.github.akshay.spark.teikametrics
import com.github.akshay.spark.teikametrics.TeikametricsProcessor.doSomeThing
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec



class SampleJobTest extends AnyFlatSpec {
  "doSomeThing" should "Do Some Thing" in {
    val jobName = "SampleJob"
    val filePath = "Some file Path"
    val topic = "sample-topic"
    val env = Environment.LOCAL
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName(jobName)
      .getOrCreate()

    val fileService = FileServiceFactory.instance(env)
    if (fileService.isEmpty) throw new NullPointerException("Instance of File Service not found")
    val outputDF = doSomeThing(spark, filePath, fileService.get)
    import spark.implicits._
    val expectedDF = Seq(
      ("1", "Tom")
    ).toDF("id", "name")
    assertDF(spark, outputDF, expectedDF)
  }
  def assertDF(spark: SparkSession, actualDF: DataFrame, expectedDF: DataFrame): Unit = {
    val actual = dfToStringSeqArray(spark, actualDF)
    val expected = dfToStringSeqArray(spark, expectedDF)
    if (actual.length != expected.length) assert(false)
    actual.zip(expected)
      .foreach(actualExpected => {
        if (actualExpected._1.length != actualExpected._2.length) assert(false)
        actualExpected._1.zip(actualExpected._2).foreach(zipValue => assert(zipValue._1 == zipValue._2))
      })
  }
  private def dfToStringSeqArray(spark: SparkSession, x: DataFrame): Array[Seq[String]] = {
    import spark.implicits._
    x.map {
      row => row.toSeq.map(value => value.asInstanceOf[String])
    }
      .collectAsList().toArray.asInstanceOf[Array[Seq[String]]]
  }
}