package advanced.dynamic

import org.apache.spark.sql.SparkSession

object DynamicPartitioning extends App {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("DynamicInsert")
    .master("local")
    .getOrCreate()


  val inputDF = spark.read.json("src/main/resources/datalake-input/")

  inputDF
    .coalesce(1)
    .write
    .partitionBy("date")
    .mode("overwrite")
    .json("src/main/resources/Datalake/")

}



//  spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
