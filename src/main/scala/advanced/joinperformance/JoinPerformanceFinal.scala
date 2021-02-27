package advanced.joinperformance

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object JoinPerformanceFinalWoOptimizations extends App {

  val spark = SparkSession
    .builder()
    .appName("ProcessingModel")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
  spark.conf.set("spark.sql.shuffle.partitions", 8)

  //Personas
  val country1 = spark.range(100000).withColumn("country", lit("mexico"))
  val country2 = spark.range(5).withColumn("country", lit("spain"))
  val country3 = spark.range(5).withColumn("country", lit("greece"))
  val country4 = spark.range(1).withColumn("country", lit("tunisia"))

  //Sucursuales
  val bankBrand1 = spark.range(10000).toDF("BBVA").withColumn("country", lit("mexico"))
  val bankBrand2 = spark.range(1).toDF("Caixa").withColumn("country", lit("spain"))
  val bankBrand3 = spark.range(1).toDF("HSBC").withColumn("country", lit("greece"))

  val countriesDF = (country1 union country2 union country3 union country4)

  val brandDF = (bankBrand1 union bankBrand2 union bankBrand3)

  // countriesDF left join brandDF by country
  val result =

  result.write.format("noop").mode("overwrite").save

  Thread.sleep(800000)

}


