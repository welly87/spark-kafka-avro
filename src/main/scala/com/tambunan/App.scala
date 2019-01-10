package com.tambunan

import org.apache.spark.sql.SparkSession
/**
 * Hello world!
 *
 */
object App {

  def main(args: Array[String]): Unit = {
    val schemaRegistryAddr = "http://localhost:8081"
    val servers = "localhost:9092"

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("StructuredNetworkWordCount")
      .getOrCreate()

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", servers)
      .option("subscribe", "t")
      .load()

    val query = df.writeStream
//      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()

//      .select(
//        from_avro("key", "t-key", schemaRegistryAddr).as("key"),
//        from_avro("value", "t-value", schemaRegistryAddr).as("value"))


  }
}
