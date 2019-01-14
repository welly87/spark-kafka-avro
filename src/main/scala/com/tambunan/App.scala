package com.tambunan

import java.sql.Timestamp

import example.avro.User
import org.apache.avro.io.DecoderFactory
import org.apache.avro.specific.SpecificDatumReader
import org.apache.spark.sql.SparkSession
/**
 * Hello world!
 *
 */

case class KafkaMessage(key: Array[Byte], value: Array[Byte], topic: String, partition: Int, offset: Long, timestamp: Timestamp, timestampType: Int)

case class WebUser(registertime: Long, userId: String, region_id: String, gender: String)

object App {

  def deserialize(message: KafkaMessage): WebUser = {
    println(message.value.length)

    val userReader = new SpecificDatumReader[User](classOf[User])
    //Schema.parse(new File("/home/andal/spark-kafka-avro/src/main/resources/user.avsc"))
    val decoder = DecoderFactory.get.binaryDecoder(message.value, null)
    val user = userReader.read(null, decoder)
    println("user => " + user)

    return WebUser(user.getRegistertime, user.getUserid.toString, user.getRegionId.toString, user.getGender.toString)
  }

  def main(args: Array[String]): Unit = {
    val schemaRegistryAddr = "http://localhost:8081"
    val servers = "localhost:9092"

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("StructuredNetworkWordCount")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", servers)
      .option("subscribe", "user")
      .option("startingOffsets", "earliest")
      .load()


    import spark.implicits._

    val ds = df.as[KafkaMessage].filter(x => x.value != null).map(x => deserialize(x))

    val query = ds.writeStream
      //      .outputMode("complete")
      .format("console")
      .start()

//    val query = df.writeStream
////      .outputMode("complete")
//      .format("console")
//      .start()

    query.awaitTermination()
  }
}
