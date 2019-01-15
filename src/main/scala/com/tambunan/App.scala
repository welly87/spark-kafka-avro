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
    val reader = new SpecificDatumReader[User](User.getClassSchema)
    val decoder = DecoderFactory.get.binaryDecoder(message.value, null)
    val user = reader.read(null, decoder)
    return WebUser(user.getRegistertime, user.getUserid.toString, user.getRegionId.toString, user.getGender.toString)
  }

  def main(args: Array[String]): Unit = {
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
//      .option("startingOffsets", "earliest")
      .load()


    import spark.implicits._

    val ds = df.as[KafkaMessage].filter(x => x.value != null).map(x => deserialize(x))

    val query = ds.writeStream
      .format("console")
      .start()

    query.awaitTermination()
  }
}
