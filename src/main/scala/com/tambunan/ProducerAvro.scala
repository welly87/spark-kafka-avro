package com.tambunan
import java.util.Properties

import example.avro.User
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object Producer2Avro {
  def main(args: Array[String]): Unit = {
    val kafkaProps = new Properties()
    kafkaProps.put("bootstrap.servers", "localhost:9092")
    kafkaProps.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer")
    kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    kafkaProps.put("schema.registry.url", "http://localhost:8081")

    val producer = new KafkaProducer[String, User](kafkaProps)

    for (a <- 1 to 1) {
      val record = new ProducerRecord[String, User]("user", ""+a, new User(1234L, "welly", "1", "MALE"))
      try {
        println("sending")
        val metadata = producer.send(record).get()

        println(metadata.offset())

      } catch {
        case x: Exception => {
          x.printStackTrace()
        }
        case _ => println("error")
      }
    }


  }
}
