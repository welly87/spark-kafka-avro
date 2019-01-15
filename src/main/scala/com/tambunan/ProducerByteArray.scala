package com.tambunan

import java.io.ByteArrayOutputStream
import java.util.Properties

import example.avro.User
import org.apache.avro.io.EncoderFactory
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object ProducerByteArray {

  def serialize(user: User): Array[Byte] = {
    val writer = new SpecificDatumWriter[User](User.getClassSchema)
    val out = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get.binaryEncoder(out, null)
    writer.write(user, encoder)
    encoder.flush()

    return out.toByteArray
  }

  def main(args: Array[String]): Unit = {
    val kafkaProps = new Properties()
    kafkaProps.put("bootstrap.servers", "localhost:9092")
    kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    kafkaProps.put("schema.registry.url", "http://localhost:8081")

    val producer = new KafkaProducer[String, Array[Byte]](kafkaProps)

    for (a <- 1 to 1) {
      val byteArray = serialize(new User(1234L, "welly", "1", "MALE"))
      val record = new ProducerRecord[String, Array[Byte]]("user", ""+a, byteArray)
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
