package com.tambunan

import java.io.FileOutputStream
import java.util
import java.util.{Collections, Properties}

import example.avro.User
import org.apache.avro.io.DecoderFactory
import org.apache.avro.specific.SpecificDatumReader
import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConversions._

/**
  * Created by tambunanw on 7/11/16.
  */
object ConsumerByteArrayAvro {
  def main(args: Array[String]) {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("group.id", "AvroConsumer")
    //https://mail-archives.apache.org/mod_mbox/kafka-users/201605.mbox/%3CCAHwHRrVxTBD1+7sGMHz9L0J=4qzH4c2RmBHs+ijZ_gR+gZZ9kg@mail.gmail.com%3E
    props.put("specific.avro.reader", "true")
    props.put("auto.offset.reset", "earliest")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("schema.registry.url", "http://localhost:8081")


    val consumer = new KafkaConsumer[String, Array[Byte]](props)

    consumer.subscribe(Collections.singletonList("user"))

    while (true) {
      val records = consumer.poll(100)
      for (record <- records) {
//            val out = new FileOutputStream("User2.avro")
//            out.write(record.value)
//            out.flush()
//            out.close()
            println(record.value().size)

        val reader = new SpecificDatumReader[User](classOf[User])
        val decoder = DecoderFactory.get.binaryDecoder(record.value(), null)
        val user2 = reader.read(null, decoder)

        println(user2.getRegistertime)
      }

      consumer.commitSync()
    }
  }
}