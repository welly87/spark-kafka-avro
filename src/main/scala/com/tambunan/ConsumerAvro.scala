import java.util
import java.util.{Collections, Properties}

import example.avro.User
import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import collection.JavaConversions._
/**
  * Created by tambunanw on 7/11/16.
  */
object ConsumerAvro {
  def main(args: Array[String]) {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("group.id", "AvroConsumer")
    //https://mail-archives.apache.org/mod_mbox/kafka-users/201605.mbox/%3CCAHwHRrVxTBD1+7sGMHz9L0J=4qzH4c2RmBHs+ijZ_gR+gZZ9kg@mail.gmail.com%3E
    props.put("specific.avro.reader", "true")
    props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("schema.registry.url", "http://localhost:8081")


    val consumer = new KafkaConsumer[String, User](props)

    consumer.subscribe(Collections.singletonList("user"), new ConsumerRebalanceListener {
      override def onPartitionsAssigned(collection: util.Collection[TopicPartition]): Unit = {
        println("partitions assigned", collection.head.topic())

        //        for (partition <- collection) {
        //          consumer.seek(partition, 0)
        //        }

        //        println("seek partition")
      }

      override def onPartitionsRevoked(collection: util.Collection[TopicPartition]): Unit = {

      }
    })

    while (true) {
      val records = consumer.poll(100)
      for (record <- records) {
        println(record.value().getRegistertime)
      }

      consumer.commitSync()
    }
  }
}