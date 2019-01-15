package com.tambunan

import java.io.{ByteArrayOutputStream, FileInputStream}

import example.avro.User
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter}

object SerDeMain {
  def main(args: Array[String]): Unit = {
//    val user = new User(1234L, "welly", "1", "MALE")
//
//    val writer = new SpecificDatumWriter[User](User.getClassSchema)
//    val out = new ByteArrayOutputStream()
//    val encoder = EncoderFactory.get.binaryEncoder(out, null)
//    writer.write(user, encoder)
//    encoder.flush()
//
//    println(out.size())
//
////    out.flush()
//
//    println(out.toByteArray.length)

    val in = new FileInputStream("User2.avro")
    val arr = new Array[Byte](in.available())

    in.read(arr, 0, in.available())

    val reader = new SpecificDatumReader[User](classOf[User])
    val decoder = DecoderFactory.get.binaryDecoder(arr, null)
    val user2 = reader.read(null, decoder)

    println(user2.getRegistertime)
  }
}
