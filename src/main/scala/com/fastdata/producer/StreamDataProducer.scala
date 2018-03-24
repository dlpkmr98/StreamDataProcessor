/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.fastdata.producer

import java.io.File
import java.io.FileNotFoundException
import java.io.IOException
import java.nio.charset.Charset
import org.apache.kafka.clients.producer._
import scala.io.Source
import scala.io.Source._
import util.control.Breaks._

object StreamDataProducer extends App {

  val producer = new KafkaProducer[String, String](PropertiesTemplate.props)
  val dir = new File(PropertiesTemplate.dirPath)
  if (dir.isDirectory()) {
    val files: Array[File] = dir.listFiles();
    for (file: File <- files) {
      if (file.isFile()) {
        val UTF8 = Charset.forName("UTF-8");
        try {
          for (line <- Source.fromFile(file.getAbsolutePath).getLines()) { 
            val i=0
            for(i <- 1 to 1){
            if (line == null) break
            if (line.equals("")) {
            } else {
              val record = new ProducerRecord[String, String](PropertiesTemplate.topic, line);
              producer.send(record, new Callback {
                override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
                  if (exception != null) {
                    println("Send failed for record {}" + record.value() + " " + exception);
                  } else {
                    println("send successfully…." + record.value());
                  } } }) }} } } catch {
          case ex: FileNotFoundException => println("Couldn't find that file.")
          case ex: IOException => println("Had an IOException trying to read that file")
        } }}}
  
  producer.close
}

