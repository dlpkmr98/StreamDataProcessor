package com.fastdata.streaming

import org.apache.spark.streaming.kafka._
import com.fastdata.initilization.LookupDataInitilizer

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.dstream.DStream
import scala.collection.mutable.ListBuffer
import com.fastdata.initilization.ConfigurationLoader

object SparkMultiTopicConsumer extends App with java.io.Serializable {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  println("Spark kafka Streaming...")

  val conf = new SparkConf().setAppName("SparkKafkaStreamService").setMaster("local[*]")
  val ssc = new StreamingContext(conf, Seconds(5))
  val topic_name = "filedata,impdata,cleandata"
  val topicsMap = topic_name.split(",").map((_, 1)).toMap
   KafkaUtils.createStream(ssc, ConfigurationLoader.stream_processor_props.getProperty("zookeeper.connect"),
      ConfigurationLoader.stream_processor_props.getProperty("group.id"),
      topicsMap).map(_._2).foreachRDD(rdd => {

        if (rdd.take(1).length != 0) {
          
          rdd.collect.foreach(println)
          
        }
      })
  
  
  
  ssc.start()
  ssc.awaitTermination()
  
  

}