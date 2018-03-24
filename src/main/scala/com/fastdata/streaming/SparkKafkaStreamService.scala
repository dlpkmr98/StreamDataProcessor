package com.fastdata.streaming

import com.fastdata.persistence._
import com.fastdata.initilization.ConfigurationLoader
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

class SparkKafkaStreamService extends StreamingService with java.io.Serializable {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  override def startStream(db_service: PersistenceService) {
    println("Spark kafka Streaming...")

    val meter_price = db_service.getLookupData    

    val conf = new SparkConf().setAppName("SparkKafkaStreamService").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))
    val topicsSet = ConfigurationLoader.stream_processor_props.getProperty("topic.name").split(",").map((_, 1)).toMap
    //   val kafkaParams = Map[String, String]("metadata.broker.list" -> ConfigurationLoader.stream_processor_props.getProperty("bootstrap.servers"),
    //       "zookeeper.connect" ->  ConfigurationLoader.stream_processor_props.getProperty("zookeeper.connect"),
    //       "group.id" ->  ConfigurationLoader.stream_processor_props.getProperty("group.id"))
    // val readings = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    KafkaUtils.createStream(ssc, ConfigurationLoader.stream_processor_props.getProperty("zookeeper.connect"),
      ConfigurationLoader.stream_processor_props.getProperty("group.id"),
      topicsSet).map(_._2).foreachRDD(rdd => {

        if (rdd.take(1).length != 0) {
          val meterRDD = rdd.map(x => {
            val split = x.split(",")
            val price = if (split.contains("standard")) meter_price._1.toString().toDouble else meter_price._2.toString().toDouble
            var u_price: Double = split(3).toInt * price
            MeterReading(split(0).toInt, split(1), split(2), split(3).toInt, split(4).toInt, u_price, 0.0, "N")
          })

         // meterRDD.collect().foreach(println)
          val avg_price = meterRDD.map(x => (x.zip_code, ListBuffer(x.u_price))).keyBy(_._1)
            .reduceByKey((x, y) => (x._1, (x._2 ++= y._2))).map(x => (x._1, x._2._2.sum / x._2._2.size))

          val final_output = meterRDD.keyBy(_.zip_code).join(avg_price).mapValues(x => {
            MeterReading(x._1.meter_id, x._1.tariff, x._1.timestamp, x._1.meter_usage, x._1.zip_code, x._1.u_price, x._2, if (x._1.u_price > x._2) "Y" else "N")
          }).values

          final_output.collect().foreach(println)
          final_output.foreach(x => { db_service.setData(x) })

        }

      })

    // Start the computation
    ssc.start()
    ssc.awaitTermination()

  }
}