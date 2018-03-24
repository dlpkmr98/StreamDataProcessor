package com.fastdata.streaming

import org.apache.flink.streaming.api.scala._
import com.fastdata.persistence._
import java.util.Properties
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.TimeCharacteristic
import com.fastdata.initilization.ConfigurationLoader
import com.fastdata.initilization.LookupDataInitilizer
import com.mongodb.casbah.MongoCursor
import com.mongodb.casbah.Imports._


 /**
   * An individual Meter reading case class, describing meeter id, tariff,timestamp ,meter usage and zip code.
   */
case class MeterReading(meter_id: Int, tariff: String, timestamp: String, meter_usage: Int,
                          zip_code: Int, u_price: Double, avg_price: Double, exceed_status: String)
class FlinkKafkaStreamService extends StreamingService with java.io.Serializable {

   override def startStream(db_service: PersistenceService) {
     
    val meter_price = db_service.getLookupData  
    // create environment and configure it
    //val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createRemoteEnvironment("localhost",6123)
     val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //env.setParallelism(4)
  
    env.enableCheckpointing(10000)
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    
    // create a stream of meter readings, assign timestamps  
    val kafkaConsumer: FlinkKafkaConsumer09[String] = new FlinkKafkaConsumer09[String](ConfigurationLoader.stream_processor_props.getProperty("topic.name"), new SimpleStringSchema(), ConfigurationLoader.stream_processor_props)

    val readings: DataStream[MeterReading] = env.addSource(kafkaConsumer)(TypeInformation.of(classOf[(String)])).map(x =>
      {
        val split = x.split(",")       
        val price = if (split.contains("standard")) meter_price._1.toString().toDouble else meter_price._2.toString().toDouble
        var u_price: Double = split(3).toInt * price
        MeterReading(split(0).toInt, split(1), split(2), split(3).toInt, split(4).toInt, u_price, 0.0, "N")
      })(TypeInformation.of(classOf[(MeterReading)])).name("Meter Reading Data Stream")

    val avg_stream = readings.map { x => (x.zip_code, x.u_price) }(TypeInformation.of(classOf[(Int, Double)]))
      .keyBy(_._1)(TypeInformation.of(classOf[(Int)]))
      .timeWindow(Time.seconds(5))
      .apply((key: Int, window: TimeWindow, input: Iterable[(Int, Double)], out: Collector[(Int, Double)]) => {
        val avg = input.map(_._2).sum / input.size
        out.collect(key, avg)
      })(TypeInformation.of(classOf[(Int, Double)])).name("Final Meter AVG")
      
    val final_output = readings.keyBy(_.zip_code)(TypeInformation.of(classOf[(Int)]))
      .join(avg_stream)
      .where(_.zip_code)(TypeInformation.of(classOf[(Int)]))
      .equalTo(_._1).window(TumblingEventTimeWindows.of(Time.seconds(5))).apply((x: MeterReading, b: (Int, Double)) => {
        MeterReading(x.meter_id, x.tariff, x.timestamp, x.meter_usage, x.zip_code, x.u_price, b._2, if (x.u_price > b._2) "Y" else "N")
      })(TypeInformation.of(classOf[(MeterReading)])).name("Final Meter Reading")

    final_output.print
    final_output.map(x => { db_service.setData(x) })(TypeInformation.of(classOf[(Unit)])).name("Storage Service")
    
    env.execute("FlinkKafkaStream")
  }

}