package com.fastdata.persistence

import com.fastdata.streaming.MeterReading
import org.apache.hadoop.hbase.io._
import org.apache.hadoop.hbase.util._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import com.fastdata.initilization.ConfigurationLoader

object HbaseFactory {

  val tableName = ConfigurationLoader.persistence_db_props.getProperty(ConfigurationLoader.persistence_db + ".persistence_collection").toString

  // set up Hadoop HBase configuration using TableInputFormat
  val conf = HBaseConfiguration.create()
  conf.set(TableInputFormat.INPUT_TABLE, tableName)
  val newTable = new HTable(conf, tableName)

  //  Convert a row of meter object data to an HBase put object
  def convertToPut(sensor: MeterReading): (ImmutableBytesWritable, Put) = {

   
    // create a composite row key: sensorid_date time  
    val rowkey = sensor.meter_id + "_" + sensor.timestamp
    val put = new Put(Bytes.toBytes(rowkey))
    // add to column family data, column  data values to put object
    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("meter_id"), Bytes.toBytes(sensor.meter_id))
    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("tariff"), Bytes.toBytes(sensor.tariff))
    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("timestamp"), Bytes.toBytes(sensor.timestamp))
    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("meter_usage"), Bytes.toBytes(sensor.meter_usage))
    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("zip_code"), Bytes.toBytes(sensor.zip_code))
    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("exceed_status"), Bytes.toBytes(sensor.exceed_status))
    return (new ImmutableBytesWritable(Bytes.toBytes(rowkey)), put)
  }

}