package com.fastdata.initilization

import java.util.Properties
import scala.io.Source._
import com.fastdata.persistence._
import com.fastdata.streaming._
import java.io.BufferedReader
import java.io.InputStream
import java.io.InputStreamReader

object ConfigurationLoader {

  val component_props,persistence_db_props,stream_processor_props,stream_data_pipeline_props,lookup_db_props = new Properties()
   var p_service: PersistenceService = null
   var s_service: StreamingService = null

  component_props.load(fromURL(Thread.currentThread.getContextClassLoader.getResource("component.properties")).bufferedReader())
  val persistence_db = component_props.getProperty("persistence_db")
  val lookup_db = component_props.getProperty("lookup_db")
  val stream_processor = component_props.getProperty("stream_processor")
  val stream_data_pipeline = component_props.getProperty("stream_data_pipeline")
  
  persistence_db_props.load(fromURL(Thread.currentThread.getContextClassLoader.getResource("persistence.properties")).bufferedReader())
  lookup_db_props.load(fromURL(Thread.currentThread.getContextClassLoader.getResource("lookup.properties")).bufferedReader())
  p_service = Class.forName("com.fastdata.persistence."+persistence_db+"Service").newInstance().asInstanceOf[PersistenceService]

  stream_processor_props.load(fromURL(Thread.currentThread.getContextClassLoader.getResource("consumer.properties")).bufferedReader())
  s_service = Class.forName("com.fastdata.streaming."+stream_processor+stream_data_pipeline+"StreamService").newInstance().asInstanceOf[StreamingService]
  
  

}