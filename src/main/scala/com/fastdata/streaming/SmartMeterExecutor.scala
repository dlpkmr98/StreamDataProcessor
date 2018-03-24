package com.fastdata.streaming

import com.fastdata.persistence._
import com.fastdata.initilization._

object SmartMeterExecutor extends App {

  //find appropriate db service
  println("find appropriate db service")
   def db_service: PersistenceService= ConfigurationLoader.p_service
  
  //find appropriate streaming service
  println("find appropriate streaming service")
  def s_service: StreamingService = ConfigurationLoader.s_service

  //load lookup data into memory{can load any type of data into memory-lookup_collection_map = HashMap[String, Any]()}
  println("load lookup data into memory")
  LookupDataInitilizer.initilizeLookupData(db_service)

  //call appropriate streaming service {depends on find meter usage status use case}
  println("call appropriate streaming service")
  s_service.startStream(db_service)

}