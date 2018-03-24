/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.fastdata.initilization

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import com.fastdata.persistence._
import com.fastdata.streaming._


object LookupDataInitilizer{  
  
  val lookup_collection_map = HashMap[String, Any]()
  def initilizeLookupData(db_service:PersistenceService){  
  val lookup_collection = ConfigurationLoader.lookup_db_props.getProperty(ConfigurationLoader.lookup_db+".lookup_source").toString()
   for (coll <- lookup_collection.split(",")) lookup_collection_map += (coll -> db_service.getData(coll))
  }
}
