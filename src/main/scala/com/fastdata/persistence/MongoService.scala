/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.fastdata.persistence

import com.mongodb.casbah.commons.MongoDBObject
import scala.collection.mutable._
import net.liftweb.json._
import net.liftweb.json.Serialization.write
import com.fastdata.streaming.MeterReading
import com.mongodb.casbah.MongoCursor
import com.mongodb.casbah.Imports._
import com.fastdata.initilization.LookupDataInitilizer




class MongoService extends PersistenceService with java.io.Serializable{

  override def setData(obj: Any): Unit = {   
    val builder = MongoDBObject.newBuilder
    implicit val formats = DefaultFormats
    val jsonObj = write(obj.asInstanceOf[MeterReading])   
    builder += "meter_data" -> jsonObj
    MongoFactory.setCollection.save(builder.result)    
   
  }

  override def getData(obj: Any): MongoCursor = {
    var dbObjects = MongoFactory.getCollection(obj.toString()).find()     
    dbObjects
  }
  
   override def getLookupData:(Double,Double) = {
     var sprice, dprice = 0.0
     for (obj <- LookupDataInitilizer.lookup_collection_map("smart_meter_teriff").asInstanceOf[MongoCursor]) {
      if (obj.getAs[String]("teriff_type").get == "standard") sprice = obj.getAs[String]("price").get.toDouble else dprice = obj.getAs[String]("price").get.toDouble
    }
    (sprice,dprice)
   }

}
