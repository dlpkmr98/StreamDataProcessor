/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.fastdata.persistence

import com.mongodb.casbah._
import com.fastdata.initilization.ConfigurationLoader


object MongoFactory {
  
  val server = ConfigurationLoader.persistence_db_props.getProperty(ConfigurationLoader.persistence_db+".servers").toString
  val port = ConfigurationLoader.persistence_db_props.getProperty(ConfigurationLoader.persistence_db+".port").toInt
  val database = ConfigurationLoader.persistence_db_props.getProperty(ConfigurationLoader.persistence_db+".database").toString
  
  val getConnection: MongoClient = MongoClient(server,port)
  def closeConnection { getConnection.close}
  def getCollection(coll: String): MongoCollection = getConnection(database)(coll)
  def setCollection: MongoCollection = getConnection(database)(ConfigurationLoader.persistence_db_props.getProperty(ConfigurationLoader.persistence_db+".persistence_collection").toString)

}
