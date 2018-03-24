package com.fastdata.persistence

class CassandraService extends PersistenceService with java.io.Serializable{
  
  override def setData(obj: Any): Unit = {
      //write logic here
  }

  override def getData(obj: Any): Any = {
    //write logic here
  }
  
  override def getLookupData:(Double,Double) = {
     //write logic here
    (Double.MinValue,Double.MinValue)
   }
}