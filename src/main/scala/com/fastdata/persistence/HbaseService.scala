package com.fastdata.persistence

import com.fastdata.streaming.MeterReading


class HbaseService extends PersistenceService with java.io.Serializable{
  
   override def setData(obj: Any): Unit = {
     // convert sensor data to put object and write to HBase  Table CF data    
      val newObj = HbaseFactory.convertToPut(obj.asInstanceOf[MeterReading])._2
      HbaseFactory.newTable.put(newObj)     
      HbaseFactory.newTable.flushCommits();
  }

  override def getData(obj: Any): Any = {
    //write logic here
  }
  
  override def getLookupData:(Double,Double) = {
     //write logic here
    (Double.MinValue,Double.MinValue)
   }

  
}