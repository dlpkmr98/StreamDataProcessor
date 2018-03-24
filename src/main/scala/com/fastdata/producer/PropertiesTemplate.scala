/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.fastdata.producer

import java.util.Properties
import scala.io.Source._

object PropertiesTemplate {

  val props = new Properties()
  props.load(fromURL(Thread.currentThread.getContextClassLoader.getResource("producer.properties")).bufferedReader())
  val dirPath = "D:\\MeterData\\"
  val topic = "dilip"
 
}
