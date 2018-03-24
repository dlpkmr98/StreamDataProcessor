/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.fastdata.persistence

trait PersistenceService  {
  def setData(x: Any): Unit
  def getData(x:Any) : Any
  def getLookupData:(Any,Any)
}
