package com.fastdata.streaming

import com.fastdata.persistence._

trait StreamingService {
  
  def startStream(obj:PersistenceService):Unit
  
}