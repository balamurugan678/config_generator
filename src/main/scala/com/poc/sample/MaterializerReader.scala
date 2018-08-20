package com.poc.sample

import com.poc.sample.Models.MaterialConfig

trait MaterializerReader {

  def readMaterializerConfig(): Seq[MaterialConfig]

}
