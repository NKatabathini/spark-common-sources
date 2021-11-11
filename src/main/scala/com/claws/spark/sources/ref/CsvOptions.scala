package com.claws.spark.sources.ref

case class CsvOptions(format: String = ExternalSrcConsts.csv,
                      header: Boolean,
                      delimeter: String,
                      inferSchma: Boolean)  
