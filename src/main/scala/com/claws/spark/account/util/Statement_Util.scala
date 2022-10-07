package com.claws.spark.account.util

import org.apache.spark.sql.functions.udf

import java.sql.Date
import java.text.SimpleDateFormat

object Statement_Util {

  val productUDF = udf(getProduct _)

  val dateUDF = udf(getDateFromString _)
  val UPIProdUDF = udf(getUPIProducts _)


  def getProduct(value : String) : String = {
//    println(value)
    val data = value.split(" |-")(0).replace(",","")
//    println(data)
    data
  }

// other way
//  def productUDF = udf((value: String) => {
//    value.split("' '|-")(0).replace(",", "")
//  })

  def getDateFromString(dt: String): String = {

    val df = new SimpleDateFormat("dd/MM/yyyy")
    val date = df.parse(dt)
    val op =new SimpleDateFormat("yyyy-MM-dd").format(date)
//    println(op)
    op
  }

  def getUPIProducts(trans : String) : String = {

    trans.split("#| |-").last

  }


}
