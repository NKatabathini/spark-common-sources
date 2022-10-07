package com.claws.spark.account

import com.claws.spark.account.model.Statement
import com.claws.spark.account.util.Statement_Util
import com.claws.spark.sources.ops.ExternalSources
import com.claws.spark.sources.ref.CsvOptions
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, date_format, lower, regexp_replace, split, sum, to_date, to_timestamp, trim, unix_timestamp, when, year}
import org.apache.spark.sql.types.DoubleType

object Account_Statement {

  def main(args: Array[String]): Unit = {

    val file_path : String = "C:\\data\\Account_Statement.csv"
    val spark = SparkSession.builder().master("local[2]")
      .appName("account_statement_analysis")
      .getOrCreate()

    import spark.implicits._
//    val csv = CsvOptions("csv",true,"," ,true)
//    val stmt = ExternalSources.getDS_csv[Statement](spark,file_path,csv)

    val stmt = spark.read.option("header",true).csv(file_path)
      .withColumnRenamed("Cheque/Ref No", "ref_num")
      .withColumnRenamed("Withdrawal (?)", "withdrawal")
      .withColumnRenamed("Deposit (?)", "deposit")
      .withColumnRenamed("Closing Balance (?)", "close_bal")

    val acc_stmt = stmt.withColumn("withdrawal", regexp_replace(col("withdrawal"), ",",""))
      .withColumn("deposit", regexp_replace(col("deposit"), ",",""))
      .withColumn("close_bal", regexp_replace(col("close_bal"), ",",""))
      .withColumn("withdrawal", regexp_replace(col("withdrawal"), "-", "0"))
      .withColumn("deposit", regexp_replace(col("deposit"), "-", "0"))
      .withColumn("close_bal", regexp_replace(col("close_bal"), "-", "0"))
//      .as[Statement]


    println("number of transactions as of 20220927 :"+acc_stmt.count())

//    acc_stmt.select(sum("withdrawal").as("sum_withdrawal"),sum("deposit").as("sum_deposit"))
//      .show()
//    acc_stmt.show(20,false)
//
//    acc_stmt.filter(col("Transaction details").contains("OBJECT TECHNOLOGY SOLUTIONS INDIA"))
//      .select(sum(col("deposit"))).show()
//    acc_stmt.filter(col("Transaction details").contains("COGNIZANT"))
//      .select(sum(col("deposit"))).show()
//    acc_stmt.filter(col("Transaction details").contains("IBM"))
//      .select(sum(col("deposit"))).show()

//    acc_stmt.withColumn("OTSI salary", when(col("Transaction details").contains("OBJECT TECHNOLOGY"),sum(col("deposit"))))
//      .withColumn("Cognizant salary", when(col("Transaction details").contains("COGNIZANT"),sum(col("deposit"))))
//      .withColumn("IBM salary", when(col("Transaction details").contains("IBM"),sum(col("deposit"))))
//      .select("OTSI salary","Cognizant salary","IBM salary")
//      .show

//    val prodList = List("POS", "NWD","ATW","IMPS","UPI")
//    acc_stmt.withColumn("product", Statement_Util.productUDF(col("Transaction details")))
//      .withColumn("withdrawal", col("withdrawal").cast(DoubleType))
//      .withColumn("year", year(to_date(Statement_Util.dateUDF(trim(col("Date"))),"yyyy-MM-dd").cast("timestamp")))
//      .select("year","product","withdrawal")
//      .filter(col("product").isin(prodList :_*))
//      .groupBy(col("product")).pivot("year").sum("withdrawal")
//      .show(20)


    val expenseType = List("snack","lend","cred","debt","food","haircut","help","liquid","medicine","petrol","rta","shirt")
    acc_stmt.withColumn("product", Statement_Util.UPIProdUDF(col("Transaction details")))
      .withColumn("withdrawal", col("withdrawal").cast(DoubleType))
      .withColumn("year", year(to_date(Statement_Util.dateUDF(trim(col("Date"))), "yyyy-MM-dd").cast("timestamp")))
      .select("year", "product", "withdrawal")
      .filter(lower(col("product")).isin(expenseType :_*))
      .groupBy(col("year")).pivot("product").sum("withdrawal")
      .show(200)
  }
}
