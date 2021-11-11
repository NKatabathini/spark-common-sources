package com.claws.spark.sources.ops

import com.claws.spark.sources.ref.{CsvOptions, ExternalSrcConsts, JdbcOptions}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.csv.CSVOptions
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

import scala.reflect.runtime.universe.TypeTag
import scala.util.{Failure, Try}
object ExternalSources {

  def getDS[T <: Product : TypeTag](spark: SparkSession,
                                    source: String,
                                    format: String,
                                    jdbcCred: JdbcOptions,
                                    csvOptions: CsvOptions,
                                    force: Boolean = true): Dataset[T] = {
    import spark.implicits._
    val ds = Try {
      val ds = format match {
        case ExternalSrcConsts.parquet => spark.read.schema(Encoders.product[T].schema).parquet(source)
        case ExternalSrcConsts.avro => spark.read.format("avro").load(source)
        case ExternalSrcConsts.json => spark.read.schema(Encoders.product[T].schema).json(source)
        case ExternalSrcConsts.text => spark.read.text(source)
        case ExternalSrcConsts.glue => spark.table(source)
        case ExternalSrcConsts.delta => spark.read.format("delta").load(source)

        case ExternalSrcConsts.json_multiline => spark.read.schema(Encoders.product[T].schema)
          .option("multiline","true").option("mode","PERMISSIVE").json(source)

        case ExternalSrcConsts.csv => spark.read.format(csvOptions.format)
          .option(ExternalSrcConsts.header, if(csvOptions.header) ExternalSrcConsts.true_string else ExternalSrcConsts.false_string)
          .option(ExternalSrcConsts.delimeter,ExternalSrcConsts.delimeter)
          .option(ExternalSrcConsts.inferSchema, if(csvOptions.inferSchma) ExternalSrcConsts.true_string else ExternalSrcConsts.false_string)
          .load(source)

        case ExternalSrcConsts.jdbc => {
          val fullUrl = jdbcCred.hostPrefix + jdbcCred.host + "/" + jdbcCred.dbclusteridentifier
          spark.read.format(ExternalSrcConsts.jdbc)
            .option(ExternalSrcConsts.driver, jdbcCred.driver)
            .option(ExternalSrcConsts.url, fullUrl)
            .option(ExternalSrcConsts.dbtable, source)
            .option(ExternalSrcConsts.user, jdbcCred.userName)
            .option(ExternalSrcConsts.pasword, jdbcCred.password)
            .load()
        }

      }
      ds.as[T]
    }

    ds match {
      case Failure(exception) => {
        if (!source.isEmpty) println(s"!! getDS $source not found")
        if (!force) spark.createDataset[T](Seq()) else throw exception
      }
      case found => {
        println(s"***getDS found for $source")
        found.get
      }

    }
  }

  def getDS_Parquet[T <: Product : TypeTag](spark: SparkSession, source: String) : Dataset[T] = getDS[T](spark,source,ExternalSrcConsts.parquet,null, null, true )

  def getDS_by_format[T <: Product : TypeTag](spark: SparkSession, source: String, format: String) : Dataset[T] = getDS[T](spark,source,format,null, null, true )

  def getDS_jdbc[T <: Product : TypeTag](spark: SparkSession, source: String, jdbcOptions: JdbcOptions) : Dataset[T] = getDS[T](spark,source,ExternalSrcConsts.jdbc,jdbcOptions, null, true )

  def getDS_csv[T <: Product : TypeTag](spark: SparkSession, source: String, csvOptions: CsvOptions) : Dataset[T] = getDS[T](spark,source,ExternalSrcConsts.csv,null, csvOptions, true )

  def getDF(spark: SparkSession,
                                    source: String,
                                    format: String,
                                    jdbcCred: JdbcOptions,
                                    csvOptions: CsvOptions,
                                    force: Boolean = true): DataFrame = {
    import spark.implicits._
    val df = Try {
      val df = format match {
        case ExternalSrcConsts.parquet => spark.read.parquet(source)
        case ExternalSrcConsts.avro => spark.read.format("avro").load(source)
        case ExternalSrcConsts.json => spark.read.json(source)
        case ExternalSrcConsts.text => spark.read.text(source)
        case ExternalSrcConsts.glue => spark.table(source)
        case ExternalSrcConsts.delta => spark.read.format("delta").load(source)

        case ExternalSrcConsts.csv => spark.read.format(csvOptions.format)
          .option(ExternalSrcConsts.header, if(csvOptions.header) ExternalSrcConsts.true_string else ExternalSrcConsts.false_string)
          .option(ExternalSrcConsts.delimeter,ExternalSrcConsts.delimeter)
          .option(ExternalSrcConsts.inferSchema, if(csvOptions.inferSchma) ExternalSrcConsts.true_string else ExternalSrcConsts.false_string)
          .load(source)

        case ExternalSrcConsts.jdbc => {
          val fullUrl = jdbcCred.hostPrefix + jdbcCred.host + "/" + jdbcCred.dbclusteridentifier
          spark.read.format(ExternalSrcConsts.jdbc)
            .option(ExternalSrcConsts.driver, jdbcCred.driver)
            .option(ExternalSrcConsts.url, fullUrl)
            .option(ExternalSrcConsts.dbtable, source)
            .option(ExternalSrcConsts.user, jdbcCred.userName)
            .option(ExternalSrcConsts.pasword, jdbcCred.password)
            .load()
        }

      }

      df
    }

    df match {
      case Failure(exception) => {
        if (!source.isEmpty) println(s"!! getDF $source not found")
        if (!force) spark.createDataFrame(Seq()) else throw exception
      }
      case found => {
        println(s"***getDF found for $source")
        found.get
      }

    }
  }

  def getDF_Parquet(spark: SparkSession, source: String) : DataFrame = getDF(spark,source,ExternalSrcConsts.parquet,null, null, true )

  def getDF_by_format(spark: SparkSession, source: String, format: String) : DataFrame = getDF(spark,source,format,null, null, true )

  def getDF_jdbc(spark: SparkSession, source: String, jdbcOptions: JdbcOptions) : DataFrame = getDF(spark,source,ExternalSrcConsts.jdbc,jdbcOptions, null, true )

  def getDF_csv(spark: SparkSession, source: String, csvOptions: CsvOptions) : DataFrame = getDF(spark,source,ExternalSrcConsts.csv,null, csvOptions, true )


  def writeDS[T <: Product : TypeTag](spark: SparkSession,
                                      ds: Dataset[T],
                                      path: String,
                                      format: String,
                                      saveMode: SaveMode) : Unit = {
    if(saveMode == SaveMode.Overwrite)
      println(s"Writing Dataset to $path")
    else if(saveMode == SaveMode.Append)
      println(s"Appending Dataset to $path")

    if(format.equalsIgnoreCase(ExternalSrcConsts.delta))
      ds.write.mode(saveMode).format(format).save(path)
    else
      ds.write.mode(saveMode).format(format).save(path)
  }

  def writeDF(spark: SparkSession,
                                      df: DataFrame,
                                      path: String,
                                      format: String,
                                      saveMode: SaveMode) : Unit = {
    if(saveMode == SaveMode.Overwrite)
      println(s"Writing Dataset to $path")
    else if(saveMode == SaveMode.Append)
      println(s"Appending Dataset to $path")

    if(format.equalsIgnoreCase(ExternalSrcConsts.delta))
      df.write.mode(saveMode).format(format).save(path)
    else
      df.write.mode(saveMode).format(format).save(path)
  }


}
