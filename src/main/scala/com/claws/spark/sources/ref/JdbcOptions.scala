package com.claws.spark.sources.ref

case class JdbcOptions(hostPrefix: String,
                       driver: String,
                       dbclusteridentifier: String,
                       engine: String,
                       host: String,
                       masterarn: String,
                       password: String,
                       port: String,
                       userName: String) extends Serializable
