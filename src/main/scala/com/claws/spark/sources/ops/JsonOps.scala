package com.claws.spark.sources.ops

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.util.Try

object JsonOps {

  private lazy val ObjectMapper = initialize()

  private def initialize() : ObjectMapper = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
  }

  def parse[T](json: String, expected: Class[T]) : T = {
    ObjectMapper.readValue(json, expected)
  }

  def toString[T](entity: T) : String = {
  ObjectMapper.writeValueAsString(entity)
  }

  def isJson[T](json: String, expected: Class[T]) : Boolean = {
    Try(parse(json, expected)).isSuccess
  }
}
