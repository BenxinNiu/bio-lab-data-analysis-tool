package main.scala

import org.apache.spark.sql.SparkSession
import Consolidator._

object Entry extends App{

  val spark = SparkSession.builder
    .master("local")
    .appName("shopifyDataChallenge")
    .getOrCreate()


  DaliyTemperatureConsolidator.consolidateRecords()

}
