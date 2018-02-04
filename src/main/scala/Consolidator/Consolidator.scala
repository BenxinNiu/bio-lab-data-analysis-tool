package Consolidator
import java.io.PrintWriter
import java.text.SimpleDateFormat

import main.scala.Entry.spark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, udf}

trait Consolidator {

  private val tmpPath:String="file:////home/benxin/tmp/labData/PRODfile.csv"
  def consolidateRecords(): Unit ={

    consolidate()
  }

  def consolidate(): DataFrame={
loadCSV("")
  }

  def loadCSV(path:String=null): DataFrame ={
    if(path!=null)
   spark.read.option("header",true).csv(path)
    else {
      spark.read.option("header", true).csv(tmpPath).select("*").where(col("Temp")=!=""&&col("Light")=!="")
    }
  }
  def writeDfToFile(df:DataFrame,pw:PrintWriter):Unit={
    df.rdd.collect.foreach(b=>{
      pw.write(b.toString.replace("[","").replace("]","") + "\n")
    })
  }

  def writeDfToFile(df:DataFrame,path:String):Unit={
    val pw=new PrintWriter(path)
    pw.write("Date,Day,Time,Temp,Light,Monthly_Light_avg,Monthly_Temp_avg,Daily_Light_avg,Daily_Temp_avg\n")
    df.rdd.collect.foreach(b=>{
      pw.write(b.toString.replace("[","").replace("]","") + "\n")
    })
    pw.close()
  }



}
