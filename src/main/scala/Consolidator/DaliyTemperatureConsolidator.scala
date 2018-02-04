package Consolidator

import java.io.PrintWriter

import main.scala.Entry.spark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
object DaliyTemperatureConsolidator extends Consolidator {

override def consolidate():DataFrame={
  val df=loadCSV()
  val processed=processAllCSV(df)


getMonthlyAverage(processed)
getDailyAverage(processed)

//  val monthly=spark.read.option("header",true).csv(monthlyResult)
//              .select(col("Date"),col("Day"),col("Time"),col("Temp"),col("Light")
//                ,col("Light_avgerage").as("Monthly_Light_avg"),col("Temperature_Average").as("Monthly_Temp_avg"))
//
//  val daily=spark.read.option("header",true).csv(dailyResult)
//    .select(col("Date"),col("Day"),col("Time"),col("Temp"),col("Light")
//      ,col("Light_avgerage").as("Daily_Light_avg"),col("Temperature_Average").as("Daily_Temp_avg"))
//
// val finaldf= monthly.join(daily,monthly("Date")===daily("Date"),"left")
//    .select(monthly("Date"),monthly("Day"),monthly("Time"),monthly("Temp"),monthly("Light"),monthly("Monthly_Light_avg"),
//      monthly("Monthly_Temp_avg"),daily("Daily_Light_avg"),daily("Daily_Temp_avg"))
//  writeDfToFile(finaldf,finalResult)
//finaldf


}

  val finalResult="/home/benxin/tmp/labData/Finalresult.csv"
  val monthlyResult=  "/home/benxin/tmp/labData/Monthlyresult.csv"
  val dailyResult= "/home/benxin/tmp/labData/Dailyresult.csv"

  def processAllCSV(df:DataFrame):DataFrame={
   df.select(trimDate(col("Date")).as("asOfDate"),
           trimMonth(col("Date")).as("Month"),
           getDate(col("Day")).as("actualDate"),
           getMonth(col("Day")).as("actualMonth"),
           col("Date"),col("Day"),col("Time"),
           trim(col("Temp")).as("Temp"),
           trim(col("Light")).as("Light"))
  }



  def getMonthlyAverage(df:DataFrame):DataFrame={
    val pw=new PrintWriter(monthlyResult)
    pw.write("Date,Day,Time,Temp,Light,Monthly_Temp_avg,Monthly_Light_avg\n")

    val withMonthDf=df.select("*").where(col("actualMonth")=!="N/A")
     val WOMonthDf=df.select("*").where(col("actualMonth")==="N/A")

    var result:DataFrame=null
    withMonthDf.select(col("actualMonth")).dropDuplicates.rdd.collect.foreach(a=>{
      val tmpDate=a.toString.replaceAll("\\[","").replaceAll("\\]","")
      println(tmpDate)
      val thisMonth=df.select("*").where(col("actualMonth")===tmpDate)
      result=calculateMean(thisMonth)
      result= result.select(col("Date"),col("Day"),
        col("Time"),col("Temp"),col("Light"),
        col("Temperature_Average").as("Monthly_Temp_avg"),
        col("Light_avgerage").as("Monthly_Light_avg"))
      writeDfToFile(result,pw)
      result.show
    })

   WOMonthDf.select(col("Month")).dropDuplicates.rdd.collect.foreach(a=>{
     val tmpDate=a.toString.replaceAll("\\[","").replaceAll("\\]","")
     println(tmpDate)
     val thisMonth=df.select("*").where(col("Month")===tmpDate)
     result=calculateMean(thisMonth)
     result= result.select(col("Date"),col("Day"),
       col("Time"),col("Temp"),col("Light"),
       col("Temperature_Average").as("Monthly_Temp_avg"),
       col("Light_avgerage").as("Monthly_Light_avg"))
     writeDfToFile(result,pw)
     result.show
   })
 pw.close()
    result
  }


  def getDailyAverage(df:DataFrame):DataFrame={
  val pw=new PrintWriter(dailyResult)
    pw.write("Date,Day,Time,Temp,Light,Daily_Temp_avg,Daily_Light_avg\n")

    val withMonthDf=df.select("*").where(col("actualMonth")=!="N/A")
    val WOMonthDf=df.select("*").where(col("actualMonth")==="N/A")

    var result:DataFrame=null
    withMonthDf.select(col("actualDate")).dropDuplicates.rdd.collect.foreach(a=>{
      val tmpDate=a.toString.replaceAll("\\[","").replaceAll("\\]","")
      println(tmpDate)
      val thisDay=df.select("*").where(col("actualDate")===tmpDate)
      result=calculateMean(thisDay)
      result= result.select(col("Date"),col("Day"),
        col("Time"),col("Temp"),col("Light"),
        col("Temperature_Average").as("Daily_Temp_avg"),
        col("Light_avgerage").as("Daily_Light_avg"))
      writeDfToFile(result,pw)
      result.show
    })

    WOMonthDf.select(col("asOfDate")).dropDuplicates.rdd.collect.foreach(a=>{
      val tmpDate=a.toString.replaceAll("\\[","").replaceAll("\\]","")
      println(tmpDate)
      val thisDay=df.select("*").where(col("asOfDate")===tmpDate)
      result=calculateMean(thisDay)
      result= result.select(col("Date"),col("Day"),
        col("Time"),col("Temp"),col("Light"),
        col("Temperature_Average").as("Daily_Temp_avg"),
        col("Light_avgerage").as("Daily_Light_avg"))
      writeDfToFile(result,pw)
      result.show
    })
    pw.close()

null
  }





  def calculateMean(day:DataFrame):DataFrame={
  val light=day.select("*").where(col("Light")=!="0"||col("Light")=!=0)
  val meanLight=light.select(avg("Light").as("Light_avgerage")).first.toString.replace("[","").replace("]","")
  val meanTemp= day.select(avg("Temp").as("Temperature_Average")).first.toString.replace("[","").replace("]","")
  day.withColumn("Light_avgerage",lit(meanLight)).withColumn("Temperature_Average",lit(meanTemp))

  }

  val getDate=udf((date:String)=>{
    if(date=="" || date==null || date=="  " || date==" ")
      "N/A"
    else {
      val tmp = date.replaceAll(" ", "/").split("/")
      tmp(0) + "/" + tmp(1) + "/" + tmp(2)
    }
  })

  val getMonth=udf((date:String)=>{
    if(date =="" || date==null || date=="  " || date==" ")
      "N/A"
    else {
      val tmp = date.replaceAll(" ", "/").split("/")
      tmp(0) + "/" + tmp(2)
    }
    })


  val formatTime=udf((date:String)=>{
    date.replaceAll(" ","/")
  }
  )

  val trimDate= udf((date:String)=>{
    val tmp=date.replaceAll(" ","/").split("/")
    tmp(0)+"/"+tmp(1)+"/"+tmp(2)
  })

  val trimMonth=udf((date:String)=>{
    val tmp=date.replaceAll(" ","/").split("/")
    tmp(1)+"/"+tmp(2)
  })

  val trim=udf((s:String)=>{
    s.replaceAll(",","").replaceAll("\"","")
  })


}
