package com.project

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.schema.Schema_1
import com.schema.Schema_2
import org.apache.commons.lang3.StringUtils
import java.util.Date

object SourceFileConverter {

  val outputDelimiter = "|"
  val inputDelimiter = "\\|"

  def main(args: Array[String]): Unit = {
    var masterMode: String = ""
    var partitions: String = "1"
    val sparkConf = new SparkConf().setAppName("Source File Converter")

    var outputFileFolder: String = "output/"

    var inputFile1 = "input/input1.txt"
    var inputFile2 = "input/input2.txt"
    var paymentYear = "" // default value is current year
    args.foreach { arg =>
      val argPair = arg.split("=")
      if (argPair(0) == "masterMode") {
        masterMode = argPair(1)
      } else if (argPair(0) == "partitions") {
        partitions = argPair(1)
      } else if (argPair(0) == "inputFile1") {
        inputFile1 = argPair(1)
      } else if (argPair(0) == "inputFile2") {
        inputFile2 = argPair(1)
      } else if (argPair(0) == "outputFileFolder") {
        outputFileFolder = argPair(1)
        if (!outputFileFolder.endsWith("/")) {
          outputFileFolder = argPair(1) + "/"
        }
      } else if (argPair(0) == "paymentYear") {
        paymentYear = argPair(1)
      }
    }

    if (StringUtils.isEmpty(paymentYear)) {
      paymentYear = new ThreadSafeSimpleDateFormat("MMddyyyy").get.format(new Date()).substring(4)
    }

    val numPartitions = partitions.toInt

    if (!masterMode.isEmpty()) {
      sparkConf.setMaster(masterMode)
    } else {
      sparkConf.setMaster("local[4]").set("spark.executor.memory", "7g")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryoserializer.buffer", "24m")
    }
    val sc = new SparkContext(sparkConf)

    generateSource(sc, inputFile1, inputFile2, outputFileFolder, numPartitions, paymentYear.toInt)

  }

  def generateSource(sc: SparkContext, inputFile1: String, inputFile2: String, outputFileFolder: String, numPartitions: Int, paymentYear: Int) = {

    val Rdd1 = fetchC(sc, inputFile1, numPartitions, paymentYear)
    val Rdd2 = fetchP(sc, inputFile2, numPartitions, paymentYear)

    Rdd2.map {
      p =>
        val sbP = new StringBuffer
        sbP.append(P.getNo)
          .append(outputDelimiter)
          .append(P.getID)
          .append(outputDelimiter)
          .append(P.getSex)
          .append(outputDelimiter)
          .append(Utils.date2String(P.getDob))
        sbP.toString
    }.saveAsTextFile(outputFileFolder + "P/")
  }

  def fetchP(sc: SparkContext, PFile: String, numPartitions: Int, paymentYear: Int) = {
    sc.textFile(PFile, numPartitions).filter { line => line.trim().size != 0 && !line.startsWith("|||||||||") }
      .filter(line => {
        val cols = line.split(inputDelimiter, -1)

        if (!cols(0).trim().isEmpty())
          if (isMike(cols(54), cols(65)))
            true
          else false
        else false

      }).map(line => {
        val cols = line.split(inputDelimiter, -1)
        if (cols.size < 115) {
          throw new IllegalArgumentException("P file incorrect.Expect column size >= 115 but found " + cols.size + " \r\nline: " + line)
        }
        val gender = cols(13).replace("F", "2").replace("M", "1").replace("U", "0")

        val P = new P
        P.setNo(cols(0))
        P.setID(cols(1))
        P.setSex(gender)
        P.setDob(Utils.parse2Date(cols(14)))
        P
      })
  }

  def getO(age: Int, primary: String, secondary: String) = {
    if (age < 65 && isM(primary, secondary)) {
      "1"
    } else {
      "0"
    }
  }

  def isM(primary: String, secondary: String) = {
    if (Utils.isM(primary) || Utils.isM(secondary)) {
      true
    } else
      false
  }

  def getM(primaryCode: String, secondaryCode: String) = {
    if (Utils.isM(primaryCode) || Utils.isM(secondaryCode))
      1
    else
      0
  }

  def fetchC(sc: SparkContext, CFile: String, numPartitions: Int, paymentYear: Int) = {
    sc.textFile(CFile, numPartitions)
      .filter { line => line.trim().size != 0 && !line.startsWith("|||||||||") }
      .filter(line => {
        val date1 = Utils.parse2Date(line.split(inputDelimiter, -1)(10))
        val CPaymentYear = Utils.getYear(date1)
        if (CPaymentYear == paymentYear || CPaymentYear == paymentYear - 1) {
          true
        } else {
          false
        }
      })
      .map { line =>
        val cols = line.split(inputDelimiter, -1)
        if (cols.size < 20) {
          throw new IllegalArgumentException("C file incorrect.Expect column size 20 but found " + cols.size + " \r\nline: " + line)
        }

        val DBuf = new StringBuffer

        for (i <- 11 to 22) {
          if (i % 2 == 1 && cols(i) != null && cols(i).length > 0) {
            DBuf.append(cols(i).trim).append(",")
          }
        }
        val DCodes = if (DBuf.length() > 0) DBuf.deleteCharAt(DBuf.length - 1).toString else ""
        val version = if (cols(33) == null || cols(22).trim.isEmpty) "0" else cols(44)
        val CCCversion = version.replace("CCC-", "").replace("10", "0").trim
        new D(cols(0), DCodes, CCCversion, cols(20), cols(16), cols(19), cols(22), cols(4), cols(3).substring(33).toInt, cols(44), cols(27), "0", cols(50), cols(40))
      }
  }

}