package sparkjsonpak

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._



object conversionDFtoString {

	def main(args:Array[String]):Unit={

			val conf=new SparkConf().setAppName("Conversion_Dataframe2string").setMaster("local[*]")
					val sc=new SparkContext(conf)
					sc.setLogLevel("Error")
					val spark=SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
					import spark.implicits._

					val schema_struct = StructType(Array(
							StructField("txnno",IntegerType,true),
							StructField("txndate",StringType,true),
							StructField("custno",StringType,true),
							StructField("amount", StringType, true),
							StructField("category", StringType, true),
							StructField("product", StringType, true),
							StructField("city", StringType, true),
							StructField("state", StringType, true),
							StructField("spendby", StringType, true)
							))

					val txndata = spark.read.schema(schema_struct).csv("file:///D://data//txns")


					println

					txndata.createOrReplaceTempView("txndata")
					println("===========Tempview created=================")


					val max_val = spark.sql("select max(txnno) from txndata")

					val maxvalue=max_val.collect().map(x=>x.mkString("")).mkString("").toInt
					println("Maximum value of txnno is   "  +maxvalue )


	}
}