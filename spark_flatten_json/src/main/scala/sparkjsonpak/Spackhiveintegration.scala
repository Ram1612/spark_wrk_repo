package sparkjsonpak

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Spackhiveintegration {

	def main(args:Array[String]):Unit={

			val conf=new SparkConf().setAppName("Spark_NULL_Handling").setMaster("local[*]")
					val sc=new SparkContext(conf)
					sc.setLogLevel("Error")
					val spark=SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
					import spark.implicits._

					val df_null = spark.read.format("csv").option("InferSchema","true").option("header","true").load("file:///D://input_files//nulldata.txt")
					df_null.show(false)
					df_null.printSchema()

					println("===<TASK --1> Handling NULL - Replacing with provided literal values========")
					val result = df_null.na.fill(0,Seq("id","amount")).
					na.fill("NA",Array("name")).
					na.fill("NA",Array("place"))

					result.show(false)
					result.printSchema()

					println
					println("==============<TASK --2> Added new column with current date=================")
					val final_df = result.withColumn("current_date", date_format(current_timestamp(), "dd-MM-yyyy")).show()

	} 

}