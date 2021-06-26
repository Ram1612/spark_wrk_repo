package sparkjsonpak

import org.apache.spark._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object hiveexecution_eclipse {

	def main(args:Array[String]):Unit={

			val conf=new SparkConf().setAppName("Spark_hive_integration_eclipse").setMaster("local[*]")
					val sc=new SparkContext(conf)
					sc.setLogLevel("Error")
					val spark=SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
					import spark.implicits._

					val hc =new HiveContext(sc)
					import hc.implicits._
					
					println("==========Select Hive table from Spark session===============")
			    spark.sql("select * from spark_hive.txnrecords_parquet limit 10").show()
					
	}

}