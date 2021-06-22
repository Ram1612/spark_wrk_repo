package sparkjsonpak

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object flattenjson {

	def main(args:Array[String]):Unit={

			val conf=new SparkConf().setAppName("spark_integration").setMaster("local[*]")
					val sc=new SparkContext(conf)
					sc.setLogLevel("Error")
					val spark=SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
					import spark.implicits._

					println ("===========usual json read===============")
					val df_json = spark.read.format("json").option("multiline","true").load("file:///D://json_files//reqapi.json")
					df_json.show(false)
					df_json.printSchema

					println ("=========== json to flatten ===============")

					val df_flattenjson = df_json.select (
							col ("data.avatar"),
							col ("data.email"),
							col ("data.first_name"),
							col ("data.id"),
							col ("data.last_name"),
							col ("page"),
							col ("per_page"),
							col ("support.text"),
							col ("support.url"),
							col ("total") ,
							col ("total_pages"))

					df_flattenjson.show(false)
					df_flattenjson.printSchema

					println ("===========Flatten to json(complex dataset)===============")

					val df_createjson = df_flattenjson.select (
							struct (col ("avatar").alias("avatar"),
									col ("email").alias("email"),
									col ("first_name").alias("first_name"),
									col ("id").alias("id"),
									col ("last_name").alias("last_name")).alias("data"),
							col ("page"),
							col ("per_page"),
							struct (col ("text").alias("text"),
									col ("url").alias("url")).alias("support"),
							col ("total") ,
							col ("total_pages"))

					df_createjson.show(false)
					df_createjson.printSchema


	}
}