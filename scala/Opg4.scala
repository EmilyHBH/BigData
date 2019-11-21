/*import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, ArrayType}
import com.databricks.spark.xml._
import org.apache.spark.sql.functions._*/

val ways = StructType(Array(
	StructField("_id", StringType, nullable = false),
	StructField("tag", ArrayType(StructType(Array(
		StructField("_k",StringType, nullable = true),
		StructField("_v",StringType, nullable = true)
	))), nullable = true),
	StructField("nd", ArrayType(StructType(Array(
		StructField("_ref",StringType, nullable = true)
	))), nullable = true)
))

val wayList = spark.read.format("xml").option("rootTag","osm").schema(ways).option("rowTag", "way").load("hdfs://localhost:9000/map.osm")

val highwayList = wayList.filter(array_contains($"tag._k", "highway")).
					withColumn("NrOfNdNodes", size($"nd")).
					drop("nd","tag")

(highwayList.sort(desc("NrOfNdNodes")).limit(20)).rdd.saveAsTextFile("hdfs://localhost:9000/scala/opg4")
