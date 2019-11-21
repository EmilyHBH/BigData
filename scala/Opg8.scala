import org.apache.spark.sql.types.{StructType, StructField, StringType, ArrayType, IntegerType, FloatType}
import com.databricks.spark.xml._
import org.apache.spark.sql.functions._

val building = StructType(Array(
	StructField("_id", StringType, nullable = false),
	StructField("tag", ArrayType(StructType(Array(
		StructField("_k", StringType, nullable = false)
	))), nullable = true),
	StructField("nd", ArrayType(StructType(Array(
		StructField("_ref", StringType, nullable = false)
	))), nullable = true)
))

val wayList = spark.read.format("xml").option("rootTag","osm").schema(building).option("rowTag", "way").load("hdfs://localhost:9000/map.osm")

val buildingsList = wayList.filter(array_contains($"tag._k", "building"))

val buildingListWithNdExploded = buildingsList.select($"_id", explode($"nd")).select($"_id", $"col._ref")

val nodeLat = StructType(Array(
	StructField("_id", StringType, nullable = false),
	StructField("_lat", FloatType, nullable = false)
))

val nodeList = spark.read.format("xml").option("rootTag","osm").schema(nodeLat).option("rowTag", "node").load("hdfs://localhost:9000/map.osm")

buildingListWithNdExploded.as("b").
	join(nodeList.as("n"), $"b._ref" === $"n._id", "inner").
	drop($"n._id").
	groupBy($"b._id").agg((max($"_lat") - min($"_lat")).as("diff")).
	sort(desc("diff")).
	limit(1).rdd.saveAsTextFile("hdfs://localhost:9000/scala/opg8")
