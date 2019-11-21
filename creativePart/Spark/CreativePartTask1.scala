import org.apache.spark.sql.types.{StructType, StructField, StringType, ArrayType, IntegerType, FloatType, DoubleType}
import com.databricks.spark.xml._
import org.apache.spark.sql.functions._
import math._

val R =  6372.8  // earth's radius in km
val origoX = 10.662;
val origoY = 59.898;

def haversine(lat1:Double, lon1:Double, lat2:Double, lon2:Double)={
	val dLat=(lat2 - lat1).toRadians
	val dLon=(lon2 - lon1).toRadians

	val a = pow(sin(dLat/2),2) + pow(sin(dLon/2),2) * cos(lat1.toRadians) * cos(lat2.toRadians)
	val c = 2 * asin(sqrt(a))
	floor(R * c)
}

val nodeIdsWithATM = StructType(Array(
	StructField("_lat", DoubleType, nullable = false),
	StructField("_lon", DoubleType, nullable = false),
	StructField("tag", ArrayType(StructType(Array(
		StructField("_k",StringType, nullable = true),
		StructField("_v",StringType, nullable = true)
	))), nullable = true)
))

// udf wrappers
val latUDF = udf((lat:Double) => {
  haversine(lat, origoY, origoX, origoY)
})
val lonUDF = udf((lon:Double) => {
  haversine(origoX, lon, origoX, origoY)
})

// Loads data into node schema. filters for child tag k=amenity v=atm, then gets the grid area of node
val nodeList = spark.read.format("xml").option("rootTag", "osm").schema(nodeIdsWithATM).option("rowTag", "node").load("hdfs://localhost:9000/osloMap.osm")
val nodeListWithATM = nodeList.filter(array_contains($"tag._k", "amenity") && array_contains($"tag._v", "atm"))
val nodelistXY = nodeListWithATM.withColumn("xy", concat(latUDF($"_lat"), lonUDF($"_lon")))

// Loads byturer, then gets grid area
val byturer = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("hdfs://localhost:9000/creativePart/turer.csv")
val xyByturer = byturer.withColumn("xy", concat(latUDF($"end_station_latitude"), lonUDF($"end_station_longitude")))

// Joining the two dataframes
val merged = nodelistXY.as("n").join(xyByturer.as("b"), $"n.xy" === $"b.xy", "inner")

// Get average duration grouped by the xy grid.
merged.groupBy($"n.xy").agg(avg($"duration")).rdd.saveAsTextFile("hdfs://localhost:9000/scala/creative1")
