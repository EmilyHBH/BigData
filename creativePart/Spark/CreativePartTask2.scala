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

val nodeIdsWithStreetAddr = StructType(Array(
	StructField("_id", StringType, nullable = false),
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

// Loads data into node schema. filters for child tag k=addr:street. then gets the grid area of node. counts the nodes in the grid area
val nodeList = spark.read.format("xml").option("rootTag", "osm").schema(nodeIdsWithStreetAddr).option("rowTag", "node").load("hdfs://localhost:9000/osloMap.osm")
val nodeListWithAddrStreet = nodeList.filter(array_contains($"tag._k", "addr:street"))
val nodelistXY = nodeListWithAddrStreet.withColumn("xy", concat(latUDF($"_lat"), lonUDF($"_lon")))
val nrOfStreetsInGrid = nodelistXY.groupBy($"xy").agg(count($"_id").as("streets"))

// Loads byturer, then gets grid area and counts the amount of trips started there
val byturer = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("hdfs://localhost:9000/creativePart/turer.csv")
val xyByturer = byturer.withColumn("xy", concat(latUDF($"start_station_latitude"), lonUDF($"start_station_longitude")))
val nrOfTripsInGrid = xyByturer.groupBy($"xy").agg(count($"start_station_id").as("started_trips"))

// Joining the two dataframes, gets the ratio and saves result as textfile
val merged = nrOfStreetsInGrid.as("n").join(nrOfTripsInGrid.as("b"), $"n.xy" === $"b.xy", "inner")
val result = merged.withColumn("ratio", ($"started_trips"/$"streets"))
result.select($"n.xy", $"ratio").rdd.saveAsTextFile("hdfs://localhost:9000/scala/creative2")
