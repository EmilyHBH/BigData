import org.apache.spark.sql.types.{StructType, StructField, StringType, ArrayType, IntegerType}
import com.databricks.spark.xml._
import org.apache.spark.sql.functions._

// Schema for the node id's. Has node id & array(tag) for filtering out specific child tags
val nodeIdsWithTrafficCalmingHump = StructType(Array(
	StructField("_id", StringType, nullable = false),
	StructField("tag", ArrayType(StructType(Array(
		StructField("_k",StringType, nullable = true),
		StructField("_v",StringType, nullable = true)
	))), nullable = true)
))

// Loads the node data into the schema nodeId. Explodes the tag array, and filters out the correct rows on the tag key and value.
val nodeList = spark.read.format("xml").option("rootTag", "osm").schema(nodeIdsWithTrafficCalmingHump).option("rowTag", "node").load("hdfs://localhost:9000/map.osm")
val nodeListExploded = nodeList.select($"_id", explode($"tag")).select($"_id", $"col._k", $"col._v")

val nodeListWithTrafficCalmingHump = nodeListExploded.where("_k == 'traffic_calming' AND _v == 'hump'")

// Schema for ways. Has Way id, array(tag) and array(nd). The tags are used to filter out highways, and nd's use the ref to join with nodeId.id
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

// Load way data into schema. Explodes and filters for correct tags.
val wayList = spark.read.format("xml").option("rootTag","osm").schema(ways).option("rowTag", "way").load("hdfs://localhost:9000/map.osm")
val wayListWithTagExploded = wayList.select($"_id", explode($"tag"), $"nd").select($"_id", $"col._k", $"col._v", $"nd")
val highwayList = wayListWithTagExploded.where("_k LIKE 'highway%'")
val highwayRefExploded = highwayList.select($"_id", $"_k", $"_v", explode($"nd")).select($"_id", $"_k", $"_v", $"col._ref")

// Inner join way_nd_ref == node_id.
val ndRefToTrafficCalmingHump = highwayRefExploded.as("d1").join(nodeListWithTrafficCalmingHump.as("d2"), $"_ref" === $"d2._id", "inner")

// Gets the 15 highways that refer to traffic_calming humps the most.
ndRefToTrafficCalmingHump.groupBy($"d1._id").count().sort(desc("count")).limit(15).rdd.saveAsTextFile("hdfs://localhost:9000/scala/opg7")
