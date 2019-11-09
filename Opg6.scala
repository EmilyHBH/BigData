import org.apache.spark.sql.types.{StructType, StructField, StringType, ArrayType, IntegerType}
import com.databricks.spark.xml._
import org.apache.spark.sql.functions._

// Schema for node id's. Has the node id and array(tag) which is used to filter out for nodes with tag k=barrier v=lift_gate
val nodeIdsWithBarrierLiftGate = StructType(Array(
	StructField("_id", StringType, nullable = false),
	StructField("tag", ArrayType(StructType(Array(
		StructField("_k",StringType, nullable = true),
		StructField("_v",StringType, nullable = true)
	))), nullable = true)
))

// Loads data into node schema. Explodes array(Tag) to filter for nodes with tag k=barrier v=lift_gate
val nodeList = spark.read.format("xml").option("rootTag", "osm").schema(nodeIdsWithBarrierLiftGate).option("rowTag", "node").load("hdfs://localhost:9000/map.osm")
val nodeListWithBarrierLiftGate = nodeList.filter(array_contains($"tag._k", "barrier") && array_contains($"tag._v", "lift_gate"))

// Schema for ways
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

val highwayList = wayList.select($"_id", explode($"tag"), $"nd").select($"_id", $"col._k", $"col._v", $"nd").
					where("_k LIKE 'highway' AND _v IN ('service','road','path','unclassified')")

val highwayRefExploded = highwayList.select($"_id", $"_k", $"_v", explode($"nd")).select($"_id", $"_k", $"_v", $"col._ref")

// inner join on highwayRef.ref = nodeID.id
val ndRefToBarrierLiftNodeAmount = highwayRefExploded.join(nodeListWithBarrierLiftGate.as("d2"), $"_ref" === $"d2._id", "inner").count()

println(ndRefToBarrierLiftNodeAmount)
//ndRefToBarrierLiftNodeAmount.rdd.saveAsTextFile("hdfs://localhost:9000/scala/opg6")
