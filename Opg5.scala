/*import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import com.databricks.spark.xml._
import org.apache.spark.sql.functions._*/

val wayList = spark.read.format("xml").option("rootTag","osm").option("rowTag", "way").load("hdfs://localhost:9000/map.osm")

val buildingsList = wayList.filter(array_contains($"tag._k", "building")).
                    withColumn("NumberOfNodes", size($"nd"))

buildingsList.select(avg("NumberOfNodes")).rdd.saveAsTextFile("hdfs://localhost:9000/scala/opg5")
