val file = sc.textFile("hdfs://localhost:9000/xmlhadoop/map.osm")

val streets = file.
	filter(line => line.contains("k=\"addr:street\"")).
	map(word => (word.slice(26,word.length()-3), 1)).
	reduceByKey(_+_)

streets.saveAsTextFile("hdfs://localhost:9000/scala/opg2")
