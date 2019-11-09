val file = sc.textFile("hdfs://localhost:9000/xmlhadoop/map.osm")
val buildings = file.filter(line => line.contains("k=\"building\""))
println("Total amount of buildings:\t" + buildings.count())
