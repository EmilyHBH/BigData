val file = sc.textFile("hdfs://localhost:9000/xmlhadoop/map.osm")

val updates = file.
	filter(line => line.contains("version=")).
	map(word => (
		word.slice(word.indexOf("version=\"")+9, word.indexOf("\" ",word.indexOf("version=\""))).toFloat, // key float that is version nr
		word.slice(2, word.indexOf(" ", 3)) + " with id " + word.slice(word.indexOf("id=\"")+4, word.indexOf("\" ",word.indexOf("id=\""))))).
	max

println(updates)
