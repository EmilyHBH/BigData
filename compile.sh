#!/bin/bash
CLASS_PATH=`hadoop classpath`
javac -classpath ${CLASS_PATH} XMLHadoop$1.java
jar cf prod/xmlhadoop$1.jar XMLHadoop$1*.class
