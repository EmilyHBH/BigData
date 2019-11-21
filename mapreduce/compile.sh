#!/bin/bash
CLASS_PATH=`hadoop classpath`
javac -classpath ${CLASS_PATH} Task$1.java
jar cf prod/task$1.jar Task$1*.class
