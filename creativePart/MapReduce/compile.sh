#!/bin/bash
CLASS_PATH=`hadoop classpath`
javac -classpath ${CLASS_PATH} CreativePart$1.java
jar cf prod/creativePart$1.jar CreativePart$1*.class
