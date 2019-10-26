#!/bin/bash
sh delete_hadoop_result.sh $1
sh compile.sh $1
sh run.sh $1
echo "\n\n~~Results~~\n\n"
sh printResult.sh $1
