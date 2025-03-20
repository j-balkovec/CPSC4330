#!/bin/bash

hadoop fs -mkdir /input
hadoop fs -put $1 /input/$1
hadoop fs -mkdir /output

cd WordCountJava

./compile-map-reduce WordCount
./run-map-reduce WordCount /input/$1 /output/word-count-output

cd ..

hadoop fs -getmerge /output/word-count-output word-count-output.txt
cat word-count-output.txt | ./filter-non-lc-letters.sh | sort -k 2 -n -r | head -n 10

# Clean up...
hadoop fs -rm -r /input
hadoop fs -rm -r /output

rm word-count-output.txt

