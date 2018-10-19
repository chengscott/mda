#!/bin/bash -xe
# mvn tidy:pom
# java-format -i *.java */*

mvn clean package

#hdfs dfs -rm -r out

yarn jar target/PageRank-1.0.jar pagerank.PageRank
