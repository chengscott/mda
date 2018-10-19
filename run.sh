#!/bin/bash -xe
mvn clean package

#hadoop fs -rm -f -r /user/chengscott/out
hdfs dfs -rm -r out

yarn jar target/PageRank-1.0.jar pagerank.PageRank
