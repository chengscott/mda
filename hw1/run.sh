#!/bin/bash -xe
# mvn tidy:pom
# java-format -i *.java */*

mvn clean package

input=$1
output=$2
[ -z "$input" ] && input="/user/chengscott/data/p2p-Gnutella04.txt"
[ -z "$output" ] && output="/user/chengscott/p2p-20"

yarn jar target/PageRank-1.0.jar pagerank.PageRank "$input" "$output"
