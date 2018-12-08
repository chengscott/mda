#!/bin/bash -xe
# mvn tidy:pom
# java-format -i src/*.java src/*/*

mvn clean package

input=$INPUT
centroid=$CENTROID
distance=$DISTANCE
output=$OUTPUT
[ -z "$input" ] && input="/user/chengscott/hw3/data.txt"
[ -z "$centroid" ] && centroid="/user/chengscott/hw3/c1.txt"
[ -z "$distance" ] && distance="euclidean"
[ -z "$output" ] && output="/user/chengscott/kmeans"

yarn jar target/KMeans-1.0.jar kmeans.KMeans "$input" "$centroid" "$distance" "$output"
