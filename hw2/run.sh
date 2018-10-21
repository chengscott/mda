#!/bin/bash -xe
# mvn tidy:pom
# java-format -i src/*.java src/*/*

mvn clean package

input=$1
output=$2
[ -z "$input" ] && input="/user/chengscott/data/athletics"
[ -z "$output" ] && output="/user/chengscott/lsh"

yarn jar target/LSH-1.0.jar lsh.LSH "$input" "$output"
