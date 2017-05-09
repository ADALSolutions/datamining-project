#!/bin/bash
./gradlew compileJava
export CP=$(./gradlew showDepsClasspath | grep jar)
java -Dspark.master=local -cp $CP:build/classes/main it.unipd.dei.dm1617.ProjectMain ./Datasets/small-sample.dat.bz2

