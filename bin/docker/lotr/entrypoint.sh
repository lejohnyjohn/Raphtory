#!/bin/bash
component=$1
#command="examplesLotr/runMain com.raphtory.examples.lotrTopic.Distributed $component"
#sbt "$command"

if [ $1 = "client" ]
then
  java -cp example-lotr-assembly-0.5.jar com.raphtory.examples.lotrTopic.LOTRClient
else
  java -cp example-lotr-assembly-0.5.jar com.raphtory.examples.lotrTopic.LOTRService $component
fi
