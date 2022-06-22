#!/bin/bash
component=$1

if [ $1 = "client" ]
then
  java -cp example-lotr-assembly-0.1.0.jar com.raphtory.examples.lotr.LOTRClient
else
  java -cp example-lotr-assembly-0.1.0.jar com.raphtory.examples.lotr.LOTRService $component
fi
