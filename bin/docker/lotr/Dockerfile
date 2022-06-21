#FROM hseeberger/scala-sbt:eclipse-temurin-11.0.13_1.5.5_2.13.7
FROM eclipse-temurin:11.0.12_7-jdk
ENV COMPONENT=querymanager
#COPY core /raphtory/core
#COPY examples /raphtory/examples
#COPY project /raphtory/project
#COPY build.sbt /raphtory
COPY examples/raphtory-example-lotr/target/scala-2.13/example-lotr-assembly-0.5.jar /raphtory/
WORKDIR /raphtory
#RUN sbt "examplesLotr/compile"
COPY bin/docker/lotr/entrypoint.sh /raphtory
RUN chmod 700 entrypoint.sh
CMD ./entrypoint.sh $COMPONENT
