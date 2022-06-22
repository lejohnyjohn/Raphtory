#FROM hseeberger/scala-sbt:eclipse-temurin-11.0.13_1.5.5_2.13.7
FROM eclipse-temurin:11.0.12_7-jdk
ENV COMPONENT=querymanager
COPY examples/raphtory-example-lotr/target/scala-2.13/example-lotr-assembly-0.1.0.jar /raphtory/
RUN curl -o /tmp/lotr.csv https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv
WORKDIR /raphtory
COPY bin/docker/lotr/entrypoint.sh /raphtory
RUN chmod 700 entrypoint.sh
CMD ./entrypoint.sh $COMPONENT
