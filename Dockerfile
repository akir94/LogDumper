FROM gradle:jdk8-alpine

RUN mkdir -p /home/gradle/src
WORKDIR /home/gradle/src

COPY . /home/gradle/src
RUN gradle build

ENV KAFKA_ADDRESS "localhost:9092"

RUN tar -xvf build/distributions/LogDumper.tar

WORKDIR /home/gradle/src/LogDumper/lib
CMD java -cp "*" org.z.logdumper.Main
