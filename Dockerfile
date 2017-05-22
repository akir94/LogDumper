FROM gradle:jdk8-alpine

RUN mkdir -p /home/gradle/src
USER root
COPY . /home/gradle/src
RUN chown -R gradle:gradle /home/gradle/src
USER gradle
WORKDIR /home/gradle/src

RUN gradle build
RUN tar xvf build/distributions/LogDumper.tar

ENV KAFKA_ADDRESS "localhost:9092"
ENV DUMP_FILE "/tmp/dump_file"

WORKDIR /home/gradle/src/LogDumper/lib
CMD java -cp "*" org.z.logdumper.Main
