FROM gradle:jdk8-alpine

RUN mkdir -p /home/gradle/src
COPY . /tmp/src
RUN cp -r /tmp/src /home/gradle

WORKDIR /home/gradle/src
RUN gradle build
RUN tar xvf build/distributions/src.tar

ENV KAFKA_ADDRESS "localhost:9092"
ENV DUMP_FILE "/tmp/dump_file"

WORKDIR /home/gradle/src/src/lib
CMD java -cp "*" org.z.logdumper.Main
