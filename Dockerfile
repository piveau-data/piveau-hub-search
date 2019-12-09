FROM openjdk:jre-alpine

ENV VERTICLE_FILE hub-search-fat.jar

# Set the location of the verticles
ENV VERTICLE_HOME /usr/verticles

EXPOSE 8080 8081

RUN addgroup -S vertx && adduser -S -g vertx vertx

# Copy your fat jar to the container
COPY target/$VERTICLE_FILE $VERTICLE_HOME/
COPY conf/elasticsearch $VERTICLE_HOME/conf/elasticsearch
COPY conf/config.json.sample $VERTICLE_HOME/conf/config.json
COPY test $VERTICLE_HOME/test

RUN chown -R vertx $VERTICLE_HOME
RUN chmod -R g+w $VERTICLE_HOME

USER vertx

# Launch the verticle
WORKDIR $VERTICLE_HOME
ENTRYPOINT ["sh", "-c"]
CMD ["exec java -Xmx2048m -jar $VERTICLE_FILE"]
