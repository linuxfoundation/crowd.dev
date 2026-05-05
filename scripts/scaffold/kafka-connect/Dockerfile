FROM --platform=linux/amd64 confluentinc/cp-kafka-connect:8.0.3-1-ubi9

USER root

RUN yum install -y jq findutils unzip

RUN confluent-hub install snowflakeinc/snowflake-kafka-connector:2.5.0 --no-prompt
COPY tmp/kafka-connect-http/ /usr/share/confluent-hub-components/kafka-connect-http/
RUN confluent-hub install confluentinc/kafka-connect-jdbc:10.8.4 --no-prompt

# -----------------------------------------------------------------------------
# Tinybird Append Schema SMT
# Download from:
# https://github.com/linuxfoundation/tinybird-append-schema-smt/releases
# -----------------------------------------------------------------------------
COPY tmp/custom-plugins/tinybird-append-schema-SMT/tinybird-append-schema-SMT-1.0.8.jar /usr/share/java/tinybird-append-schema-SMT/tinybird-append-schema-SMT-1.0.8.jar



VOLUME /storage

USER root

