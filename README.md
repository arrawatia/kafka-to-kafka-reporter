Kafka To Kafka Metrics Reporter
==============================
Converts Kafka broker metrics to JSON and writes them a Kafka topic.

Installation
------------

1. Run `mvn package` to build the `kafka-to-kafka-1.0.0.jar`.
2. Add `kafka-to-kafka-1.0.0.jar` to the `<kafka_broker_dir/libs/` .
3. Configure the broker (see the configuration section below).
4. Restart the broker.

Configuration
------------
1. Make sure that the metrics topic (default: kafka-metrics) exists on the broker. The broker will create it automatically if auto-creation of topics is not disabled.
2. Edit the `server.properties` file of your installation, activate the reporter by setting:
 
 
        kafka.metrics.reporters=com.quantiply.kafka.KafkaToKafkaMetricsReporter[,kafka.metrics.KafkaCSVMetricsReporter, ...]
        kafka.to.kafka.metrics.reporter.enabled=true
        kafka.to.kafka.metrics.topic=kafka-metrics

Warning
---
This is very experimental. Use at your own risk.

