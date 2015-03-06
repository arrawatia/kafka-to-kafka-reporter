Kafka To Kafka Metrics Reporter
==============================


Install On Broker
------------

1. Build the `kafka-to-kafka-1.0.0.jar` jar using `mvn package`.
2. Add `kafka-to-kafka-1.0.0.jar` to the `libs/` 
   directory of your kafka broker installation
3. Configure the broker (see the configuration section below)
4. Restart the broker

Configuration
------------

Edit the `server.properties` file of your installation, activate the reporter by setting:

    kafka.metrics.reporters=com.quantiply.KafkaToKafkaMetricsReporter[,kafka.metrics.KafkaCSVMetricsReporter[,....]]
    kafka.to.kafka.metrics.reporter.enabled=true
    kafka.to.kafka.metrics.topic=kafka-metrics


Simply build the jar and publish it to your maven internal repository (this 
package is not published to any public repositories unfortunately).
