/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.quantiply.kafka;

import com.yammer.metrics.Metrics;
import kafka.metrics.KafkaMetricsConfig;
import kafka.metrics.KafkaMetricsReporter;
import kafka.metrics.KafkaMetricsReporterMBean;
import kafka.producer.ProducerConfig;
import kafka.utils.VerifiableProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

interface KafkaToKafkaReporterMBean extends KafkaMetricsReporterMBean {}

public class KafkaToKafkaMetricsReporter implements KafkaMetricsReporter, KafkaToKafkaReporterMBean {
    private static final Logger log = LoggerFactory.getLogger(KafkaToKafkaMetricsReporter.class);
    private TopicReporter underlying;
    private VerifiableProperties props;
    private boolean running;
    private boolean initialized;
    private String topic;

    @Override
    public String getMBeanName() {
        return "kafka:type=com.quantiply.kafka.metrics.KafkaToKafkaMetricsReporter";
    }

    synchronized public void init(VerifiableProperties props) {
        if (!initialized) {
            this.props = props;
            props.props().put("metadata.broker.list", String.format("%s:%d", "localhost", props.getInt("port")));
            topic = props.getString("kafka.to.kafka.metrics.topic");

            final KafkaMetricsConfig metricsConfig = new KafkaMetricsConfig(props);

            this.underlying = new TopicReporter(Metrics.defaultRegistry(),
                    new ProducerConfig(props.props()),
                    topic,
                    props.getString("broker.id"));

            if (props.getBoolean("kafka.to.kafka.metrics.reporter.enabled", false)) {
                initialized = true;
                startReporter(metricsConfig.pollingIntervalSecs());
                log.debug("Kafka to Kafka Reporter started.");
            }
        }
    }

    synchronized public void startReporter(long pollingPeriodSecs) {
        if (initialized && !running) {
            underlying.start(pollingPeriodSecs, TimeUnit.SECONDS);
            running = true;
            log.info(String.format("Started Kafka to Kafka metrics reporter with polling period %d seconds", pollingPeriodSecs));
        }
    }

    public synchronized void stopReporter() {
        if (initialized && running) {
            underlying.shutdown();
            running = false;
            log.info("Stopped Kafka to Kafka metrics reporter");
            underlying = new TopicReporter(Metrics.defaultRegistry(), new ProducerConfig(props.props()), topic, props.getString("broker.id"));
        }
    }
}
