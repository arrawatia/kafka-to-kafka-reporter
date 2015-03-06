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

import com.yammer.metrics.core.*;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.reporting.AbstractPollingReporter;
import com.yammer.metrics.stats.Snapshot;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class TopicReporter extends AbstractPollingReporter implements MetricProcessor<Long> {
    private final MetricPredicate predicate = MetricPredicate.ALL;
    private final ProducerConfig producerConfig;
    private final String topic;
    private  String brokerId ;
    private Producer producer;

    public TopicReporter(MetricsRegistry metricsRegistry, ProducerConfig producerConfig, String topic, String brokerId) {
        super(metricsRegistry, "kafka-to-kafka-reporter");
        this.producerConfig = producerConfig;
        this.topic = topic;
        this.brokerId = brokerId;

    }

    public void run() {
        final Set<Map.Entry<MetricName, Metric>> metrics=getMetricsRegistry().allMetrics().entrySet();
        try {
            for (Map.Entry<MetricName, Metric> entry : metrics) {
                final MetricName metricName = entry.getKey();
                final Metric metric = entry.getValue();
                if (predicate.matches(metricName, metric)) {
                    metric.processWith(this, entry.getKey(), 0L);
                }
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    public void processMeter(MetricName name, Metered meter, Long context) {
        Map<String,String> data = new HashMap<String,String>();
        data.put("name", name.getName());
        data.put("group", name.getGroup());
        data.put("type", name.getType());
        data.put("count","" + meter.count());
        data.put("oneMinuteRate","" + meter.oneMinuteRate());
        data.put("fiveMinuteRate","" + meter.fiveMinuteRate());
        data.put("fifteenMinuteRate","" + meter.fifteenMinuteRate());
        data.put("meanRate","" + meter.meanRate());
        send(data);
    }

    public void processCounter(MetricName name, Counter counter, Long context) {
        Map<String,String> data = new HashMap<String,String>();
        data.put("name", name.getName());
        data.put("group", name.getGroup());
        data.put("type", name.getType());
        data.put("count","" + counter.count());
        send(data);
    }

    public void processHistogram(MetricName name, Histogram histogram, Long context) {
        Map<String,String> data = new HashMap<String,String>();
        data.put("name", name.getName());
        data.put("group", name.getGroup());
        data.put("type", name.getType());
        data.put("min","" + histogram.min());
        data.put("max","" + histogram.max());
        data.put("mean","" + histogram.mean());
        data.put("stdDev","" + histogram.stdDev());
        final Snapshot snapshot = histogram.getSnapshot();
        data.put("median","" + snapshot.getMedian());
        data.put("75thPercentile","" + snapshot.get75thPercentile());
        data.put("95thPercentile","" + snapshot.get95thPercentile());
        data.put("98thPercentile","" + snapshot.get98thPercentile());
        data.put("99thPercentile","" + snapshot.get99thPercentile());
        data.put("999thPercentile","" + snapshot.get999thPercentile());
        send(data);
    }

    public void processTimer(MetricName name, Timer timer, Long context) {
          Map<String,String> data = new HashMap<String,String>();
        data.put("name", name.getName());
        data.put("group", name.getGroup());
        data.put("type", name.getType());
        data.put("min", "" + timer.min());
        data.put("max","" + timer.max());
        data.put("mean","" + timer.mean());
        data.put("stdDev","" + timer.stdDev());
        final Snapshot snapshot = timer.getSnapshot();
        data.put("median","" + snapshot.getMedian());
        data.put("75thPercentile","" + snapshot.get75thPercentile());
        data.put("95thPercentile","" + snapshot.get95thPercentile());
        data.put("98thPercentile","" + snapshot.get98thPercentile());
        data.put("99thPercentile","" + snapshot.get99thPercentile());
        data.put("999thPercentile", "" + snapshot.get999thPercentile());
        send(data);
    }

    public void processGauge(MetricName name, Gauge<?> gauge, Long context){
        Map<String,String> data = new HashMap<String,String>();
        data.put("name", name.getName());
        data.put("group", name.getGroup());
        data.put("type", name.getType());
        data.put("count", "" + gauge.value());
        send(data);
    }

    @Override
    public void start(long period, TimeUnit unit) {
        super.start(period, unit);
    }

    @Override
    public void shutdown() {
            super.shutdown();
    }

    private String mapToJSONString(Map<String, String> map){
        StringBuilder sb = new StringBuilder();
        Iterator<Map.Entry<String, String>> iter = map.entrySet().iterator();
        sb.append("{");
        while (iter.hasNext()) {
            Map.Entry<String, String> entry = iter.next();
            sb.append(entry.getKey());
            sb.append('=').append('"');
            sb.append(entry.getValue());
            sb.append('"');
            if (iter.hasNext()) {
                sb.append(',').append(' ');
            }
        }
        sb.append("}");
        return sb.toString();
    }
    private void send(Map<String,String> message) {
        if(this.producer == null) {
            this.producer = new Producer(producerConfig);
        }
        message.put("brokerId", brokerId);
        message.put("timeStamp", new Date().toString());
        try {
            producer.send(new KeyedMessage(topic, mapToJSONString(message).getBytes("UTF-8")));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
}

