/*
 * Copyright 2015 Fabian Hueske / Vasia Kalavri
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.streamingwithflink.chapter5;

import io.github.streamingwithflink.util.SensorReading;
import io.github.streamingwithflink.util.SensorSource;
import io.github.streamingwithflink.util.SensorTimeAssigner;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Example program to demonstrate simple transformation functions: filter, map, and flatMap.
 */
public class BasicTransformations {

    public static void main(String[] args) throws Exception {

        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // use event time for the application
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // configure watermark interval
        env.getConfig().setAutoWatermarkInterval(1000L);

        // ingest sensor stream
        DataStream<SensorReading> readings = env
            // SensorSource generates random temperature readings
            .addSource(new SensorSource())
            // assign timestamps and watermarks which are required for event time
            .assignTimestampsAndWatermarks(new SensorTimeAssigner());

        // filter out sensor measurements with temperature below 25 degrees
        DataStream<SensorReading> filteredReadings = readings
            .filter(r -> r.temperature >= 25);

        // the above filter transformation using a FilterFunction instead of a lambda function
        // DataStream<SensorReading> filteredReadings = readings
        //     .filter(new TemperatureFilter(25));

        // project the reading to the id of the sensor
        DataStream<String> sensorIds = filteredReadings
            .map(r -> r.id);

        // the above map transformation using a MapFunction instead of a lambda function
        // DataStream<String> sensorIds = filteredReadings
        //     .map(new IdExtractor());

        // split the String id of each sensor to the prefix "sensor" and sensor number
        DataStream<String> splitIds = sensorIds
            .flatMap((FlatMapFunction<String, String>)
                    (id, out) -> { for (String s: id.split("_")) { out.collect(s);}})
            // provide result type because Java cannot infer return type of lambda function
            .returns(Types.STRING);

        // the above flatMap transformation using a FlatMapFunction instead of a lambda function
        // DataStream<String> splitIds = sensorIds
        //         .flatMap(new IdSplitter());

        // print result stream to standard out
        splitIds.print();

        // execute application
        env.execute("Basic Transformations Example");
    }

    /**
     * User-defined FilterFunction to filter out SensorReading with temperature below the threshold.
     */
    public static class TemperatureFilter implements FilterFunction<SensorReading> {

        private final double threshold;

        public TemperatureFilter(double threshold) {
            this.threshold = threshold;
        }

        @Override
        public boolean filter(SensorReading r) {
            return r.temperature >= threshold;
        }
    }

    /**
     * User-defined MapFunction to extract a reading's sensor id.
     */
    public static class IdExtractor implements MapFunction<SensorReading, String> {

        @Override
        public String map(SensorReading r) throws Exception {
            return r.id;
        }
    }

    /**
     * User-defined FlatMapFunction that splits a sensor's id String into a prefix and a number.
     */
    public static class IdSplitter implements FlatMapFunction<String, String> {

        @Override
        public void flatMap(String id, Collector<String> out) {

            String[] splits = id.split("_");

            for (String split : splits) {
                out.collect(split);
            }
        }
    }

}
