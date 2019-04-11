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
package io.github.streamingwithflink.chapter1;

import io.github.streamingwithflink.util.SensorReading;
import io.github.streamingwithflink.util.SensorSource;
import io.github.streamingwithflink.util.SensorTimeAssigner;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class AverageSensorReadings {

    /**
     * main() defines and executes the DataStream program.
     *
     * @param args program arguments
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // use event time for the application
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // configure watermark interval
        env.getConfig().setAutoWatermarkInterval(1000L);

        // ingest sensor stream
        DataStream<SensorReading> sensorData = env
            // SensorSource generates random temperature readings
            .addSource(new SensorSource())
            // assign timestamps and watermarks which are required for event time
            .assignTimestampsAndWatermarks(new SensorTimeAssigner());

        DataStream<SensorReading> avgTemp = sensorData
            // convert Fahrenheit to Celsius using and inlined map function
            .map( r -> new SensorReading(r.id, r.timestamp, (r.temperature - 32) * (5.0 / 9.0)))
            // organize stream by sensor
            .keyBy(r -> r.id)
            // group readings in 1 second windows
            .timeWindow(Time.seconds(1))
            // compute average temperature using a user-defined function
            .apply(new TemperatureAverager());

        // print result stream to standard out
        avgTemp.print();

        // execute application
        env.execute("Compute average sensor temperature");
    }

    /**
     *  User-defined WindowFunction to compute the average temperature of SensorReadings
     */
    public static class TemperatureAverager implements WindowFunction<SensorReading, SensorReading, String, TimeWindow> {

        /**
         * apply() is invoked once for each window.
         *
         * @param sensorId the key (sensorId) of the window
         * @param window meta data for the window
         * @param input an iterable over the collected sensor readings that were assigned to the window
         * @param out a collector to emit results from the function
         */
        @Override
        public void apply(String sensorId, TimeWindow window, Iterable<SensorReading> input, Collector<SensorReading> out) {

            // compute the average temperature
            int cnt = 0;
            double sum = 0.0;
            for (SensorReading r : input) {
                cnt++;
                sum += r.temperature;
            }
            double avgTemp = sum / cnt;

            // emit a SensorReading with the average temperature
            out.collect(new SensorReading(sensorId, window.getEnd(), avgTemp));
        }
    }
}
