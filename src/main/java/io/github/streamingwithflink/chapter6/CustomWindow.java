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
package io.github.streamingwithflink.chapter6;

import io.github.streamingwithflink.util.SensorReading;
import io.github.streamingwithflink.util.SensorSource;
import io.github.streamingwithflink.util.SensorTimeAssigner;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Collection;
import java.util.Collections;

public class CustomWindow {

    public static void main(String[] args) throws Exception {

        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // checkpoint every 10 seconds
        env.getCheckpointConfig().setCheckpointInterval(10_000);

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

        DataStream<Tuple4<String, Long, Long, Integer>> countsPerThirtySecs = sensorData
            .keyBy(r -> r.id)
            // a custom window assigner for 30 seconds tumbling windows
            .window(new ThirtySecondsWindows())
            // a custom trigger that fires early (at most) every second
            .trigger(new OneSecondIntervalTrigger())
            // count readings per window
            .process(new CountFunction());

        countsPerThirtySecs.print();

        env.execute("Run custom window example");
    }

    /**
     * A custom window that groups events in to 30 second tumbling windows.
     */
    public static class ThirtySecondsWindows extends WindowAssigner<Object, TimeWindow> {

        long windowSize = 30_000L;

        @Override
        public Collection<TimeWindow> assignWindows(Object e, long ts, WindowAssignerContext ctx) {

            // rounding down by 30 seconds
            long startTime = ts - (ts % windowSize);
            long endTime = startTime + windowSize;
            // emitting the corresponding time window
            return Collections.singletonList(new TimeWindow(startTime, endTime));
        }

        @Override
        public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
            return EventTimeTrigger.create();
        }

        @Override
        public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
            return new TimeWindow.Serializer();
        }

        @Override
        public boolean isEventTime() {
            return true;
        }
    }

    /**
     * A trigger thet fires early. The trigger fires at most every second.
     */
    public static class OneSecondIntervalTrigger extends Trigger<SensorReading, TimeWindow> {

        @Override
        public TriggerResult onElement(SensorReading r, long ts, TimeWindow w, TriggerContext ctx) throws Exception {
            // firstSeen will be false if not set yet
            ValueState<Boolean> firstSeen = ctx.getPartitionedState(
                new ValueStateDescriptor<>("firstSeen", Types.BOOLEAN));

            // register initial timer only for first element
            if (firstSeen.value() == null) {
                // compute time for next early firing by rounding watermark to second
                long t = ctx.getCurrentWatermark() + (1000 - (ctx.getCurrentWatermark() % 1000));
                ctx.registerEventTimeTimer(t);
                // register timer for the end of the window
                ctx.registerEventTimeTimer(w.getEnd());
                firstSeen.update(true);
            }
            // Continue. Do not evaluate window per element
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long ts, TimeWindow w, TriggerContext ctx) throws Exception {
            if (ts == w.getEnd()) {
                // final evaluation and purge window state
                return TriggerResult.FIRE_AND_PURGE;
            } else {
                // register next early firing timer
                long t = ctx.getCurrentWatermark() + (1000 - (ctx.getCurrentWatermark() % 1000));
                if (t < w.getEnd()) {
                    ctx.registerEventTimeTimer(t);
                }
                // fire trigger to early evaluate window
                return TriggerResult.FIRE;
            }
        }

        @Override
        public TriggerResult onProcessingTime(long ts, TimeWindow w, TriggerContext ctx) throws Exception {
            // Continue. We don't use processing time timers
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow w, TriggerContext ctx) throws Exception {
            // clear trigger state
            ValueState<Boolean> firstSeen = ctx.getPartitionedState(
                new ValueStateDescriptor<>("firstSeen", Types.BOOLEAN));
            firstSeen.clear();
        }
    }

    /**
     * A window function that counts the readings per sensor and window.
     * The function emits the sensor id, window end, tiem of function evaluation, and count.
     */
    public static class CountFunction
            extends ProcessWindowFunction<SensorReading, Tuple4<String, Long, Long, Integer>, String, TimeWindow> {

        @Override
        public void process(
                String id,
                Context ctx,
                Iterable<SensorReading> readings,
                Collector<Tuple4<String, Long, Long, Integer>> out) throws Exception {
            // count readings
            int cnt = 0;
            for (SensorReading r : readings) {
                cnt++;
            }
            // get current watermark
            long evalTime = ctx.currentWatermark();
            // emit result
            out.collect(Tuple4.of(id, ctx.window().getEnd(), evalTime, cnt));
        }
    }
}
