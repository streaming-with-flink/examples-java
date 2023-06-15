package io.github.streamingwithflink.chapter5;

import io.github.streamingwithflink.util.SensorReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * Program demonstrating a rolling sum.
 */
public class ReduceTransformations {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<String,String>> inputStream = env.fromElements(
        Tuple2.of("en","tea"),Tuple2.of("fr","good"), Tuple2.of("fr","game"), Tuple2.of("en","cake")
        );
        DataStream<Tuple2<String,String>> resultStream = inputStream
                .keyBy(0)
                .reduce((x,y)-> Tuple2.of(x.f0,x.f1+" "+y.f1));
        resultStream.print();
        env.execute();
    }
}
