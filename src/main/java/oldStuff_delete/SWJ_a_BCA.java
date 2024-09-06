package oldStuff_delete;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import util.*;

import java.util.concurrent.TimeUnit;

import static java.lang.Math.max;

/**
 * This is class runs a Sliding Window Join Query with the order [[B X C]^wx x A]^wy
 * x=1 und y=2 if (w1.size > w2.size) and x=2 und y=1 else
 * no clue if that holds!
 * parameters:
 * Run with these parameters:
 * --input ./src/main/resources/QnV_R2000070.csv
 */

public class SWJ_a_BCA {
    public static void main(String[] args) throws Exception {

        final ParameterTool parameters = ParameterTool.fromArgs(args);
        // Checking input parameters
        // the number of keys, should be equals or more as parallelism
        Integer numberOfKeys = parameters.getInt("keys", 16);
        // we except minutes
        Integer w1Size = parameters.getInt("w1size", 20);
        Integer w1Slide = parameters.getInt("s1size", 10);
        Integer w2Size = parameters.getInt("w1size", 20);
        Integer w2Slide = parameters.getInt("s1size", 10);
        long throughput = parameters.getLong("tput", 100000);
        double selectivity = parameters.getDouble("sel", 0.1);
        String timePropagation = parameters.get("time_propagation", "A");

        String outputPath;
        if (!parameters.has("output")) {
            outputPath = "./src/main/resources/resultSWJ_BCA.csv";
        } else {
            outputPath = parameters.get("output");
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Tuple3<Integer, Integer, Long>> streamA = env.addSource(new ArtificalSourceFunction(throughput, max(w1Size, w2Size), selectivity, numberOfKeys))
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp(1000));
        streamA.print();

        DataStream<Tuple3<Integer, Integer, Long>> streamB = env.addSource(new ArtificalSourceFunction(throughput, max(w1Size, w2Size), selectivity, numberOfKeys))
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp(1000));

        DataStream<Tuple3<Integer, Integer, Long>> streamC = env.addSource(new ArtificalSourceFunction(throughput, max(w1Size, w2Size), selectivity, numberOfKeys))
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp(1000));

        streamA.flatMap(new ThroughputLogger<Tuple3<Integer, Integer, Long>>(ArtificalSourceFunction.RECORD_SIZE_IN_BYTE, throughput));
        streamB.flatMap(new ThroughputLogger<Tuple3<Integer, Integer, Long>>(ArtificalSourceFunction.RECORD_SIZE_IN_BYTE, throughput));
        streamC.flatMap(new ThroughputLogger<Tuple3<Integer, Integer, Long>>(ArtificalSourceFunction.RECORD_SIZE_IN_BYTE, throughput));

        Integer w_1Size;
        Integer w_1Slide;
        Integer w_2Size;
        Integer w_2Slide;

        if (w1Size > w2Size) {
            w_1Size = w1Size;
            w_1Slide = w1Slide;
            w_2Size = w2Size;
            w_2Slide = w2Slide;
            timePropagation = "C";
        } else {
            w_1Size = w2Size;
            w_1Slide = w2Slide;
            w_2Size = w1Size;
            w_2Slide = w1Slide;
            timePropagation = "B";
        }

        // join A B
        DataStream<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> streamBC = streamB.join(streamC)
                .where(new UDFs.getKeyT3())
                .equalTo(new UDFs.getKeyT3())
                .window(SlidingEventTimeWindows.of(Time.minutes(w_1Size), Time.minutes(w_1Slide)))
                .apply(new FlatJoinFunction<Tuple3<Integer, Integer, Long>, Tuple3<Integer, Integer, Long>, Tuple6<Integer, Integer, Long, Integer, Integer, Long>>() {
                    @Override
                    public void join(Tuple3<Integer, Integer, Long> d1, Tuple3<Integer, Integer, Long> d2, Collector<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> collector) throws Exception {
                        collector.collect(new Tuple6<>(d1.f0, d1.f1, d1.f2, d2.f0, d2.f1, d2.f2));
                    }
                }).assignTimestampsAndWatermarks(new UDFs.ExtractTimestampBC(1000, timePropagation));

        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC = streamBC.join(streamA)
                .where(new UDFs.getKeyT6())
                .equalTo(new UDFs.getKeyT3())
                .window(SlidingEventTimeWindows.of(Time.minutes(w_2Size), Time.minutes(w_2Slide)))
                .apply(new FlatJoinFunction<Tuple6<Integer, Integer, Long, Integer, Integer, Long>, Tuple3<Integer, Integer, Long>, Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>>() {

                    public void join(Tuple6<Integer, Integer, Long, Integer, Integer, Long> d1, Tuple3<Integer, Integer, Long> d2, Collector<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> collector) throws Exception {
                        collector.collect(new Tuple9<>(d2.f0, d2.f1, d2.f2, d1.f0, d1.f1, d1.f2, d1.f3, d1.f4, d1.f5));
                    }
                });

        streamABC//.print();
                .writeAsText(outputPath, FileSystem.WriteMode.OVERWRITE); //.setParallelism(1);

        //System.out.println(env.getExecutionPlan());
        JobExecutionResult executionResult = env.execute("My Flink Job");
        System.out.println("The job took " + executionResult.getNetRuntime(TimeUnit.MILLISECONDS) + "ms to execute");
    }

}
