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
 * This is class runs a Sliding Window Join Query with the order [[A X B]^w1 x C]^w2
 * parameters:
 * Run with these parameters:
 */

public class SWJ_a_ABC {
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
            outputPath = "./src/main/resources/resultSWJ_ABC.csv";
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

        // join A B
        DataStream<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> streamAB = streamA.join(streamB)
                .where(new UDFs.getKeyT3())
                .equalTo(new UDFs.getKeyT3())
                .window(SlidingEventTimeWindows.of(Time.minutes(w1Size), Time.minutes(w1Slide)))
                .apply(new FlatJoinFunction<Tuple3<Integer, Integer, Long>, Tuple3<Integer, Integer, Long>, Tuple6<Integer, Integer, Long, Integer, Integer, Long>>() {
                    @Override
                    public void join(Tuple3<Integer, Integer, Long> d1, Tuple3<Integer, Integer, Long> d2, Collector<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> collector) throws Exception {
                        collector.collect(new Tuple6<>(d1.f0, d1.f1, d1.f2, d2.f0, d2.f1, d2.f2));
                    }
                }).assignTimestampsAndWatermarks(new UDFs.ExtractTimestampAB(1000, timePropagation));

        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC = streamAB.join(streamC)
                .where(new UDFs.getKeyT6())
                .equalTo(new UDFs.getKeyT3())
                .window(SlidingEventTimeWindows.of(Time.minutes(w2Size), Time.minutes(w2Slide)))
                .apply(new FlatJoinFunction<Tuple6<Integer, Integer, Long, Integer, Integer, Long>, Tuple3<Integer, Integer, Long>, Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>>() {

                    public void join(Tuple6<Integer, Integer, Long, Integer, Integer, Long> d1, Tuple3<Integer, Integer, Long> d2, Collector<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> collector) throws Exception {
                        collector.collect(new Tuple9<>(d1.f0, d1.f1, d1.f2, d1.f3, d1.f4, d1.f5, d2.f0, d2.f1, d2.f2));
                    }
                });

        streamABC//.print();
                .writeAsText(outputPath, FileSystem.WriteMode.OVERWRITE); //.setParallelism(1);

        //System.out.println(env.getExecutionPlan());
        JobExecutionResult executionResult = env.execute("My Flink Job");
        System.out.println("The job took " + executionResult.getNetRuntime(TimeUnit.MILLISECONDS) + "ms to execute");
    }

}
