package oldStuff_delete;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import util.*;

import java.util.concurrent.TimeUnit;

import static java.lang.Math.max;

/**
 * Run with these parameters:
 */

public class IVJ_a_B_AC {
    public static void main(String[] args) throws Exception {

        final ParameterTool parameters = ParameterTool.fromArgs(args);
        // Checking input parameters
        Integer numberOfKeys = parameters.getInt("keys", 16);
        double selectivity = parameters.getDouble("sel", 0.1);
        String timePropagation = parameters.get("time_propagation", "A");
        Integer upperBound1 = parameters.getInt("uB1", 15);
        Integer lowerBound1 = parameters.getInt("lB1", 15);
        Integer upperBound2 = parameters.getInt("uB2", 15);
        Integer lowerBound2 = parameters.getInt("lB2", 15);
        long throughput = parameters.getLong("tput", 100000);

        String outputPath;
        if (!parameters.has("output")) {
            outputPath = "./src/main/resources/resultIVJ_ACB.csv";
        } else {
            outputPath = parameters.get("output");
        }

        Integer windowSize = max(upperBound1 + lowerBound1, upperBound2 + lowerBound2);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Tuple3<Integer, Integer, Long>> streamA = env.addSource(new ArtificalSourceFunction(throughput, windowSize, selectivity, numberOfKeys))
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp(1000));

        DataStream<Tuple3<Integer, Integer, Long>> streamB = env.addSource(new ArtificalSourceFunction(throughput, windowSize, selectivity, numberOfKeys))
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp(1000));

        DataStream<Tuple3<Integer, Integer, Long>> streamC = env.addSource(new ArtificalSourceFunction(throughput, windowSize, selectivity, numberOfKeys))
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp(1000));

        streamA.flatMap(new ThroughputLogger<Tuple3<Integer, Integer, Long>>(ArtificalSourceFunction.RECORD_SIZE_IN_BYTE, throughput));
        streamB.flatMap(new ThroughputLogger<Tuple3<Integer, Integer, Long>>(ArtificalSourceFunction.RECORD_SIZE_IN_BYTE, throughput));
        streamC.flatMap(new ThroughputLogger<Tuple3<Integer, Integer, Long>>(ArtificalSourceFunction.RECORD_SIZE_IN_BYTE, throughput));

        DataStream<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> streamAC = streamA.keyBy(new UDFs.getKeyT3())
                .intervalJoin(streamC.keyBy(new UDFs.getKeyT3()))
                .between(Time.minutes(lowerBound1), Time.minutes(upperBound1))
                .lowerBoundExclusive()
                .upperBoundExclusive()
                .process(new ProcessJoinFunction<Tuple3<Integer, Integer, Long>, Tuple3<Integer, Integer, Long>, Tuple6<Integer, Integer, Long, Integer, Integer, Long>>() {

                    @Override
                    public void processElement(Tuple3<Integer, Integer, Long> d1, Tuple3<Integer, Integer, Long> d2, ProcessJoinFunction<Tuple3<Integer, Integer, Long>, Tuple3<Integer, Integer, Long>, Tuple6<Integer, Integer, Long, Integer, Integer, Long>>.Context context, Collector<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> collector) throws Exception {
                        collector.collect(new Tuple6<>(d1.f0, d1.f1, d1.f2, d2.f0, d2.f1, d2.f2));
                    }
                })
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestampAB(1000, timePropagation));

        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC = streamB.keyBy(new UDFs.getKeyT3())
                .intervalJoin(streamAC.keyBy(new UDFs.getKeyT6()))
                .between(Time.minutes(lowerBound2), Time.minutes(upperBound2))
                .lowerBoundExclusive()
                .upperBoundExclusive()
                .process(new ProcessJoinFunction<Tuple3<Integer, Integer, Long>, Tuple6<Integer, Integer, Long, Integer, Integer, Long>, Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>>() {

                    @Override
                    public void processElement(Tuple3<Integer, Integer, Long> d1, Tuple6<Integer, Integer, Long, Integer, Integer, Long> d2, ProcessJoinFunction<Tuple3<Integer, Integer, Long>, Tuple6<Integer, Integer, Long, Integer, Integer, Long>, Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>>.Context context, Collector<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> collector) throws Exception {
                        collector.collect(new Tuple9<>(d2.f0, d2.f1, d2.f2, d1.f0, d1.f1, d1.f2, d2.f3, d2.f4, d2.f5));
                    }
                });

        streamABC//.print();
                .writeAsText(outputPath, FileSystem.WriteMode.OVERWRITE); //.setParallelism(1);

        //System.out.println(env.getExecutionPlan());
        JobExecutionResult executionResult = env.execute("My Flink Job");
        System.out.println("The job took " + executionResult.getNetRuntime(TimeUnit.MILLISECONDS) + "ms to execute");
    }

}
