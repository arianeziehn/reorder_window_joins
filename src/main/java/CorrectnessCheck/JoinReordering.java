package CorrectnessCheck;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import util.ArtificalSourceFunction;
import util.ThroughputLogger;
import util.Tuple3ParallelSourceFunction;
import util.UDFs;

import java.util.concurrent.TimeUnit;

import static java.lang.Math.max;

public class JoinReordering {
    public static void main(String[] args) throws Exception {

        final ParameterTool parameters = ParameterTool.fromArgs(args);
        // Checking input parameters
        String file = parameters.get("inputQnV", "./src/main/resources/QnV_R2000070.csv");
        String filePM = parameters.get("inputPM", "./src/main/resources/luftdaten_11245.csv");
        // the number of keys, should be equals or more as parallelism
        Integer numberOfKeys = parameters.getInt("keys", 5);
        Integer velFilter = parameters.getInt("vel", 90);
        Integer quaFilter = parameters.getInt("qua", 80);
        Integer pm10Filter = parameters.getInt("pmb", 20);
        // we except minutes
        Integer w1Size = parameters.getInt("w1size", 20);
        Integer w1Slide = parameters.getInt("s1size", 20);
        Integer w2Size = parameters.getInt("w1size", 20);
        Integer w2Slide = parameters.getInt("s1size", 20);
        long throughput = parameters.getLong("tput", 10000); //tput per second, for me at least rather heavy already
        String timePropagation = parameters.get("time_propagation", "A");

        String outputPath;
        if (!parameters.has("output")) {
            outputPath = "./src/main/resources/resultSWJ_ACB_TW.csv";
        } else {
            outputPath = parameters.get("output");
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Tuple3<Integer, Integer, Long>> streamA = env.addSource(new Tuple3ParallelSourceFunction(file, numberOfKeys, ",", throughput, "V"))
                .setParallelism(5) // this is 16/3 make it automatic
                .filter(t -> t.f1 > velFilter )
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp(60000));

        DataStream<Tuple3<Integer, Integer, Long>> streamB = env.addSource(new Tuple3ParallelSourceFunction(file, numberOfKeys, ",", throughput, "Q"))
                .setParallelism(5) // this is 16/3 make it automatic
                .filter(t -> t.f1 > quaFilter)
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp(60000));

        DataStream<Tuple3<Integer, Integer, Long>> streamC = env.addSource(new Tuple3ParallelSourceFunction(filePM, numberOfKeys, ";", throughput))
                .setParallelism(5)
                .filter(t -> t.f1 > pm10Filter)
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp(180000));

        streamA.flatMap(new ThroughputLogger<Tuple3<Integer, Integer, Long>>(ArtificalSourceFunction.RECORD_SIZE_IN_BYTE, throughput));
        streamB.flatMap(new ThroughputLogger<Tuple3<Integer, Integer, Long>>(ArtificalSourceFunction.RECORD_SIZE_IN_BYTE, throughput));
        streamC.flatMap(new ThroughputLogger<Tuple3<Integer, Integer, Long>>(ArtificalSourceFunction.RECORD_SIZE_IN_BYTE, throughput));

        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC =
            //    new SWJ_a_ABC_parameter(streamA,streamB,streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
              new SWJ_a_ACB_parameter(streamA,streamB,streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
           // new SWJ_a_BCA_parameter(streamA,streamB,streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        //     new SWJ_a_CBA_parameter(streamA,streamB,streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();

        streamABC//.print();
                .writeAsText(outputPath, FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        JobExecutionResult executionResult = env.execute("My Flink Job");
        System.out.println("The job took " + executionResult.getNetRuntime(TimeUnit.MILLISECONDS) + "ms to execute");
    }
}
