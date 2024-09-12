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
import static java.lang.Math.round;

/**
 * This class runs the combinations of a two way Sliding Window Join Queries [[A X B]^w1 x C]^w2
 */

public class SWJCluster {
    public static void main(String[] args) throws Exception {

        final ParameterTool parameters = ParameterTool.fromArgs(args);
        // Checking input parameters
        // the number of keys, should be equals or more as parallelism
        Integer numberOfKeys = parameters.getInt("keys", 16);
        // we except minutes
        Integer w1Size = parameters.getInt("w1size", 20);
        Integer w1Slide = parameters.getInt("w1slide", 10);
        Integer w2Size = parameters.getInt("w2size", 20);
        Integer w2Slide = parameters.getInt("w2slide", 10);
        long throughput = parameters.getLong("tput", 100); //for me at least rather heavy already
        int freqA = parameters.getInt("freqA", 30); // 1 tuples per minute that is how to play with selective
        int freqB = parameters.getInt("freqB", 15);
        int freqC = parameters.getInt("freqC", 1);
        int parallelism = parameters.getInt("para", 16);
        int runtime = parameters.getInt("run", 1);
        String joinOrder = parameters.get("order", "ABC");
        // 1 * 20 = 20 tuples per window per stream
        // 20*20 for window one * 20 for third window
        int maxFreq = max(max(freqA,freqB),freqC);
        long throughputA = (long) (throughput*((double) (freqA)/(maxFreq)));
        long throughputB = (long) (throughput * ((double) (freqB)/(maxFreq)));
        long throughputC = (long) (throughput*((double) (freqC)/(maxFreq)));
        String timePropagation = parameters.get("tProp", "A");

        String outputPath;
        if (!parameters.has("output")) {
            outputPath = "./src/main/resources/resultSWJ";
        } else {
            outputPath = parameters.get("output") + "SWJ";
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Tuple3<Integer, Integer, Long>> streamA = env.addSource(new ArtificalSourceFunction(throughputA, runtime, freqA, numberOfKeys))
                .setParallelism(parallelism)
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp(60000)); // as we consider Minutes

        DataStream<Tuple3<Integer, Integer, Long>> streamB = env.addSource(new ArtificalSourceFunction(throughputB, runtime, freqB, numberOfKeys))
                .setParallelism(parallelism)
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp(60000));

        DataStream<Tuple3<Integer, Integer, Long>> streamC = env.addSource(new ArtificalSourceFunction(throughputC, runtime, freqC, numberOfKeys))
                .setParallelism(parallelism)
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp(60000));

        streamA.flatMap(new ThroughputLogger<Tuple3<Integer, Integer, Long>>(ArtificalSourceFunction.RECORD_SIZE_IN_BYTE, throughputA));
        streamB.flatMap(new ThroughputLogger<Tuple3<Integer, Integer, Long>>(ArtificalSourceFunction.RECORD_SIZE_IN_BYTE, throughputB));
        streamC.flatMap(new ThroughputLogger<Tuple3<Integer, Integer, Long>>(ArtificalSourceFunction.RECORD_SIZE_IN_BYTE, throughputC));

        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> resultStream;

        // join A B
        if (joinOrder.equals("ABC")) { // that is the original query
            System.out.println(joinOrder + "with Windows w1 (" + w1Size + ";" +w1Slide + ") and w2 (" + w2Size + ";" +w2Slide + ").");
            resultStream = new SWJ_ab_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        }
        else if (joinOrder.equals("BAC")) { // this is commutative it works without any 'magic' akka default
            System.out.println(joinOrder + "with Windows w1 (" + w1Size + ";" +w1Slide + ") and w2 (" + w2Size + ";" +w2Slide + ").");
            resultStream = new SWJ_ab_BAC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        }
        else if (joinOrder.equals("ACB")) { // this works via time Propagation of a.ts by default
            System.out.println(joinOrder + "with Windows w1 (" + w1Size + ";" +w1Slide + ") and w2 (" + w2Size + ";" +w2Slide + ").");
            resultStream = new SWJ_ac_AC_w2_B_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        }
        else if (joinOrder.equals("CAB")) { // consequently via commutativity this one also
            System.out.println(joinOrder + "with Windows w1 (" + w1Size + ";" +w1Slide + ") and w2 (" + w2Size + ";" +w2Slide + ").");
            resultStream = new SWJ_ac_CA_w2_B_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        }
        else if (joinOrder.equals("BCA") && w1Slide >= w1Size && w2Slide >= w2Size) {

            if (w1Size > w2Size) {
                System.out.println(joinOrder + "with Windows smaller w1 (" + w1Size + ";" +w1Slide + ") and larger w2 (" + w2Size + ";" +w2Slide + ").");
                timePropagation = "C";
                resultStream = new SWJ_bc_BC_w1_A_w2(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
            } else {
                System.out.println(joinOrder + "with smaller Windows w1 (" + w1Size + ";" +w1Slide + ") and larger w2 (" + w2Size + ";" +w2Slide + ").");
                resultStream = new SWJ_bc_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
            }

        }
        else if (joinOrder.equals("CBA") && w1Slide >= w1Size && w2Slide >= w2Size) {
            if (w1Size > w2Size) {
                System.out.println(joinOrder + "with Windows smaller w1 (" + w1Size + ";" +w1Slide + ") and larger w2 (" + w2Size + ";" +w2Slide + ").");
                timePropagation = "C";
                resultStream = new SWJ_bc_CB_w1_A_w2(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
            } else {
                System.out.println(joinOrder + "with smaller Windows w1 (" + w1Size + ";" +w1Slide + ") and larger w2 (" + w2Size + ";" +w2Slide + ").");
                resultStream = new SWJ_bc_CB_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
            }

        } else {
            System.out.println("Something went wrong and we do ABC");
            resultStream = new SWJ_ab_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        }

        resultStream//.print();
                .writeAsText(outputPath, FileSystem.WriteMode.OVERWRITE);

        //System.out.println(env.getExecutionPlan());
        JobExecutionResult executionResult = env.execute("My Flink Job");
        System.out.println("The job took " + executionResult.getNetRuntime(TimeUnit.MILLISECONDS) + "ms to execute");
    }

}
