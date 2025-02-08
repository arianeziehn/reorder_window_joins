import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import util.*;

import java.util.concurrent.TimeUnit;

import static java.lang.Math.max;

/**
 * This class runs the combinations of a two-way Sliding Window Join Queries [[[[A X B]^w1 x C]^w2 x D]^w3 x E]^w4
 * only timestamp B is propagated between all WJs
 * Note this class handles only a subset of orders, currently: ABCDE, BDCAE, ABECD, and BEDAC
 */

public class SWJClusterT4_5Streams {
    public static void main(String[] args) throws Exception {

        final ParameterTool parameters = ParameterTool.fromArgs(args);
        // Checking input parameters
        // the number of keys, should be equals or more as parallelism
        int numberOfKeys = parameters.getInt("keys", 16);
        // we except minutes
        int w1Size = parameters.getInt("w1size", 15);
        int w1Slide = parameters.getInt("w1slide", 5);
        int w2Size = parameters.getInt("w2size", 15);
        int w2Slide = parameters.getInt("w2slide", 15);
        int w3Size = parameters.getInt("w3size", 30);
        int w3Slide = parameters.getInt("w3slide", 30);
        int w4Size = parameters.getInt("w4size", 15);
        int w4Slide = parameters.getInt("w4slide", 1);
        long throughput = parameters.getLong("tput", 100);
        int freqA = parameters.getInt("freqA", 29); // equals 29 tuples per event time minute that is how to play with selective, note that event time is just an artificial time
        int freqB = parameters.getInt("freqB", 2);
        int freqC = parameters.getInt("freqC", 5);
        int freqD = parameters.getInt("freqD", 1);
        int freqE = parameters.getInt("freqE", 63);
        int parallelism = parameters.getInt("para", 16);
        int runtime = parameters.getInt("run", 25); // the time the source runs
        String joinOrder = parameters.get("order", "ABCDE");
        // 1 * 20 = 20 tuples per window per stream
        // 20*20 for window one * 20 for third window
        int maxFreq = max(max(max(max(freqA, freqB), freqC), freqD), freqE);
        long throughputA = (long) (throughput * ((double) (freqA) / (maxFreq)));
        long throughputB = (long) (throughput * ((double) (freqB) / (maxFreq)));
        long throughputC = (long) (throughput * ((double) (freqC) / (maxFreq)));
        long throughputD = (long) (throughput * ((double) (freqD) / (maxFreq)));
        long throughputE = (long) (throughput * ((double) (freqE) / (maxFreq)));

        String timePropagation = parameters.get("tProp", "B"); // the stream name for time propagation

        String outputPath;
        if (!parameters.has("output")) {
            outputPath = "./src/main/resources/resultSWJ";
        } else {
            outputPath = parameters.get("output") + joinOrder;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        System.out.println(joinOrder + "with Windows w1 (" + w1Size + ";" + w1Slide + ") and w2 (" + w2Size + ";" + w2Slide + "with Windows w3 (" + w3Size + ";" + w3Slide + ") and w4 (" + w4Size + ";" + w4Slide + ").");

        DataStream<Tuple16<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>> resultStream;

        if (joinOrder.equals("ABCDE")) {

            DataStream<Tuple4<Integer, Integer, Long, Long>> streamA = env.addSource(new ArtificialSourceFunctionT4(throughputA, runtime, freqA, numberOfKeys))
                    .setParallelism(parallelism)
                    .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp_T4(60000)); // as we consider Minutes

            DataStream<Tuple4<Integer, Integer, Long, Long>> streamB = env.addSource(new ArtificialSourceFunctionT4(throughputB, runtime, freqB, numberOfKeys))
                    .setParallelism(parallelism)
                    .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp_T4(60000));

            streamA.flatMap(new ThroughputLogger<Tuple4<Integer, Integer, Long, Long>>(ArtificialSourceFunctionT4.RECORD_SIZE_IN_BYTE, throughputA));
            streamB.flatMap(new ThroughputLogger<Tuple4<Integer, Integer, Long, Long>>(ArtificialSourceFunctionT4.RECORD_SIZE_IN_BYTE, throughputB));

            DataStream<Tuple7<Integer, Integer, Long, Integer, Integer, Long, Long>> streamAB = new SWJ_AR_T4(streamA, streamB, w1Size, w1Slide).run()
                    .assignTimestampsAndWatermarks(new UDFs.ExtractTimestampAR_T7(60000, timePropagation));

            DataStream<Tuple4<Integer, Integer, Long, Long>> streamC = env.addSource(new ArtificialSourceFunctionT4(throughputC, runtime, freqC, numberOfKeys))
                    .setParallelism(parallelism)
                    .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp_T4(60000));

            streamC.flatMap(new ThroughputLogger<Tuple4<Integer, Integer, Long, Long>>(ArtificialSourceFunctionT4.RECORD_SIZE_IN_BYTE, throughputC));

            DataStream<Tuple10<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>> streamABC = new SWJ_AC_T7(streamAB, streamC, w2Size, w2Slide).run()
                    .assignTimestampsAndWatermarks(new UDFs.ExtractTimestampAR_T10(60000, timePropagation));

            DataStream<Tuple4<Integer, Integer, Long, Long>> streamD = env.addSource(new ArtificialSourceFunctionT4(throughputD, runtime, freqD, numberOfKeys))
                    .setParallelism(parallelism)
                    .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp_T4(60000));

            streamD.flatMap(new ThroughputLogger<Tuple4<Integer, Integer, Long, Long>>(ArtificialSourceFunctionT4.RECORD_SIZE_IN_BYTE, throughputD));

            DataStream<Tuple13<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>> streamABCD = new SWJ_AC_T10(streamABC, streamD, w3Size, w3Slide).run()
                    .assignTimestampsAndWatermarks(new UDFs.ExtractTimestampAR_T13(60000, timePropagation));

            DataStream<Tuple4<Integer, Integer, Long, Long>> streamE = env.addSource(new ArtificialSourceFunctionT4(throughputE, runtime, freqE, numberOfKeys))
                    .setParallelism(parallelism)
                    .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp_T4(60000));

            streamE.flatMap(new ThroughputLogger<Tuple4<Integer, Integer, Long, Long>>(ArtificialSourceFunctionT4.RECORD_SIZE_IN_BYTE, throughputE));

            resultStream = new SWJ_AC_T13(streamABCD, streamE, w4Size, w4Slide).run();


        } else if (joinOrder.equals("ABECD")) {

            DataStream<Tuple4<Integer, Integer, Long, Long>> streamA = env.addSource(new ArtificialSourceFunctionT4(throughputA, runtime, freqA, numberOfKeys))
                    .setParallelism(parallelism)
                    .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp_T4(60000)); // as we consider Minutes

            DataStream<Tuple4<Integer, Integer, Long, Long>> streamB = env.addSource(new ArtificialSourceFunctionT4(throughputB, runtime, freqB, numberOfKeys))
                    .setParallelism(parallelism)
                    .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp_T4(60000));

            streamA.flatMap(new ThroughputLogger<Tuple4<Integer, Integer, Long, Long>>(ArtificialSourceFunctionT4.RECORD_SIZE_IN_BYTE, throughputA));
            streamB.flatMap(new ThroughputLogger<Tuple4<Integer, Integer, Long, Long>>(ArtificialSourceFunctionT4.RECORD_SIZE_IN_BYTE, throughputB));

            DataStream<Tuple7<Integer, Integer, Long, Integer, Integer, Long, Long>> streamAB = new SWJ_AR_T4(streamA, streamB, w1Size, w1Slide).run()
                    .assignTimestampsAndWatermarks(new UDFs.ExtractTimestampAR_T7(60000, timePropagation));

            DataStream<Tuple4<Integer, Integer, Long, Long>> streamE = env.addSource(new ArtificialSourceFunctionT4(throughputE, runtime, freqE, numberOfKeys))
                    .setParallelism(parallelism)
                    .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp_T4(60000));

            streamE.flatMap(new ThroughputLogger<Tuple4<Integer, Integer, Long, Long>>(ArtificialSourceFunctionT4.RECORD_SIZE_IN_BYTE, throughputE));

            DataStream<Tuple10<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>> streamABE = new SWJ_AC_T7(streamAB, streamE, w4Size, w4Slide).run()
                    .assignTimestampsAndWatermarks(new UDFs.ExtractTimestampAR_T10(60000, timePropagation));

            DataStream<Tuple4<Integer, Integer, Long, Long>> streamC = env.addSource(new ArtificialSourceFunctionT4(throughputC, runtime, freqC, numberOfKeys))
                    .setParallelism(parallelism)
                    .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp_T4(60000));

            streamC.flatMap(new ThroughputLogger<Tuple4<Integer, Integer, Long, Long>>(ArtificialSourceFunctionT4.RECORD_SIZE_IN_BYTE, throughputC));

            DataStream<Tuple13<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>> streamABCE = new SWJ_ABD_T10(streamABE, streamC, w2Size, w2Slide).run()
                    .assignTimestampsAndWatermarks(new UDFs.ExtractTimestampAR_T13(60000, timePropagation));

            DataStream<Tuple4<Integer, Integer, Long, Long>> streamD = env.addSource(new ArtificialSourceFunctionT4(throughputD, runtime, freqD, numberOfKeys))
                    .setParallelism(parallelism)
                    .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp_T4(60000));

            streamD.flatMap(new ThroughputLogger<Tuple4<Integer, Integer, Long, Long>>(ArtificialSourceFunctionT4.RECORD_SIZE_IN_BYTE, throughputD));

            resultStream = new SWJ_ABCE_T13(streamABCE, streamD, w3Size, w3Slide).run();

        } else if (joinOrder.equals("BDCAE")) {

            DataStream<Tuple4<Integer, Integer, Long, Long>> streamD = env.addSource(new ArtificialSourceFunctionT4(throughputD, runtime, freqD, numberOfKeys))
                    .setParallelism(parallelism)
                    .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp_T4(60000)); // as we consider Minutes

            DataStream<Tuple4<Integer, Integer, Long, Long>> streamB = env.addSource(new ArtificialSourceFunctionT4(throughputB, runtime, freqB, numberOfKeys))
                    .setParallelism(parallelism)
                    .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp_T4(60000));

            streamD.flatMap(new ThroughputLogger<Tuple4<Integer, Integer, Long, Long>>(ArtificialSourceFunctionT4.RECORD_SIZE_IN_BYTE, throughputD));
            streamB.flatMap(new ThroughputLogger<Tuple4<Integer, Integer, Long, Long>>(ArtificialSourceFunctionT4.RECORD_SIZE_IN_BYTE, throughputB));

            DataStream<Tuple7<Integer, Integer, Long, Integer, Integer, Long, Long>> streamBD = new SWJ_AR_T4(streamB, streamD, w3Size, w3Slide).run()
                    .assignTimestampsAndWatermarks(new UDFs.ExtractTimestampBR_T7(60000, timePropagation));

            DataStream<Tuple4<Integer, Integer, Long, Long>> streamC = env.addSource(new ArtificialSourceFunctionT4(throughputC, runtime, freqC, numberOfKeys))
                    .setParallelism(parallelism)
                    .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp_T4(60000));

            streamC.flatMap(new ThroughputLogger<Tuple4<Integer, Integer, Long, Long>>(ArtificialSourceFunctionT4.RECORD_SIZE_IN_BYTE, throughputC));

            DataStream<Tuple10<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>> streamBCD = new SWJ_AB_T7(streamBD, streamC, w2Size, w2Slide).run()
                    .assignTimestampsAndWatermarks(new UDFs.ExtractTimestampBR_T10(60000, timePropagation));

            DataStream<Tuple4<Integer, Integer, Long, Long>> streamA = env.addSource(new ArtificialSourceFunctionT4(throughputA, runtime, freqA, numberOfKeys))
                    .setParallelism(parallelism)
                    .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp_T4(60000)); // as we consider Minutes

            streamA.flatMap(new ThroughputLogger<Tuple4<Integer, Integer, Long, Long>>(ArtificialSourceFunctionT4.RECORD_SIZE_IN_BYTE, throughputA));

            DataStream<Tuple13<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>> streamABCD = new SWJ_AR_T10(streamBCD, streamA, w1Size, w1Slide).run()
                    .assignTimestampsAndWatermarks(new UDFs.ExtractTimestampAR_T13(60000, timePropagation));

            DataStream<Tuple4<Integer, Integer, Long, Long>> streamE = env.addSource(new ArtificialSourceFunctionT4(throughputE, runtime, freqE, numberOfKeys))
                    .setParallelism(parallelism)
                    .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp_T4(60000));

            streamE.flatMap(new ThroughputLogger<Tuple4<Integer, Integer, Long, Long>>(ArtificialSourceFunctionT4.RECORD_SIZE_IN_BYTE, throughputE));

            resultStream = new SWJ_AC_T13(streamABCD, streamE, w4Size, w4Slide).run();

        } else if (joinOrder.equals("BEDAC")) {

            DataStream<Tuple4<Integer, Integer, Long, Long>> streamE = env.addSource(new ArtificialSourceFunctionT4(throughputE, runtime, freqE, numberOfKeys))
                    .setParallelism(parallelism)
                    .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp_T4(60000));

            DataStream<Tuple4<Integer, Integer, Long, Long>> streamB = env.addSource(new ArtificialSourceFunctionT4(throughputB, runtime, freqB, numberOfKeys))
                    .setParallelism(parallelism)
                    .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp_T4(60000));

            streamE.flatMap(new ThroughputLogger<Tuple4<Integer, Integer, Long, Long>>(ArtificialSourceFunctionT4.RECORD_SIZE_IN_BYTE, throughputE));

            streamB.flatMap(new ThroughputLogger<Tuple4<Integer, Integer, Long, Long>>(ArtificialSourceFunctionT4.RECORD_SIZE_IN_BYTE, throughputB));

            DataStream<Tuple7<Integer, Integer, Long, Integer, Integer, Long, Long>> streamBE = new SWJ_AR_T4(streamB, streamE, w4Size, w4Slide).run()
                    .assignTimestampsAndWatermarks(new UDFs.ExtractTimestampBR_T7(60000, timePropagation));

            DataStream<Tuple4<Integer, Integer, Long, Long>> streamD = env.addSource(new ArtificialSourceFunctionT4(throughputD, runtime, freqD, numberOfKeys))
                    .setParallelism(parallelism)
                    .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp_T4(60000)); // as we consider Minutes

            streamD.flatMap(new ThroughputLogger<Tuple4<Integer, Integer, Long, Long>>(ArtificialSourceFunctionT4.RECORD_SIZE_IN_BYTE, throughputD));

            DataStream<Tuple10<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>> streamBDE = new SWJ_AB_T7(streamBE, streamD, w3Size, w3Slide).run()
                    .assignTimestampsAndWatermarks(new UDFs.ExtractTimestampBR_T10(60000, timePropagation));

            DataStream<Tuple4<Integer, Integer, Long, Long>> streamA = env.addSource(new ArtificialSourceFunctionT4(throughputA, runtime, freqA, numberOfKeys))
                    .setParallelism(parallelism)
                    .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp_T4(60000)); // as we consider Minutes

            streamA.flatMap(new ThroughputLogger<Tuple4<Integer, Integer, Long, Long>>(ArtificialSourceFunctionT4.RECORD_SIZE_IN_BYTE, throughputA));

            DataStream<Tuple13<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>> streamABDE = new SWJ_AR_T10(streamBDE, streamA, w1Size, w1Slide).run()
                    .assignTimestampsAndWatermarks(new UDFs.ExtractTimestampAR_T13(60000, timePropagation));

            DataStream<Tuple4<Integer, Integer, Long, Long>> streamC = env.addSource(new ArtificialSourceFunctionT4(throughputC, runtime, freqC, numberOfKeys))
                    .setParallelism(parallelism)
                    .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp_T4(60000));

            streamC.flatMap(new ThroughputLogger<Tuple4<Integer, Integer, Long, Long>>(ArtificialSourceFunctionT4.RECORD_SIZE_IN_BYTE, throughputC));

            resultStream = new SWJ_ABDE_T13(streamABDE, streamC, w2Size, w2Slide).run();

        } else {
            System.out.println("Something went wrong");
            resultStream = env.fromElements(
                    new Tuple16<>(1, 2, (2 * 60000L), 1, 2, (2 * 60000L), 1, 2, (2 * 60000L), 1, 2, (2 * 60000L), 1, 2, (2 * 60000L), System.currentTimeMillis()));
        }

        resultStream.flatMap(new LatencyLoggerT16())
                //.print();
                .writeAsText(outputPath, FileSystem.WriteMode.OVERWRITE);

        //System.out.println(env.getExecutionPlan());
        JobExecutionResult executionResult = env.execute("My Flink Job");
        System.out.println("The job took " + executionResult.getNetRuntime(TimeUnit.MILLISECONDS) + "ms to execute");
    }

}
