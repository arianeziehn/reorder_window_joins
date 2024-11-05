import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Before;
import org.junit.Test;
import util.Tuple3ParallelSourceFunction;
import util.UDFs;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class JoinReorderingTest_2wayJoin_QnV {

    private StreamExecutionEnvironment env;
    private DataStream<Tuple3<Integer, Integer, Long>> streamA;
    private DataStream<Tuple3<Integer, Integer, Long>> streamB;
    private DataStream<Tuple3<Integer, Integer, Long>> streamC;
    private int w1Size;
    private int w1Slide;
    private int w2Size;
    private int w2Slide;
    private String timePropagation;

    @Before
    public void setup() {
        // Initialize the StreamExecutionEnvironment
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(2);

        String file = "./src/main/resources/QnV_R2000070.csv";
        String filePM = "./src/main/resources/luftdaten_11245.csv";
        // the number of keys, should be equals or more as parallelism
        Integer para = 2;
        Integer numberOfKeys = 5;
        Integer velFilter = 90;
        Integer quaFilter = 80;
        Integer pm10Filter = 20;
        // we except minutes
        long throughput = 1000; // we do it very slowly to be sure system grap it all

        // Create test streams A, B, C with some example data
        streamA = env.addSource(new Tuple3ParallelSourceFunction(file, numberOfKeys, ",", throughput, "V"))
                .setParallelism(para)
                .filter(t -> t.f1 > velFilter )
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp(60000));

        streamB = env.addSource(new Tuple3ParallelSourceFunction(file, numberOfKeys, ",", throughput, "Q"))
                .setParallelism(para)
                .filter(t -> t.f1 > quaFilter)
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp(60000));

        streamC = env.addSource(new Tuple3ParallelSourceFunction(filePM, numberOfKeys, ";", throughput, "PM10"))
                .setParallelism(para)
                .filter(t -> t.f1 > pm10Filter)
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp(180000));
    }

    @Test
    //Case A1: W1=W2, s < l
    public void CaseA1() throws Exception {
        w1Size = 10;
        w1Slide = 5;
        w2Size = 10;
        w2Slide = 5;
        timePropagation = "A";
        String testCase = "A1";
        // Execute each join operation
        //default case
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // default commutative combination
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBAC =
                new SWJ_BAC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // associative pairs
        // this works via time Propagation of a.ts by default
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamACB =
                new SWJ_AC_w2_B_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // consequently via commutativity this permutation also
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamCAB =
                new SWJ_CA_w2_B_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // and then these two cases that never work because b and c could be to far apart from each other
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamCBA =
                new SWJ_CB_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();


        String outputPath = "./src/main/resources/result_SWJ_";
        // Collect the results into lists
        streamABC
                .writeAsText(outputPath + "ABC_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBAC
                .writeAsText(outputPath + "BAC_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamACB
                .writeAsText(outputPath + "ACB_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamCAB
                .writeAsText(outputPath + "CAB_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA
                .writeAsText(outputPath + "BCA_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamCBA
                .writeAsText(outputPath + "CBA_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC = envBatch.readTextFile(outputPath + "ABC_" + testCase + ".csv").distinct().collect();
        List<String> resultBAC = envBatch.readTextFile(outputPath + "BAC_" + testCase + ".csv").distinct().collect();
        List<String> resultACB = envBatch.readTextFile(outputPath + "ACB_" + testCase + ".csv").distinct().collect();
        List<String> resultCAB = envBatch.readTextFile(outputPath + "CAB_" + testCase + ".csv").distinct().collect();
        List<String> resultBCA = envBatch.readTextFile(outputPath + "BCA_" + testCase + ".csv").distinct().collect();
        List<String> resultCBA = envBatch.readTextFile(outputPath + "CBA_" + testCase + ".csv").distinct().collect();

        // Compare the results
        assertEquals(resultABC.size(), resultBAC.size());
        assertEquals(resultABC, resultBAC);
        assertEquals(resultABC.size(), resultCAB.size());
        assertEquals(resultABC, resultCAB);
        assertEquals(resultABC.size(), resultACB.size());
        assertEquals(resultABC, resultACB);
        assertNotEquals(resultABC, resultBCA);
        assertNotEquals(resultABC, resultCBA);
    }

    @Test
    //Case A2 W1=W2, s >= l
    public void CaseA2() throws Exception {
        // Set up the testing environment
        w1Size = 10;
        w1Slide = 15;
        w2Size = 10;
        w2Slide = 15;
        timePropagation = "A";
        String testCase = "A2";
        // Execute each join operation
        //default case
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // default commutative combination
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBAC =
                new SWJ_BAC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // associative pairs
        // this works via time Propagation of a.ts by default
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamACB =
                new SWJ_AC_w2_B_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // consequently via commutativity this permutation also
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamCAB =
                new SWJ_CA_w2_B_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        //and for s > l it works because window length is equivalent
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamCBA =
                new SWJ_CB_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();


        String outputPath = "./src/main/resources/result_SWJ_";
        // Collect the results into lists
        streamABC
                .writeAsText(outputPath + "ABC_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBAC
                .writeAsText(outputPath + "BAC_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamACB
                .writeAsText(outputPath + "ACB_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamCAB
                .writeAsText(outputPath + "CAB_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA
                .writeAsText(outputPath + "BCA_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamCBA
                .writeAsText(outputPath + "CBA_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC = envBatch.readTextFile(outputPath + "ABC_" + testCase + ".csv").distinct().collect();
        List<String> resultBAC = envBatch.readTextFile(outputPath + "BAC_" + testCase + ".csv").distinct().collect();
        List<String> resultACB = envBatch.readTextFile(outputPath + "ACB_" + testCase + ".csv").distinct().collect();
        List<String> resultCAB = envBatch.readTextFile(outputPath + "CAB_" + testCase + ".csv").distinct().collect();
        List<String> resultBCA = envBatch.readTextFile(outputPath + "BCA_" + testCase + ".csv").distinct().collect();
        List<String> resultCBA = envBatch.readTextFile(outputPath + "CBA_" + testCase + ".csv").distinct().collect();

        // Compare the results
        assertEquals(resultABC.size(), resultBAC.size());
        assertEquals(resultABC, resultBAC);
        assertEquals(resultABC.size(), resultCAB.size());
        assertEquals(resultABC, resultCAB);
        assertEquals(resultABC.size(), resultACB.size());
        assertEquals(resultABC, resultACB);
        assertEquals(resultABC.size(), resultBCA.size());
        assertEquals(resultABC, resultBCA);
        assertEquals(resultABC.size(), resultCBA.size());
        assertEquals(resultABC, resultCBA);
    }

    @Test
    //Case A3 W1.l != W2.l, slide < size
    public void CaseA3_w1_geq_w2() throws Exception {
        // Set up the testing environment
        w1Size = 15;
        w1Slide = 5;
        w2Size = 10;
        w2Slide = 5;
        timePropagation = "A";
        String testCase = "A3_w1_geq_w2";
        // Execute each join operation
        //default case
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // default commutative combination
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBAC =
                new SWJ_BAC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // associative permutations
        // this works via time Propagation of a.ts by default
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamACB =
                new SWJ_AC_w2_B_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // consequently via commutativity this permutation also
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamCAB =
                new SWJ_CA_w2_B_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // same problem as in A1 there b and c can be so far apart that they do not occur together in a window
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamCBA =
                new SWJ_CB_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();


        String outputPath = "./src/main/resources/result_SWJ_";
        // Collect the results into lists
        streamABC
                .writeAsText(outputPath + "ABC_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBAC
                .writeAsText(outputPath + "BAC_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamACB
                .writeAsText(outputPath + "ACB_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamCAB
                .writeAsText(outputPath + "CAB_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA
                .writeAsText(outputPath + "BCA_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamCBA
                .writeAsText(outputPath + "CBA_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC = envBatch.readTextFile(outputPath + "ABC_" + testCase + ".csv").distinct().collect();
        List<String> resultBAC = envBatch.readTextFile(outputPath + "BAC_" + testCase + ".csv").distinct().collect();
        List<String> resultACB = envBatch.readTextFile(outputPath + "ACB_" + testCase + ".csv").distinct().collect();
        List<String> resultCAB = envBatch.readTextFile(outputPath + "CAB_" + testCase + ".csv").distinct().collect();
        List<String> resultBCA = envBatch.readTextFile(outputPath + "BCA_" + testCase + ".csv").distinct().collect();
        List<String> resultCBA = envBatch.readTextFile(outputPath + "CBA_" + testCase + ".csv").distinct().collect();

        // Compare the results
        assertEquals(resultABC.size(), resultBAC.size());
        assertEquals(resultABC, resultBAC);
        assertEquals(resultABC.size(), resultCAB.size());
        assertEquals(resultABC, resultCAB);
        assertEquals(resultABC.size(), resultACB.size());
        assertEquals(resultABC, resultACB);
        assertNotEquals(resultABC, resultBCA);
        assertNotEquals(resultABC, resultCBA);
    }

    @Test
    //Case A3 W1.l != W2.l, slide < size
    public void CaseA3_w1_lt_w2() throws Exception {
        // Set up the testing environment
        w1Size = 10;
        w1Slide = 5;
        w2Size = 15;
        w2Slide = 5;
        timePropagation = "A";
        String testCase = "A3_w1_lt_w2";
        // Execute each join operation
        //default case
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // default commutative combination
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBAC =
                new SWJ_BAC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // associative
        // this works via time Propagation of a.ts by default
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamACB =
                new SWJ_AC_w2_B_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // consequently via commutativity this permutation also
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamCAB =
                new SWJ_CA_w2_B_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // same problem as in A1 there b and c can be so far apart that they do not occur together in a window
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamCBA =
                new SWJ_CB_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();

        String outputPath = "./src/main/resources/result_SWJ_";
        // Collect the results into lists
        streamABC
                .writeAsText(outputPath + "ABC_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBAC
                .writeAsText(outputPath + "BAC_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamACB
                .writeAsText(outputPath + "ACB_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamCAB
                .writeAsText(outputPath + "CAB_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA
                .writeAsText(outputPath + "BCA_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamCBA
                .writeAsText(outputPath + "CBA_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC = envBatch.readTextFile(outputPath + "ABC_" + testCase + ".csv").distinct().collect();
        List<String> resultBAC = envBatch.readTextFile(outputPath + "BAC_" + testCase + ".csv").distinct().collect();
        List<String> resultACB = envBatch.readTextFile(outputPath + "ACB_" + testCase + ".csv").distinct().collect();
        List<String> resultCAB = envBatch.readTextFile(outputPath + "CAB_" + testCase + ".csv").distinct().collect();
        List<String> resultBCA = envBatch.readTextFile(outputPath + "BCA_" + testCase + ".csv").distinct().collect();
        List<String> resultCBA = envBatch.readTextFile(outputPath + "CBA_" + testCase + ".csv").distinct().collect();

        // Compare the results
        assertEquals(resultABC.size(), resultBAC.size());
        assertEquals(resultABC, resultBAC);
        assertEquals(resultABC.size(), resultCAB.size());
        assertEquals(resultABC, resultCAB);
        assertEquals(resultABC.size(), resultACB.size());
        assertEquals(resultABC, resultACB);
        assertNotEquals(resultABC, resultBCA);
        assertNotEquals(resultABC, resultCBA);
    }

    @Test
    //Case A4, s >= l, W1.l != W2.l, divider relationship
    public void CaseA4_w1_geq_w2() throws Exception {
        // Set up the testing environment
        w1Size = 20;
        w1Slide = 30;
        w2Size = 10;
        w2Slide = 30;
        timePropagation = "A";
        String testCase = "A4_w1_geq_w2";
        // Execute each join operation
        //default case
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // default commutative combination
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBAC =
                new SWJ_BAC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // associative
        // this works via time Propagation of a.ts by default
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamACB =
                new SWJ_AC_w2_B_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // consequently via commutativity this permutation also
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamCAB =
                new SWJ_CA_w2_B_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        /** if w1.l > w2.l and the first join does not contain the propagated timestamp, we use the largest window in the first join
         *  and  propagate the timestamp of the smallest to the second join, i.e., w2 is the window of the second join and is between A and C so
         *  we propagate C  from the first join BC
         */
        timePropagation = "C";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA =
                new SWJ_bc_BC_w1_A_w2(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamCBA =
                new SWJ_bc_CB_w1_A_w2(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();


        String outputPath = "./src/main/resources/result_SWJ_";
        // Collect the results into lists
        streamABC
                .writeAsText(outputPath + "ABC_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBAC
                .writeAsText(outputPath + "BAC_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamACB
                .writeAsText(outputPath + "ACB_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamCAB
                .writeAsText(outputPath + "CAB_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA
                .writeAsText(outputPath + "BCA_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamCBA
                .writeAsText(outputPath + "CBA_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC = envBatch.readTextFile(outputPath + "ABC_" + testCase + ".csv").distinct().collect();
        List<String> resultBAC = envBatch.readTextFile(outputPath + "BAC_" + testCase + ".csv").distinct().collect();
        List<String> resultACB = envBatch.readTextFile(outputPath + "ACB_" + testCase + ".csv").distinct().collect();
        List<String> resultCAB = envBatch.readTextFile(outputPath + "CAB_" + testCase + ".csv").distinct().collect();
        List<String> resultBCA = envBatch.readTextFile(outputPath + "BCA_" + testCase + ".csv").distinct().collect();
        List<String> resultCBA = envBatch.readTextFile(outputPath + "CBA_" + testCase + ".csv").distinct().collect();

        // Compare the results
        assertEquals(resultABC.size(), resultBAC.size());
        assertEquals(resultABC, resultBAC);
        assertEquals(resultABC.size(), resultCAB.size());
        assertEquals(resultABC, resultCAB);
        assertEquals(resultABC.size(), resultACB.size());
        assertEquals(resultABC, resultACB);
        assertEquals(resultABC.size(), resultBCA.size());
        assertEquals(resultABC, resultBCA);
        assertEquals(resultABC.size(), resultCBA.size());
        assertEquals(resultABC, resultCBA);
    }

    @Test
    //Case A4 W1!=W2, s >= l, W1.l != W2.l && W1.s == W2.s, divider relationship
    public void CaseA4_w1_lt_w2() throws Exception {
        // Set up the testing environment
        w1Size = 5;
        w1Slide = 30;
        w2Size = 30;
        w2Slide = 30;
        timePropagation = "A";
        String testCase = "A4_w1_lt_w2";
        // Execute each join operation
        //default case
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // default commutative combination
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBAC =
                new SWJ_BAC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // associative pairs
        // this works via time Propagation of a.ts by default
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamACB =
                new SWJ_AC_w2_B_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // consequently via commutativity this permutation also
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamCAB =
                new SWJ_CA_w2_B_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        /** if w1.l < w2.l and the first join does not contain the propagated timestamp, we use the largest window in the first join
         *  and  propagate the timestamp of the smallest to the second join, i.e., w1 is the second join and is between A and B so
         *  we propagate B from the first join BC
         */
        timePropagation = "B";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamCBA =
                new SWJ_CB_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();


        String outputPath = "./src/main/resources/result_SWJ_";
        // Collect the results into lists
        streamABC
                .writeAsText(outputPath + "ABC_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBAC
                .writeAsText(outputPath + "BAC_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamACB
                .writeAsText(outputPath + "ACB_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamCAB
                .writeAsText(outputPath + "CAB_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA
                .writeAsText(outputPath + "BCA_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamCBA
                .writeAsText(outputPath + "CBA_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC = envBatch.readTextFile(outputPath + "ABC_" + testCase + ".csv").distinct().collect();
        List<String> resultBAC = envBatch.readTextFile(outputPath + "BAC_" + testCase + ".csv").distinct().collect();
        List<String> resultACB = envBatch.readTextFile(outputPath + "ACB_" + testCase + ".csv").distinct().collect();
        List<String> resultCAB = envBatch.readTextFile(outputPath + "CAB_" + testCase + ".csv").distinct().collect();
        List<String> resultBCA = envBatch.readTextFile(outputPath + "BCA_" + testCase + ".csv").distinct().collect();
        List<String> resultCBA = envBatch.readTextFile(outputPath + "CBA_" + testCase + ".csv").distinct().collect();

        // Compare the results
        assertEquals(resultABC.size(), resultBAC.size());
        assertEquals(resultABC, resultBAC);
        assertEquals(resultABC.size(), resultACB.size());
        assertEquals(resultABC, resultACB);
        assertEquals(resultABC.size(), resultCAB.size());
        assertEquals(resultABC, resultCAB);
        assertEquals(resultABC.size(), resultBCA.size());
        assertEquals(resultABC, resultBCA);
        assertEquals(resultABC.size(), resultCBA.size());
        assertEquals(resultABC, resultCBA);
    }

    @Test
    //Case A6 IVJ lB = uB
    public void CaseA6() throws Exception {
        // Set up the testing environment
        w1Size = -10; // lB W1
        w1Slide = 10; // uB W1
        w2Size = -10; // lB W2
        w2Slide = 10; // uB W2

        timePropagation = "A";
        String testCase = "A6";
        // Execute each join operation with timestamp of A
        //default case
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC =
                new IVJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // default commutative combination
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBAC =
                new IVJ_BAC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        //associative combinations
        // this works via time Propagation of a.ts by default
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamACB =
                new IVJ_ACB(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // consequently via commutativity its permutation also
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamCAB =
                new IVJ_CAB(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // same problem as in A1, b and c can be so far apart that they do not occur together in a window
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA =
                new IVJ_BCA(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamCBA =
                new IVJ_CBA(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();

        String outputPath = "./src/main/resources/result_IVJ_";
        // Collect the results into lists
        streamABC
                .writeAsText(outputPath + "ABC_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBAC
                .writeAsText(outputPath + "BAC_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamACB
                .writeAsText(outputPath + "ACB_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamCAB
                .writeAsText(outputPath + "CAB_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA
                .writeAsText(outputPath + "BCA_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamCBA
                .writeAsText(outputPath + "CBA_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC = envBatch.readTextFile(outputPath + "ABC_" + testCase + ".csv").distinct().collect();
        List<String> resultBAC = envBatch.readTextFile(outputPath + "BAC_" + testCase + ".csv").distinct().collect();
        List<String> resultACB = envBatch.readTextFile(outputPath + "ACB_" + testCase + ".csv").distinct().collect();
        List<String> resultCAB = envBatch.readTextFile(outputPath + "CAB_" + testCase + ".csv").distinct().collect();
        List<String> resultBCA = envBatch.readTextFile(outputPath + "BCA_" + testCase + ".csv").distinct().collect();
        List<String> resultCBA = envBatch.readTextFile(outputPath + "CBA_" + testCase + ".csv").distinct().collect();

        // Compare the results
        assertEquals(resultABC.size(), resultBAC.size());
        assertEquals(resultABC, resultBAC);
        assertEquals(resultABC.size(), resultCAB.size());
        assertEquals(resultABC, resultCAB);
        assertEquals(resultABC.size(), resultACB.size());
        assertEquals(resultABC, resultACB);
        assertNotEquals(resultABC, resultBCA);
        assertNotEquals(resultABC, resultCBA);
    }

    @Test
    //Case A6 IVJ lB = uB, but W1 is not equal W2
    public void CaseA6_w1_neq_w2() throws Exception {
        // Set up the testing environment
        w1Size = -5; // lB W1
        w1Slide = 5; // uB W1
        w2Size = -10; // lB W1
        w2Slide = 10; // uB W1

        timePropagation = "A";
        String testCase = "A6";
        // Execute each join operation with timestamp of A
        //default case
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC =
                new IVJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // default commutative combination
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBAC =
                new IVJ_BAC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        //associative permutations
        // this works via time Propagation of a.ts by default
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamACB =
                new IVJ_ACB(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // consequently via commutativity this permutation also
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamCAB =
                new IVJ_CAB(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // same problem as in A! there b and c can be so far apart that they do not occur together in a window
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA =
                new IVJ_BCA(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamCBA =
                new IVJ_CBA(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();

        String outputPath = "./src/main/resources/result_IVJ_";
        // Collect the results into lists
        streamABC
                .writeAsText(outputPath + "ABC_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBAC
                .writeAsText(outputPath + "BAC_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamACB
                .writeAsText(outputPath + "ACB_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamCAB
                .writeAsText(outputPath + "CAB_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA
                .writeAsText(outputPath + "BCA_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamCBA
                .writeAsText(outputPath + "CBA_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC = envBatch.readTextFile(outputPath + "ABC_" + testCase + ".csv").distinct().collect();
        List<String> resultBAC = envBatch.readTextFile(outputPath + "BAC_" + testCase + ".csv").distinct().collect();
        List<String> resultACB = envBatch.readTextFile(outputPath + "ACB_" + testCase + ".csv").distinct().collect();
        List<String> resultCAB = envBatch.readTextFile(outputPath + "CAB_" + testCase + ".csv").distinct().collect();
        List<String> resultBCA = envBatch.readTextFile(outputPath + "BCA_" + testCase + ".csv").distinct().collect();
        List<String> resultCBA = envBatch.readTextFile(outputPath + "CBA_" + testCase + ".csv").distinct().collect();

        // Compare the results
        assertEquals(resultABC.size(), resultBAC.size());
        assertEquals(resultABC, resultBAC);
        assertEquals(resultABC.size(), resultCAB.size());
        assertEquals(resultABC, resultCAB);
        assertEquals(resultABC.size(), resultACB.size());
        assertEquals(resultABC, resultACB);
        assertNotEquals(resultABC, resultBCA);
        assertNotEquals(resultABC, resultCBA);
    }

    @Test
    //Case A6 IVJ lB = uB
    public void CaseA7_w1_diff_bounds() throws Exception {
        // Set up the testing environment
        w1Size = -10; // lB W1
        w1Slide = 5; // uB W1
        w2Size = -20; // lB W2
        w2Slide = 20; // uB W2

        timePropagation = "A";
        String testCase = "A7";
        // Execute each join operation with timestamp of A
        //default case
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC =
                new IVJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // for BA instead of AB boundary swap of W1
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBAC =
                new IVJ_BAC(streamA, streamB, streamC, -w1Slide, -w1Size, w2Size, w2Slide, timePropagation).run();
        // this works via time Propagation of a.ts by default, right order of streams
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamACB =
                new IVJ_ACB(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // consequently via commutativity this permutation also, W2 is a IVJ with equal-sized bounds so no boundary swap is required
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamCAB =
                new IVJ_CAB(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // same problem as in A1 there b and c can be so far apart that they do not occur together in a window, i.e.,
        // there is no clear window assignment between BC
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA =
                new IVJ_BCA(streamA, streamB, streamC, -w1Slide, -w1Size, -w2Slide, -w2Size, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamCBA =
                new IVJ_CBA(streamA, streamB, streamC, -w1Slide, -w1Size, -w2Slide, -w2Size, timePropagation).run();

        String outputPath = "./src/main/resources/result_IVJ_";
        // Collect the results into lists
        streamABC
                .writeAsText(outputPath + "ABC_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBAC
                .writeAsText(outputPath + "BAC_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamACB
                .writeAsText(outputPath + "ACB_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamCAB
                .writeAsText(outputPath + "CAB_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA
                .writeAsText(outputPath + "BCA_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamCBA
                .writeAsText(outputPath + "CBA_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC = envBatch.readTextFile(outputPath + "ABC_" + testCase + ".csv").distinct().collect();
        List<String> resultBAC = envBatch.readTextFile(outputPath + "BAC_" + testCase + ".csv").distinct().collect();
        List<String> resultACB = envBatch.readTextFile(outputPath + "ACB_" + testCase + ".csv").distinct().collect();
        List<String> resultCAB = envBatch.readTextFile(outputPath + "CAB_" + testCase + ".csv").distinct().collect();
        List<String> resultBCA = envBatch.readTextFile(outputPath + "BCA_" + testCase + ".csv").distinct().collect();
        List<String> resultCBA = envBatch.readTextFile(outputPath + "CBA_" + testCase + ".csv").distinct().collect();

        // Compare the results
        assertEquals(resultABC.size(), resultBAC.size());
        assertEquals(resultABC, resultBAC);
        assertEquals(resultABC.size(), resultCAB.size());
        assertEquals(resultABC, resultCAB);
        assertEquals(resultABC.size(), resultACB.size());
        assertEquals(resultABC, resultACB);
        assertNotEquals(resultABC, resultBCA);
        assertNotEquals(resultABC, resultCBA);
    }

    @Test
    //Case A6 IVJ lB = uB
    public void CaseA7_w2_diff_bounds() throws Exception {
        // Set up the testing environment
        w1Size = -10; // lB W1
        w1Slide = 10; // uB W1
        w2Size = -10; // lB W2
        w2Slide = 20; // uB W2

        timePropagation = "A";
        String testCase = "A7";
        // Execute each join operation with timestamp of A
        //default case
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC =
                new IVJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // commutative pair works, as W1 has equal bounds
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBAC =
                new IVJ_BAC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // associative permutations
        // this works via time Propagation of a.ts by default
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamACB =
                new IVJ_ACB(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // consequently via commutativity this permutation with boundary swap
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamCAB =
                new IVJ_CAB(streamA, streamB, streamC, w1Size, w1Slide, -w2Slide, -w2Size, timePropagation).run();
        // same problem as in A1 there b and c can be so far apart that they do not occur together in a window
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA =
                new IVJ_BCA(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamCBA =
                new IVJ_CBA(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();

        String outputPath = "./src/main/resources/result_IVJ_";
        // Collect the results into lists
        streamABC
                .writeAsText(outputPath + "ABC_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBAC
                .writeAsText(outputPath + "BAC_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamACB
                .writeAsText(outputPath + "ACB_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamCAB
                .writeAsText(outputPath + "CAB_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA
                .writeAsText(outputPath + "BCA_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamCBA
                .writeAsText(outputPath + "CBA_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC = envBatch.readTextFile(outputPath + "ABC_" + testCase + ".csv").distinct().collect();
        List<String> resultBAC = envBatch.readTextFile(outputPath + "BAC_" + testCase + ".csv").distinct().collect();
        List<String> resultACB = envBatch.readTextFile(outputPath + "ACB_" + testCase + ".csv").distinct().collect();
        List<String> resultCAB = envBatch.readTextFile(outputPath + "CAB_" + testCase + ".csv").distinct().collect();
        List<String> resultBCA = envBatch.readTextFile(outputPath + "BCA_" + testCase + ".csv").distinct().collect();
        List<String> resultCBA = envBatch.readTextFile(outputPath + "CBA_" + testCase + ".csv").distinct().collect();

        // Compare the results
        assertEquals(resultABC.size(), resultBAC.size());
        assertEquals(resultABC, resultBAC);
        assertEquals(resultABC.size(), resultACB.size());
        assertEquals(resultABC, resultACB);
        assertEquals(resultABC.size(), resultCAB.size());
        assertEquals(resultABC, resultCAB);
        assertNotEquals(resultABC, resultBCA);
        assertNotEquals(resultABC, resultCBA);
    }

    @Test
    //Case A6 IVJ lB = uB
    public void CaseA7_w1_and_w2_diff_boundaries() throws Exception {
        // Set up the testing environment
        w1Size = -5; // lB W1
        w1Slide = 10; // uB W1
        w2Size = -10; // lB W2
        w2Slide = 20; // uB W2

        timePropagation = "A";
        String testCase = "A7";
        // Execute each join operation with timestamp of A
        //default case
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC =
                new IVJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBAC =
                new IVJ_BAC(streamA, streamB, streamC, -w1Slide, -w1Size, w2Size, w2Slide, timePropagation).run();
        // this works via time Propagation of a.ts by default
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamACB =
                new IVJ_ACB(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // consequently via commutativity this permutation also
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamCAB =
                new IVJ_CAB(streamA, streamB, streamC, w1Size, w1Slide, -w2Slide, -w2Size, timePropagation).run();
        // same problem as in A1 there b and c can be so far apart that they do not occur together in a window
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA =
                new IVJ_BCA(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamCBA =
                new IVJ_CBA(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();

        String outputPath = "./src/main/resources/result_IVJ_";
        // Collect the results into lists
        streamABC
                .writeAsText(outputPath + "ABC_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBAC
                .writeAsText(outputPath + "BAC_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamACB
                .writeAsText(outputPath + "ACB_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamCAB
                .writeAsText(outputPath + "CAB_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA
                .writeAsText(outputPath + "BCA_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamCBA
                .writeAsText(outputPath + "CBA_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC = envBatch.readTextFile(outputPath + "ABC_" + testCase + ".csv").distinct().collect();
        List<String> resultBAC = envBatch.readTextFile(outputPath + "BAC_" + testCase + ".csv").distinct().collect();
        List<String> resultACB = envBatch.readTextFile(outputPath + "ACB_" + testCase + ".csv").distinct().collect();
        List<String> resultCAB = envBatch.readTextFile(outputPath + "CAB_" + testCase + ".csv").distinct().collect();
        List<String> resultBCA = envBatch.readTextFile(outputPath + "BCA_" + testCase + ".csv").distinct().collect();
        List<String> resultCBA = envBatch.readTextFile(outputPath + "CBA_" + testCase + ".csv").distinct().collect();

        // Compare the results
        assertEquals(resultABC.size(), resultBAC.size());
        assertEquals(resultABC, resultBAC);
        assertEquals(resultABC.size(), resultACB.size());
        assertEquals(resultABC, resultACB);
        assertEquals(resultABC.size(), resultCAB.size());
        assertEquals(resultABC, resultCAB);
        assertNotEquals(resultABC, resultBCA);
        assertNotEquals(resultABC, resultCBA);
    }

    @Test
    //Test Case Mixed windows
    public void CaseA3A4_MixedWindow() throws Exception {
        // Set up the testing environment
        w1Size = 60; // SWJ(60,30)
        w1Slide = 30; // u
        w2Size = 5; // TW(5)
        w2Slide = 5; //

        timePropagation = "A";
        String testCase = "MixedWindows";
        // Execute each join operation with timestamp of A
        //default case
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // default commutative combination
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBAC =
                new SWJ_BAC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // this works via time Propagation of a.ts by default
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamACB =
                new SWJ_AC_w2_B_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // consequently via commutativity this permutation also
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamCAB =
                new SWJ_CA_w2_B_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // and then these two cases that never work because b and c could be to far apart from each other
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamCBA =
                new SWJ_CB_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();


        String outputPath = "./src/main/resources/result_SWJ_";
        // Collect the results into lists
        streamABC
                .writeAsText(outputPath + "ABC_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBAC
                .writeAsText(outputPath + "BAC_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamACB
                .writeAsText(outputPath + "ACB_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamCAB
                .writeAsText(outputPath + "CAB_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA
                .writeAsText(outputPath + "BCA_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamCBA
                .writeAsText(outputPath + "CBA_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC = envBatch.readTextFile(outputPath + "ABC_" + testCase + ".csv").distinct().collect();
        List<String> resultBAC = envBatch.readTextFile(outputPath + "BAC_" + testCase + ".csv").distinct().collect();
        List<String> resultACB = envBatch.readTextFile(outputPath + "ACB_" + testCase + ".csv").distinct().collect();
        List<String> resultCAB = envBatch.readTextFile(outputPath + "CAB_" + testCase + ".csv").distinct().collect();
        List<String> resultBCA = envBatch.readTextFile(outputPath + "BCA_" + testCase + ".csv").distinct().collect();
        List<String> resultCBA = envBatch.readTextFile(outputPath + "CBA_" + testCase + ".csv").distinct().collect();

        // Compare the results
        assertEquals(resultABC.size(), resultBAC.size());
        assertEquals(resultABC, resultBAC);
        assertEquals(resultABC.size(), resultCAB.size());
        assertEquals(resultABC, resultCAB);
        assertEquals(resultABC.size(), resultACB.size());
        assertEquals(resultABC, resultACB);
        assertNotEquals(resultABC, resultBCA);
        assertNotEquals(resultABC, resultCBA);
    }
}