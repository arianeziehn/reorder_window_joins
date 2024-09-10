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
        Integer para = 2; // this is 16/3 TODO make it automatic
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

        streamC = env.addSource(new Tuple3ParallelSourceFunction(filePM, numberOfKeys, ";", throughput))
                .setParallelism(para)
                .filter(t -> t.f1 > pm10Filter)
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp(180000));
    }


    @Test
    //Case A1: W1=W2, s < l
    public void testCaseA1() throws Exception {
        w1Size = 10;
        w1Slide = 5;
        w2Size = 10;
        w2Slide = 5;
        timePropagation = "A";
        String testCase = "A1";
        // Execute each join operation
        //default case
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC =
                new SWJ_ab_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // this is commutative it works without any 'magic' akka default 
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBAC =
                new SWJ_ab_BAC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // this works via time Propagation of a.ts by default 
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamACB =
                new SWJ_ac_AC_w2_B_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // consequently via commutativity this one also
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamCAB =
                new SWJ_ac_CA_w2_B_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // and then these two cases that never work because b and c could be to far apart from each other
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA =
                new SWJ_bc_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamCBA =
                new SWJ_bc_CB_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();


        String outputPath = "./src/main/resources/resultSWJ_";
        // Collect the results into lists
        streamABC
                .writeAsText(outputPath + "ABC_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBAC
                .writeAsText(outputPath + "BAC_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamACB
                .writeAsText(outputPath + "ACB_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamCAB
                .writeAsText(outputPath + "CAB_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA
                .writeAsText(outputPath + "BCA_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamCBA
                .writeAsText(outputPath + "CBA_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC = envBatch.readTextFile(outputPath + "ABC_"+testCase+".csv").distinct().collect();
        List<String> resultBAC = envBatch.readTextFile(outputPath + "BAC_"+testCase+".csv").distinct().collect();
        List<String> resultACB = envBatch.readTextFile(outputPath + "ACB_"+testCase+".csv").distinct().collect();
        List<String> resultCAB = envBatch.readTextFile(outputPath + "CAB_"+testCase+".csv").distinct().collect();
        List<String> resultBCA = envBatch.readTextFile(outputPath + "BCA_"+testCase+".csv").distinct().collect();
        List<String> resultCBA = envBatch.readTextFile(outputPath + "CBA_"+testCase+".csv").distinct().collect();

        // Compare the results
        assertEquals(resultABC.size(), resultBAC.size());
        assertEquals(resultABC,resultBAC);
        assertEquals(resultABC.size(), resultCAB.size());
        assertEquals(resultABC,resultCAB);
        assertEquals(resultABC.size(), resultACB.size());
        assertEquals(resultABC,resultACB);
        assertNotEquals(resultABC,resultBCA);
        assertNotEquals(resultABC,resultCBA);
    }

     @Test
    //Case A2 W1=W2, s >= l
    public void testCaseA2() throws Exception {
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
                 new SWJ_ab_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
         // this is commutative it works without any 'magic' akka default
         DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBAC =
                 new SWJ_ab_BAC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
         // this works via time Propagation of a.ts by default
         DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamACB =
                 new SWJ_ac_AC_w2_B_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
         // consequently via commutativity this one also
         DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamCAB =
                 new SWJ_ac_CA_w2_B_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        //and for s > l it works simply like this
         DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA =
                 new SWJ_bc_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
         DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamCBA =
                 new SWJ_bc_CB_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();


         String outputPath = "./src/main/resources/resultSWJ_";
         // Collect the results into lists
         streamABC
                 .writeAsText(outputPath + "ABC_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
         streamBAC
                 .writeAsText(outputPath + "BAC_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
         streamACB
                 .writeAsText(outputPath + "ACB_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
         streamCAB
                 .writeAsText(outputPath + "CAB_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
         streamBCA
                 .writeAsText(outputPath + "BCA_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
         streamCBA
                 .writeAsText(outputPath + "CBA_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

         env.execute();

         final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
         envBatch.setParallelism(1);

         List<String> resultABC = envBatch.readTextFile(outputPath + "ABC_"+testCase+".csv").distinct().collect();
         List<String> resultBAC = envBatch.readTextFile(outputPath + "BAC_"+testCase+".csv").distinct().collect();
         List<String> resultACB = envBatch.readTextFile(outputPath + "ACB_"+testCase+".csv").distinct().collect();
         List<String> resultCAB = envBatch.readTextFile(outputPath + "CAB_"+testCase+".csv").distinct().collect();
         List<String> resultBCA = envBatch.readTextFile(outputPath + "BCA_"+testCase+".csv").distinct().collect();
         List<String> resultCBA = envBatch.readTextFile(outputPath + "CBA_"+testCase+".csv").distinct().collect();

         // Compare the results
         assertEquals(resultABC.size(), resultBAC.size());
         assertEquals(resultABC,resultBAC);
         assertEquals(resultABC.size(), resultCAB.size());
         assertEquals(resultABC,resultCAB);
         assertEquals(resultABC.size(), resultACB.size());
         assertEquals(resultABC,resultACB);
         assertEquals(resultABC.size(), resultBCA.size());
         assertEquals(resultABC,resultBCA);
         assertEquals(resultABC.size(), resultCBA.size());
         assertEquals(resultABC,resultCBA);
    }

    @Test
    //Case A3 W1.l != W2.l, slide < size
    public void testCaseA3_w1_geq_w2() throws Exception {
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
                new SWJ_ab_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // this is commutative it works without any 'magic' akka default
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBAC =
                new SWJ_ab_BAC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // this works via time Propagation of a.ts by default
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamACB =
                new SWJ_ac_AC_w2_B_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // consequently via commutativity this one also
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamCAB =
                new SWJ_ac_CA_w2_B_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // same problem as in A! there b and c can be so far appart that they do not occur together in a window
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA =
                new SWJ_bc_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamCBA =
                new SWJ_bc_CB_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();


        String outputPath = "./src/main/resources/resultSWJ_";
        // Collect the results into lists
        streamABC
                .writeAsText(outputPath + "ABC_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBAC
                .writeAsText(outputPath + "BAC_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamACB
                .writeAsText(outputPath + "ACB_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamCAB
                .writeAsText(outputPath + "CAB_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA
                .writeAsText(outputPath + "BCA_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamCBA
                .writeAsText(outputPath + "CBA_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC = envBatch.readTextFile(outputPath + "ABC_"+testCase+".csv").distinct().collect();
        List<String> resultBAC = envBatch.readTextFile(outputPath + "BAC_"+testCase+".csv").distinct().collect();
        List<String> resultACB = envBatch.readTextFile(outputPath + "ACB_"+testCase+".csv").distinct().collect();
        List<String> resultCAB = envBatch.readTextFile(outputPath + "CAB_"+testCase+".csv").distinct().collect();
        List<String> resultBCA = envBatch.readTextFile(outputPath + "BCA_"+testCase+".csv").distinct().collect();
        List<String> resultCBA = envBatch.readTextFile(outputPath + "CBA_"+testCase+".csv").distinct().collect();

        // Compare the results
        assertEquals(resultABC.size(), resultBAC.size());
        assertEquals(resultABC,resultBAC);
        assertEquals(resultABC.size(), resultCAB.size());
        assertEquals(resultABC,resultCAB);
        assertEquals(resultABC.size(), resultACB.size());
        assertEquals(resultABC,resultACB);
        assertNotEquals(resultABC,resultBCA);
        assertNotEquals(resultABC,resultCBA);
    }

    @Test
    //Case A3 W1.l != W2.l, slide < size
    public void testCaseA3_w1_lt_w2() throws Exception {
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
                new SWJ_ab_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // this is commutative it works without any 'magic' akka default
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBAC =
                new SWJ_ab_BAC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // this works via time Propagation of a.ts by default
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamACB =
                new SWJ_ac_AC_w2_B_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // consequently via commutativity this one also
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamCAB =
                new SWJ_ac_CA_w2_B_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // same problem as in A! there b and c can be so far apart that they do not occur together in a window
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA =
                new SWJ_bc_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamCBA =
                new SWJ_bc_CB_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();


        String outputPath = "./src/main/resources/resultSWJ_";
        // Collect the results into lists
        streamABC
                .writeAsText(outputPath + "ABC_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBAC
                .writeAsText(outputPath + "BAC_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamACB
                .writeAsText(outputPath + "ACB_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamCAB
                .writeAsText(outputPath + "CAB_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA
                .writeAsText(outputPath + "BCA_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamCBA
                .writeAsText(outputPath + "CBA_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC = envBatch.readTextFile(outputPath + "ABC_"+testCase+".csv").distinct().collect();
        List<String> resultBAC = envBatch.readTextFile(outputPath + "BAC_"+testCase+".csv").distinct().collect();
        List<String> resultACB = envBatch.readTextFile(outputPath + "ACB_"+testCase+".csv").distinct().collect();
        List<String> resultCAB = envBatch.readTextFile(outputPath + "CAB_"+testCase+".csv").distinct().collect();
        List<String> resultBCA = envBatch.readTextFile(outputPath + "BCA_"+testCase+".csv").distinct().collect();
        List<String> resultCBA = envBatch.readTextFile(outputPath + "CBA_"+testCase+".csv").distinct().collect();

        // Compare the results
        assertEquals(resultABC.size(), resultBAC.size());
        assertEquals(resultABC,resultBAC);
        assertEquals(resultABC.size(), resultCAB.size());
        assertEquals(resultABC,resultCAB);
        assertEquals(resultABC.size(), resultACB.size());
        assertEquals(resultABC,resultACB);
        assertNotEquals(resultABC,resultBCA);
        assertNotEquals(resultABC,resultCBA);
    }

   @Test
    //Case A4, s >= l, W1.l != W2.l
    public void testCaseA4_w1_geq_w2() throws Exception {
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
               new SWJ_ab_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
       // this is commutative it works without any 'magic' akka default
       DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBAC =
               new SWJ_ab_BAC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
       // this works via time Propagation of a.ts by default
       DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamACB =
               new SWJ_ac_AC_w2_B_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
       // consequently via commutativity this one also
       DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamCAB =
               new SWJ_ac_CA_w2_B_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
       // same problem as in A! there b and c can be so far apart that they do not occur together in a window
       timePropagation = "C";
       DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA =
               new SWJ_bc_BC_w1_A_w2(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
       DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamCBA =
               new SWJ_bc_CB_w1_A_w2(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();


       String outputPath = "./src/main/resources/resultSWJ_";
       // Collect the results into lists
       streamABC
               .writeAsText(outputPath + "ABC_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
       streamBAC
               .writeAsText(outputPath + "BAC_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
       streamACB
               .writeAsText(outputPath + "ACB_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
       streamCAB
               .writeAsText(outputPath + "CAB_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
       streamBCA
               .writeAsText(outputPath + "BCA_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
       streamCBA
               .writeAsText(outputPath + "CBA_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

       env.execute();

       final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
       envBatch.setParallelism(1);

       List<String> resultABC = envBatch.readTextFile(outputPath + "ABC_"+testCase+".csv").distinct().collect();
       List<String> resultBAC = envBatch.readTextFile(outputPath + "BAC_"+testCase+".csv").distinct().collect();
       List<String> resultACB = envBatch.readTextFile(outputPath + "ACB_"+testCase+".csv").distinct().collect();
       List<String> resultCAB = envBatch.readTextFile(outputPath + "CAB_"+testCase+".csv").distinct().collect();
       List<String> resultBCA = envBatch.readTextFile(outputPath + "BCA_"+testCase+".csv").distinct().collect();
       List<String> resultCBA = envBatch.readTextFile(outputPath + "CBA_"+testCase+".csv").distinct().collect();

       // Compare the results
       assertEquals(resultABC.size(), resultBAC.size());
       assertEquals(resultABC,resultBAC);
       assertEquals(resultABC.size(), resultCAB.size());
       assertEquals(resultABC,resultCAB);
       assertEquals(resultABC.size(), resultACB.size());
       assertEquals(resultABC,resultACB);
       assertEquals(resultABC.size(), resultBCA.size());
       assertEquals(resultABC,resultBCA);
       assertEquals(resultABC.size(), resultCBA.size());
       assertEquals(resultABC,resultCBA);
    }

    @Test
    //Case A4 W1!=W2, s >= l, W1.l != W2.l && W1.s == W2.s
    public void testCaseA4_w1_lt_w2() throws Exception {
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
                new SWJ_ab_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // this is commutative it works without any 'magic' akka default
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBAC =
                new SWJ_ab_BAC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // this works via time Propagation of a.ts by default
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamACB =
                new SWJ_ac_AC_w2_B_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // consequently via commutativity this one also
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamCAB =
                new SWJ_ac_CA_w2_B_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // same problem as in A! there b and c can be so far apart that they do not occur together in a window
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA =
                new SWJ_bc_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamCBA =
                new SWJ_bc_CB_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();


        String outputPath = "./src/main/resources/resultSWJ_";
        // Collect the results into lists
        streamABC
                .writeAsText(outputPath + "ABC_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBAC
                .writeAsText(outputPath + "BAC_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamACB
                .writeAsText(outputPath + "ACB_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamCAB
                .writeAsText(outputPath + "CAB_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA
                .writeAsText(outputPath + "BCA_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamCBA
                .writeAsText(outputPath + "CBA_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC = envBatch.readTextFile(outputPath + "ABC_"+testCase+".csv").distinct().collect();
        List<String> resultBAC = envBatch.readTextFile(outputPath + "BAC_"+testCase+".csv").distinct().collect();
        List<String> resultACB = envBatch.readTextFile(outputPath + "ACB_"+testCase+".csv").distinct().collect();
        List<String> resultCAB = envBatch.readTextFile(outputPath + "CAB_"+testCase+".csv").distinct().collect();
        List<String> resultBCA = envBatch.readTextFile(outputPath + "BCA_"+testCase+".csv").distinct().collect();
        List<String> resultCBA = envBatch.readTextFile(outputPath + "CBA_"+testCase+".csv").distinct().collect();

        // Compare the results
        assertEquals(resultABC.size(), resultBAC.size());
        assertEquals(resultABC,resultBAC);
        assertEquals(resultABC.size(), resultACB.size());
        assertEquals(resultABC,resultACB);
        assertEquals(resultABC.size(), resultCAB.size());
        assertEquals(resultABC,resultCAB);
        assertEquals(resultABC.size(), resultBCA.size());
        assertEquals(resultABC,resultBCA);
        assertEquals(resultABC.size(), resultCBA.size());
        assertEquals(resultABC,resultCBA);
    }

    @Test
    //Case A5: Session Windows
    public void testCaseA5() throws Exception {
        w1Size = 3;
        w2Size = 3;
        timePropagation = "B";
        String testCase = "A5";
        // Execute each join operation
        //default case
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC =
                new SeWJ_ab_ABC(streamA, streamB, streamC, w1Size, w2Size, timePropagation).run();
        // this is commutative it works without any 'magic' akka default
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBAC =
                new SeWJ_ab_BAC(streamA, streamB, streamC, w1Size, w2Size, timePropagation).run();
        // this works via time Propagation of a.ts by default
        timePropagation = "A";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamACB =
                new SeWJ_ac_ACB(streamA, streamB, streamC, w1Size, w2Size, timePropagation).run();
        // consequently via commutativity this one also
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamCAB =
                new SeWJ_ac_CAB(streamA, streamB, streamC, w1Size, w2Size, timePropagation).run();
        timePropagation = "B";
        // same problem as in A! there b and c can be so far apart that they do not occur together in a window
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA =
                new SeWJ_bc_BCA(streamA, streamB, streamC, w1Size, w2Size, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamCBA =
                new SeWJ_bc_CBA(streamA, streamB, streamC, w1Size, w2Size, timePropagation).run();

        String outputPath = "./src/main/resources/resultSeWJ_";
        // Collect the results into lists
        streamABC
                .writeAsText(outputPath + "ABC_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBAC
                .writeAsText(outputPath + "BAC_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamACB
                .writeAsText(outputPath + "ACB_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamCAB
                .writeAsText(outputPath + "CAB_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA
                .writeAsText(outputPath + "BCA_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamCBA
                .writeAsText(outputPath + "CBA_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC = envBatch.readTextFile(outputPath + "ABC_"+testCase+".csv").distinct().collect();
        List<String> resultBAC = envBatch.readTextFile(outputPath + "BAC_"+testCase+".csv").distinct().collect();
        List<String> resultACB = envBatch.readTextFile(outputPath + "ACB_"+testCase+".csv").distinct().collect();
        List<String> resultCAB = envBatch.readTextFile(outputPath + "CAB_"+testCase+".csv").distinct().collect();
        List<String> resultBCA = envBatch.readTextFile(outputPath + "BCA_"+testCase+".csv").distinct().collect();
        List<String> resultCBA = envBatch.readTextFile(outputPath + "CBA_"+testCase+".csv").distinct().collect();

        // Compare the results
        assertEquals(resultABC.size(), resultBAC.size());
        assertEquals(resultABC,resultBAC);
        assertNotEquals(resultABC,resultACB);
        assertNotEquals(resultABC,resultCAB);
        assertNotEquals(resultABC,resultBCA);
        assertNotEquals(resultABC,resultCBA);
    }

    @Test
    //Case A6 IVJ lB = uB
    public void testCaseA6() throws Exception {
        // Set up the testing environment
        w1Size = -10; // lB W1
        w1Slide = 10; // uB W1
        w2Size = -10; // lB W1
        w2Slide = 10; // uB W1

        timePropagation = "A";
        String testCase = "A6";
        // Execute each join operation with timestamp of A
        //default case
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC =
                new IVJ_ab_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // this is commutative it works without any 'magic' akka default
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBAC =
                new IVJ_ab_BAC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // this works via time Propagation of a.ts by default
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamACB =
                new IVJ_ac_ACB(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // consequently via commutativity this one also
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamCAB =
                new IVJ_ac_CAB(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // same problem as in A! there b and c can be so far apart that they do not occur together in a window
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA =
                new IVJ_bc_BCA(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
     DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamCBA =
                new IVJ_bc_CBA(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();

        String outputPath = "./src/main/resources/resultSeWJ_";
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
    public void testCaseA6_w1_neq_w2() throws Exception {
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
                new IVJ_ab_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // this is commutative it works without any 'magic' akka default
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBAC =
                new IVJ_ab_BAC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // this works via time Propagation of a.ts by default
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamACB =
                new IVJ_ac_ACB(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // consequently via commutativity this one also
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamCAB =
                new IVJ_ac_CAB(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // same problem as in A! there b and c can be so far apart that they do not occur together in a window
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA =
                new IVJ_bc_BCA(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamCBA =
                new IVJ_bc_CBA(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();

        String outputPath = "./src/main/resources/resultSeWJ_";
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
    public void testCaseA7_w1() throws Exception {
        // Set up the testing environment
        w1Size = -10; // lB W1
        w1Slide = 5; // uB W1
        w2Size = -20; // lB W1
        w2Slide = 20; // uB W1

        timePropagation = "A";
        String testCase = "A7";
        // Execute each join operation with timestamp of A
        //default case
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC =
                new IVJ_ab_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBAC =
                new IVJ_ab_BAC(streamA, streamB, streamC, -w1Slide, -w1Size, w2Size, w2Slide, timePropagation).run();
        // this works via time Propagation of a.ts by default
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamACB =
                new IVJ_ac_ACB(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // consequently via commutativity this one also
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamCAB =
                new IVJ_ac_CAB(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // same problem as in A! there b and c can be so far apart that they do not occur together in a window
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA =
                new IVJ_bc_BCA(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamCBA =
                new IVJ_bc_CBA(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();

        String outputPath = "./src/main/resources/resultSeWJ_";
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
    public void testCaseA7_w2() throws Exception {
        // Set up the testing environment
        w1Size = -10; // lB W1
        w1Slide = 10; // uB W1
        w2Size = -10; // lB W1
        w2Slide = 20; // uB W1

        timePropagation = "A";
        String testCase = "A7";
        // Execute each join operation with timestamp of A
        //default case
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC =
                new IVJ_ab_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBAC =
                new IVJ_ab_BAC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // this works via time Propagation of a.ts by default
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamACB =
                new IVJ_ac_ACB(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // consequently via commutativity this one also
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamCAB =
                new IVJ_ac_CAB(streamA, streamB, streamC, w1Size, w1Slide, -w2Slide, -w2Size, timePropagation).run();
        // same problem as in A! there b and c can be so far apart that they do not occur together in a window
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA =
                new IVJ_bc_BCA(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamCBA =
                new IVJ_bc_CBA(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();

        String outputPath = "./src/main/resources/resultSeWJ_";
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
    public void testCaseA7_w1_and_w2() throws Exception {
        // Set up the testing environment
        w1Size = -5; // lB W1
        w1Slide = 10; // uB W1
        w2Size = -10; // lB W1
        w2Slide = 20; // uB W1

        timePropagation = "A";
        String testCase = "A7";
        // Execute each join operation with timestamp of A
        //default case
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC =
                new IVJ_ab_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBAC =
                new IVJ_ab_BAC(streamA, streamB, streamC, -w1Slide, -w1Size, w2Size, w2Slide, timePropagation).run();
        // this works via time Propagation of a.ts by default
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamACB =
                new IVJ_ac_ACB(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // consequently via commutativity this one also
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamCAB =
                new IVJ_ac_CAB(streamA, streamB, streamC, w1Size, w1Slide, -w2Slide, -w2Size, timePropagation).run();
        // same problem as in A! there b and c can be so far apart that they do not occur together in a window
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA =
                new IVJ_bc_BCA(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamCBA =
                new IVJ_bc_CBA(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();

        String outputPath = "./src/main/resources/resultSeWJ_";
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
}
