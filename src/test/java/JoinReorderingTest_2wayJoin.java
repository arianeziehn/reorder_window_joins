import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Before;
import org.junit.Test;
import util.UDFs;

import java.util.List;

import static java.lang.Math.max;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class JoinReorderingTest_2wayJoin {

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

        // Create test streams A, B, C with some example data
        streamA = env.fromElements(
                new Tuple3<>(1, 2, (1*60000L)),
                new Tuple3<>(1, 3, (3*60000L)),
                new Tuple3<>(1, 2, (5*60000L)),
                new Tuple3<>(1, 3, (7*60000L)),
                new Tuple3<>(1, 2, (9*60000L)),
                new Tuple3<>(1, 3, (11*60000L)),
                new Tuple3<>(1, 2, (13*60000L)),
                new Tuple3<>(1, 3, (15*60000L)),
                new Tuple3<>(1, 2, (17*60000L)),
                new Tuple3<>(1, 3, (19*60000L)),
                new Tuple3<>(1, 2, (21*60000L)),
                new Tuple3<>(1, 3, (23*60000L)),
                new Tuple3<>(1, 2, (25*60000L)),
                new Tuple3<>(1, 3, (27*60000L)),
                new Tuple3<>(1, 2, (29*60000L)),
                new Tuple3<>(1, 3, (31*60000L)),
                new Tuple3<>(1, 2, (33*60000L)),
                new Tuple3<>(1, 3, (35*60000L)),
                new Tuple3<>(1, 2, (37*60000L)),
                new Tuple3<>(1, 3, (39*60000L)),
                new Tuple3<>(1, 2, (41*60000L)),
                new Tuple3<>(1, 3, (43*60000L)),
                new Tuple3<>(1, 2, (45*60000L)),
                new Tuple3<>(1, 3, (47*60000L)),
                new Tuple3<>(1, 2, (49*60000L)),
                new Tuple3<>(1, 3, (51*60000L)),
                new Tuple3<>(1, 2, (53*60000L)),
                new Tuple3<>(1, 3, (55*60000L)),
                new Tuple3<>(1, 2, (57*60000L)),
                new Tuple3<>(1, 3, (59*60000L)),
                new Tuple3<>(1, 2, (61*60000L)),
                new Tuple3<>(1, 3, (63*60000L)),
                new Tuple3<>(1, 2, (65*60000L)),
                new Tuple3<>(1, 3, (67*60000L)),
                new Tuple3<>(1, 2, (69*60000L)),
                new Tuple3<>(1, 3, (71*60000L)),
                new Tuple3<>(1, 2, (73*60000L)),
                new Tuple3<>(1, 3, (75*60000L)),
                new Tuple3<>(1, 2, (77*60000L)),
                new Tuple3<>(1, 3, (79*60000L))
        ).assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp(1000));

        streamB = env.fromElements(
                new Tuple3<>(1, 2, (2*60000L)),
                new Tuple3<>(1, 3, (4*60000L)),
                new Tuple3<>(1, 2, (6*60000L)),
                new Tuple3<>(1, 3, (8*60000L)),
                new Tuple3<>(1, 2, (10*60000L)),
                new Tuple3<>(1, 3, (12*60000L)),
                new Tuple3<>(1, 2, (14*60000L)),
                new Tuple3<>(1, 3, (16*60000L)),
                new Tuple3<>(1, 2, (18*60000L)),
                new Tuple3<>(1, 3, (20*60000L)),
                new Tuple3<>(1, 2, (22*60000L)),
                new Tuple3<>(1, 3, (24*60000L)),
                new Tuple3<>(1, 2, (26*60000L)),
                new Tuple3<>(1, 3, (28*60000L)),
                new Tuple3<>(1, 2, (30*60000L)),
                new Tuple3<>(1, 3, (32*60000L)),
                new Tuple3<>(1, 2, (34*60000L)),
                new Tuple3<>(1, 3, (36*60000L)),
                new Tuple3<>(1, 2, (38*60000L)),
                new Tuple3<>(1, 3, (40*60000L)),
                new Tuple3<>(1, 2, (42*60000L)),
                new Tuple3<>(1, 3, (44*60000L)),
                new Tuple3<>(1, 2, (46*60000L)),
                new Tuple3<>(1, 3, (48*60000L)),
                new Tuple3<>(1, 3, (50*60000L)),
                new Tuple3<>(1, 2, (52*60000L)),
                new Tuple3<>(1, 3, (54*60000L)),
                new Tuple3<>(1, 2, (56*60000L)),
                new Tuple3<>(1, 3, (58*60000L)),
                new Tuple3<>(1, 2, (60*60000L)),
                new Tuple3<>(1, 3, (62*60000L)),
                new Tuple3<>(1, 2, (64*60000L)),
                new Tuple3<>(1, 3, (66*60000L)),
                new Tuple3<>(1, 2, (68*60000L)),
                new Tuple3<>(1, 3, (70*60000L)),
                new Tuple3<>(1, 2, (72*60000L)),
                new Tuple3<>(1, 3, (74*60000L)),
                new Tuple3<>(1, 2, (76*60000L)),
                new Tuple3<>(1, 3, (78*60000L)),
                new Tuple3<>(1, 2, (80*60000L)),
                new Tuple3<>(1, 3, (82*60000L))
        ).assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp(1000));

        streamC = env.fromElements(
                new Tuple3<>(1, 2, (1*60000L)),
                new Tuple3<>(1, 3, (3*60000L)),
                new Tuple3<>(1, 2, (5*60000L)),
                new Tuple3<>(1, 3, (7*60000L)),
                new Tuple3<>(1, 2, (9*60000L)),
                new Tuple3<>(1, 3, (11*60000L)),
                new Tuple3<>(1, 2, (13*60000L)),
                new Tuple3<>(1, 3, (15*60000L)),
                new Tuple3<>(1, 2, (17*60000L)),
                new Tuple3<>(1, 3, (19*60000L)),
                new Tuple3<>(1, 2, (21*60000L)),
                new Tuple3<>(1, 3, (23*60000L)),
                new Tuple3<>(1, 2, (25*60000L)),
                new Tuple3<>(1, 3, (27*60000L)),
                new Tuple3<>(1, 2, (29*60000L)),
                new Tuple3<>(1, 3, (31*60000L)),
                new Tuple3<>(1, 2, (33*60000L)),
                new Tuple3<>(1, 3, (35*60000L)),
                new Tuple3<>(1, 2, (37*60000L)),
                new Tuple3<>(1, 3, (39*60000L)),
                new Tuple3<>(1, 2, (41*60000L)),
                new Tuple3<>(1, 3, (43*60000L)),
                new Tuple3<>(1, 2, (45*60000L)),
                new Tuple3<>(1, 3, (47*60000L)),
                new Tuple3<>(1, 2, (49*60000L)),
                new Tuple3<>(1, 3, (51*60000L)),
                new Tuple3<>(1, 2, (53*60000L)),
                new Tuple3<>(1, 3, (55*60000L)),
                new Tuple3<>(1, 2, (57*60000L)),
                new Tuple3<>(1, 3, (59*60000L)),
                new Tuple3<>(1, 2, (61*60000L)),
                new Tuple3<>(1, 3, (63*60000L)),
                new Tuple3<>(1, 2, (65*60000L)),
                new Tuple3<>(1, 3, (67*60000L)),
                new Tuple3<>(1, 2, (69*60000L)),
                new Tuple3<>(1, 3, (71*60000L)),
                new Tuple3<>(1, 2, (73*60000L)),
                new Tuple3<>(1, 3, (75*60000L)),
                new Tuple3<>(1, 2, (77*60000L)),
                new Tuple3<>(1, 3, (79*60000L))
        ).assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp(1000));
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
       // if w1.l > w2.l and the first join does not contain the propageted timestamp, we use the largest window in the first join
       // and  propagate the timestamp of the smallest to the second join
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
        streamA = env.fromElements(
                new Tuple3<>(1, 2, (1*60000L)),
                new Tuple3<>(1, 3, (3*60000L)),
                new Tuple3<>(1, 2, (5*60000L)),
                new Tuple3<>(1, 3, (7*60000L)),
                new Tuple3<>(1, 2, (9*60000L)),
                new Tuple3<>(1, 3, (11*60000L)),
                new Tuple3<>(1, 2, (13*60000L)),
                new Tuple3<>(1, 3, (15*60000L)),
                new Tuple3<>(1, 2, (17*60000L)),
                new Tuple3<>(1, 3, (19*60000L)),
                new Tuple3<>(1, 3, (31*60000L)),
                new Tuple3<>(1, 2, (33*60000L)),
                new Tuple3<>(1, 3, (35*60000L)),
                new Tuple3<>(1, 2, (37*60000L)),
                new Tuple3<>(1, 3, (39*60000L)),
                new Tuple3<>(1, 2, (49*60000L)),
                new Tuple3<>(1, 3, (51*60000L)),
                new Tuple3<>(1, 2, (53*60000L)),
                new Tuple3<>(1, 3, (55*60000L)),
                new Tuple3<>(1, 2, (57*60000L)),
                new Tuple3<>(1, 3, (59*60000L)),
                new Tuple3<>(1, 2, (61*60000L)),
                new Tuple3<>(1, 3, (67*60000L)),
                new Tuple3<>(1, 2, (69*60000L)),
                new Tuple3<>(1, 3, (71*60000L)),
                new Tuple3<>(1, 2, (73*60000L)),
                new Tuple3<>(1, 3, (75*60000L)),
                new Tuple3<>(1, 2, (77*60000L)),
                new Tuple3<>(1, 3, (79*60000L))
        ).assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp(1000));

        streamB = env.fromElements(
                new Tuple3<>(1, 2, (2*60000L)),
                new Tuple3<>(1, 3, (4*60000L)),
                new Tuple3<>(1, 2, (6*60000L)),
                new Tuple3<>(1, 3, (8*60000L)),
                new Tuple3<>(1, 2, (10*60000L)),
                new Tuple3<>(1, 3, (12*60000L)),
                new Tuple3<>(1, 2, (14*60000L)),
                new Tuple3<>(1, 3, (16*60000L)),
                new Tuple3<>(1, 2, (18*60000L)),
                new Tuple3<>(1, 3, (20*60000L)),
                new Tuple3<>(1, 2, (30*60000L)),
                new Tuple3<>(1, 3, (32*60000L)),
                new Tuple3<>(1, 2, (34*60000L)),
                new Tuple3<>(1, 3, (36*60000L)),
                new Tuple3<>(1, 2, (38*60000L)),
                new Tuple3<>(1, 3, (40*60000L)),
                new Tuple3<>(1, 2, (42*60000L)),
                new Tuple3<>(1, 3, (50*60000L)),
                new Tuple3<>(1, 2, (52*60000L)),
                new Tuple3<>(1, 3, (54*60000L)),
                new Tuple3<>(1, 2, (56*60000L)),
                new Tuple3<>(1, 3, (58*60000L)),
                new Tuple3<>(1, 2, (60*60000L)),
                new Tuple3<>(1, 3, (62*60000L)),
                new Tuple3<>(1, 2, (68*60000L)),
                new Tuple3<>(1, 3, (70*60000L)),
                new Tuple3<>(1, 2, (72*60000L)),
                new Tuple3<>(1, 3, (74*60000L)),
                new Tuple3<>(1, 2, (76*60000L)),
                new Tuple3<>(1, 3, (78*60000L)),
                new Tuple3<>(1, 2, (80*60000L)),
                new Tuple3<>(1, 3, (82*60000L))
        ).assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp(1000));

        streamC = env.fromElements(
                new Tuple3<>(1, 2, (1*60000L)),
                new Tuple3<>(1, 3, (3*60000L)),
                new Tuple3<>(1, 2, (5*60000L)),
                new Tuple3<>(1, 3, (7*60000L)),
                new Tuple3<>(1, 2, (9*60000L)),
                new Tuple3<>(1, 3, (11*60000L)),
                new Tuple3<>(1, 2, (13*60000L)),
                new Tuple3<>(1, 3, (15*60000L)),
                new Tuple3<>(1, 2, (17*60000L)),
                new Tuple3<>(1, 3, (19*60000L)),
                new Tuple3<>(1, 2, (21*60000L)),
                new Tuple3<>(1, 3, (23*60000L)),
                new Tuple3<>(1, 3, (31*60000L)),
                new Tuple3<>(1, 2, (33*60000L)),
                new Tuple3<>(1, 3, (35*60000L)),
                new Tuple3<>(1, 2, (37*60000L)),
                new Tuple3<>(1, 3, (39*60000L)),
                new Tuple3<>(1, 2, (41*60000L)),
                new Tuple3<>(1, 3, (43*60000L)),
                new Tuple3<>(1, 2, (45*60000L)),
                new Tuple3<>(1, 3, (47*60000L)),
                new Tuple3<>(1, 2, (49*60000L)),
                new Tuple3<>(1, 3, (51*60000L)),
                new Tuple3<>(1, 2, (53*60000L)),
                new Tuple3<>(1, 3, (55*60000L)),
                new Tuple3<>(1, 2, (57*60000L)),
                new Tuple3<>(1, 3, (59*60000L)),
                new Tuple3<>(1, 3, (71*60000L)),
                new Tuple3<>(1, 2, (73*60000L)),
                new Tuple3<>(1, 3, (75*60000L)),
                new Tuple3<>(1, 2, (77*60000L)),
                new Tuple3<>(1, 3, (79*60000L))
        ).assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp(1000));

        w1Size = 5;
        w2Size = 5;
        timePropagation = "A";
        String testCase = "A5";
        // Execute each join operation
        //default case
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC =
                new SeWJ_ab_ABC(streamA, streamB, streamC, w1Size, w2Size, timePropagation).run();
        // this is commutative it works without any 'magic' akka default
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBAC =
                new SeWJ_ab_BAC(streamA, streamB, streamC, w1Size, w2Size, timePropagation).run();
        // this works via time Propagation of a.ts by default
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamACB =
                new SeWJ_ac_ACB(streamA, streamB, streamC, w1Size, w2Size, timePropagation).run();
        // consequently via commutativity this one also
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamCAB =
                new SeWJ_ac_CAB(streamA, streamB, streamC, w1Size, w2Size, timePropagation).run();
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
        assertEquals(resultABC.size(), resultACB.size());
        assertEquals(resultABC,resultACB);
        assertEquals(resultABC.size(), resultCAB.size());
        assertEquals(resultABC,resultCAB);
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

    @Test
    //Test Case with mixed windows
    public void testMixedWindow() throws Exception {
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

}
