package util;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

public class ArtificalSourceFunction extends RichParallelSourceFunction<Tuple3<Integer, Integer, Long>> {
    private volatile boolean isRunning = true;
    public static final int RECORD_SIZE_IN_BYTE = 89;
    private long throughput;
    boolean manipulateIngestionRate;
    private int windowsize;

    private int numberOfKeys;
    private long startTime;
    private int runtime;
    double selectivity;

    public ArtificalSourceFunction(long throughput, int windowsize, double selectivity, int numberOfKeys) {
        this.numberOfKeys = numberOfKeys;
        this.runtime = 2;
        this.windowsize = windowsize;
        this.throughput = throughput;
        this.selectivity = selectivity;
        if (throughput == 0) {
            this.manipulateIngestionRate = false;
        } else {
            this.manipulateIngestionRate = true;
        }
    }

    public ArtificalSourceFunction(long throughput, int windowsize, int runtime, int selectivity) {
        this.runtime = runtime;
        this.numberOfKeys = this.getRuntimeContext().getNumberOfParallelSubtasks();
        this.windowsize = windowsize;
        this.throughput = throughput;
        this.selectivity = selectivity;
        if (throughput == 0) {
            this.manipulateIngestionRate = false;
        } else {
            this.manipulateIngestionRate = true;
        }
    }

    /***
     * Run method of the source, creates a stream of Tuple3()
     * runs for the period of runtime, e.g., 20 Minutes
     * @param sourceContext
     * @throws Exception
     */
    @Override
    public void run(SourceContext<Tuple3<Integer, Integer, Long>> sourceContext) throws Exception {
        long start = System.currentTimeMillis();
        this.startTime = start;
        boolean run = true;
        long tupleCounter = 0;
        long millisSinceEpoch = 0L;

        Random r = new Random();
        while(run) {
            long now = System.currentTimeMillis();

            Integer key = 0;

            int maxPara = this.getRuntimeContext().getNumberOfParallelSubtasks();
            if (this.numberOfKeys >= maxPara) {
                for (int i = 0; i < (this.numberOfKeys / maxPara); i++) {
                    key = this.getRuntimeContext().getIndexOfThisSubtask() + (maxPara * i);
                }
            } else {
                run = false;
                //TODO that code produces duplicates
                /**int keyP = this.getRuntimeContext().getIndexOfThisSubtask();
                 if(keyP < this.sensors){
                 KeyedDataPointGeneral velEvent = new VelocityEvent(Integer.toString(keyP),
                 millisSinceEpoch, velocity, longitude, latitude);

                 sourceContext.collect(velEvent);
                 tupleCounter++;

                 KeyedDataPointGeneral quaEvent = new QuantityEvent(Integer.toString(keyP),
                 millisSinceEpoch, quantity, longitude, latitude);

                 sourceContext.collect(quaEvent);
                 tupleCounter++;
                 }else{
                 run = false;
                 }*/
            }


            Integer value = r.nextInt();
            long eventTime = millisSinceEpoch;

            Tuple3<Integer, Integer, Long> event = new Tuple3<>(key, value, eventTime);
            sourceContext.collect(event); // output tuple
            tupleCounter++; // increase tuple counter
            millisSinceEpoch += 60000L; //increase event time by 1 minute

            // check if the tuple counts equals the defined throughput (per second)
            if (tupleCounter >= throughput && manipulateIngestionRate) {
                    // if tuples were creates faster then in 1 second, wait
                if (((1000 - (now - start)) > 0) && ((now - this.startTime) < this.runtime * 60000L)) {
                    Thread.sleep(1000 - (now - start));
                    // in case runtime is reached stop producing tuples
                }
                // reset parameters
                tupleCounter = 0;
                start = System.currentTimeMillis();
            }
            if ((now - this.startTime) >= this.runtime * 60000L) {
                System.out.println("stop");
                run = false;
                break;
            }
        }
    }

    public void cancel() {
        this.isRunning = false;
    }
}
