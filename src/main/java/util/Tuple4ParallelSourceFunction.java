package util;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.Scanner;

public class Tuple4ParallelSourceFunction extends RichParallelSourceFunction<Tuple4<Integer, Integer, Long, Long>> {
    private volatile boolean isRunning = true;
    public static final int RECORD_SIZE_IN_BYTE = 89;
    private String key;
    private Integer sensors;
    private String file;  // the source file
    private Integer sourceLoops;  //integer runs over file
    private long currentTime;
    private String delimiter = ",";
    private String streamIdentifier = "Q";
    private boolean manipulateIngestionRate = false;
    private long throughput;
    private int runtime = 25;

    public Tuple4ParallelSourceFunction(String fileName, Integer sensors, String delimiter, long throughput) {
        this.file = fileName;
        this.key = null;
        this.sensors = sensors;
        this.sourceLoops = 1;
        this.delimiter = delimiter;
        this.throughput = throughput;
        if (throughput == 0) {
            this.manipulateIngestionRate = false;
        } else {
            this.manipulateIngestionRate = true;
        }
    }

    public Tuple4ParallelSourceFunction(String fileName, Integer sensors, String delimiter, long throughput, String streamIdentifier) {
        this.file = fileName;
        this.key = null;
        this.sensors = sensors;
        this.sourceLoops = 1;
        this.delimiter = delimiter;
        this.throughput = throughput;
        if (throughput == 0) {
            this.manipulateIngestionRate = false;
        } else {
            this.manipulateIngestionRate = true;
        }
    }

    public void run(SourceContext<Tuple4<Integer, Integer, Long, Long>> sourceContext) throws Exception {
        long startTime = System.currentTimeMillis();
        boolean run = true;

        // 4 schemas
        // (1) QnV_large.csv
        // key, POINT(long,lat), ts, velo, quant, level
        //R2024876,POINT (8.769070382479487 50.79940709802762),1547424000000,34.666666666666664,2.0,F
        // (2)
        // R2000073,1543622400000,64.61111111111111,8.0
        // (3) Luftdaten
        // sensor_id;sensor_type;location;lat;lon;timestamp;P1;durP1;ratioP1;P2;durP2;ratioP2
        //11245;SDS011;5680;49.857;8.646;2018-12-01T00:00:18;5.43;;;5.33;;
        // (4)
        //sensor_id;sensor_type;location;lat;lon;timestamp;temperature;humidity
        //11246;DHT22;5680;49.857;8.646;2018-12-01T00:00:19;11.10;68.90

        try {
            int loopCount = 1;
            long tupleCounter = 0;
            Scanner scan;
            File f = new File(this.file);
            scan = new Scanner(f);

            long start = System.currentTimeMillis();

            while (scan.hasNext() && run) {

                long millisSinceEpoch = 0;
                String rawData = scan.nextLine();
                String[] data = rawData.split(delimiter);

                // key, works for all three data files
                String id;
                if (this.key == null || ((this.key.equals("1") && this.sourceLoops == 1) && !data[0].contains("_id")))
                    id = data[0].trim();
                else id = this.key;

                if (data.length == 4 && data[0].equals("R2000070")) {
                    // parse QnV
                    // time
                    if (this.sourceLoops == 1 || loopCount == 1) {
                        if (data[1].length() == 10) {
                            millisSinceEpoch = Long.parseLong(data[1]) * 1000;
                        } else {
                            millisSinceEpoch = Long.parseLong(data[1]);
                        }
                    } else {
                        this.currentTime += 60000;
                        millisSinceEpoch = this.currentTime;
                    }

                    int velocity = (int) Double.parseDouble(data[2].trim());
                    int quantity = (int) Double.parseDouble(data[3].trim());

                    float longitude = 8.615298750147367f;
                    float latitude = 49.84660732605085f;

                    if (id.equals("R2000073")) {
                        longitude = 8.615174355568845f;
                        latitude = 49.84650797558072f;
                    }

                    int maxPara = this.getRuntimeContext().getNumberOfParallelSubtasks();
                    if (this.sensors >= maxPara) {
                        for (int i = 0; i < (this.sensors / maxPara); i++) {
                            long systemTime = System.currentTimeMillis();
                            if (Objects.equals(this.streamIdentifier, "V")) {

                                Tuple4<Integer, Integer, Long, Long> event = new Tuple4<>((this.getRuntimeContext().getIndexOfThisSubtask() + (maxPara * i)), velocity,
                                        millisSinceEpoch, systemTime);

                                sourceContext.collect(event);
                                tupleCounter++;
                            } else {
                                Tuple4<Integer, Integer, Long, Long> quaEvent = new Tuple4<>((this.getRuntimeContext().getIndexOfThisSubtask() + (maxPara * i)), quantity,
                                        millisSinceEpoch, systemTime);

                                sourceContext.collect(quaEvent);
                                tupleCounter++;
                            }
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

                } else if (data.length == 10 && data[1].equals("SDS011")) {
                    // parse SDS011
                    if (this.sourceLoops == 1 || loopCount == 1) {

                        if (data[5].length() == 10) {
                            millisSinceEpoch = Long.parseLong(data[5]) * 1000;
                        } else {
                            //System.out.println("Before formatting: " + data[5]);
                            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                            LocalDateTime timestampDT = LocalDateTime.parse(data[5].trim().replace("T", " "), dtf);
                            //System.out.println("after formatting: " + timestampDT);
                            millisSinceEpoch = timestampDT.atZone(ZoneId.systemDefault())
                                    .toInstant().toEpochMilli();

                        }
                    } else {
                        this.currentTime += (Math.floor(Math.random() * (6 - 3) + 3)) * 60000;
                        millisSinceEpoch = this.currentTime;
                    }

                    int maxPara = this.getRuntimeContext().getNumberOfParallelSubtasks();
                    if (this.sensors >= maxPara) {
                        for (int i = 0; i < (this.sensors / maxPara); i++) {
                            long systemTime = System.currentTimeMillis();
                            if (Objects.equals(this.streamIdentifier, "PM10")) {
                                int p10 = (int) Double.parseDouble(data[6].trim());
                                Tuple4<Integer, Integer, Long, Long> event = new Tuple4<>((this.getRuntimeContext().getIndexOfThisSubtask() + (maxPara * i)), p10,
                                        millisSinceEpoch, systemTime);

                                sourceContext.collect(event);
                                tupleCounter++;
                            } else {
                                int p2 = (int) Double.parseDouble(data[9].trim());
                                Tuple4<Integer, Integer, Long, Long> quaEvent = new Tuple4<>((this.getRuntimeContext().getIndexOfThisSubtask() + (maxPara * i)), p2,
                                        millisSinceEpoch, systemTime);

                                sourceContext.collect(quaEvent);
                                tupleCounter++;
                            }

                        }
                    } else {
                        run = false;
                        //TODO that code produces duplicates
                        /** int keyP = this.getRuntimeContext().getIndexOfThisSubtask();
                         if(keyP < this.sensors){
                         KeyedDataPointGeneral P1Event = new PartMatter10Event(Integer.toString(keyP),
                         millisSinceEpoch, p10, longitude, latitude);

                         sourceContext.collect(P1Event);
                         tupleCounter++;

                         KeyedDataPointGeneral P2Event = new PartMatter2Event(Integer.toString(keyP),
                         millisSinceEpoch, p2, longitude, latitude);

                         sourceContext.collect(P2Event);
                         tupleCounter++;
                         }else{
                         run = false;
                         }*/
                    }
                } else if (data.length == 8 && data[1].equals("DHT22")) {
                    // parse SDS011
                    if (this.sourceLoops == 1 || loopCount == 1) {

                        if (data[5].length() == 10) {
                            millisSinceEpoch = Long.parseLong(data[5]) * 1000;
                        } else {
                            //System.out.println("Before formatting: " + data[5]);
                            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                            LocalDateTime timestampDT = LocalDateTime.parse(data[5].trim().replace("T", " "), dtf);
                            //System.out.println("after formatting: " + timestampDT);
                            millisSinceEpoch = timestampDT.atZone(ZoneId.systemDefault())
                                    .toInstant().toEpochMilli();

                        }
                    } else {
                        this.currentTime += (Math.floor(Math.random() * (6 - 3) + 3)) * 60000;
                        millisSinceEpoch = this.currentTime;
                    }

                    int maxPara = this.getRuntimeContext().getNumberOfParallelSubtasks();
                    if (this.sensors >= maxPara) {
                        for (int i = 0; i < (this.sensors / maxPara); i++) {
                            long systemTime = System.currentTimeMillis();
                            if (Objects.equals(this.streamIdentifier, "Temp")) {
                                int p10 = (int) Double.parseDouble(data[6].trim());
                                Tuple4<Integer, Integer, Long, Long> event = new Tuple4<>((this.getRuntimeContext().getIndexOfThisSubtask() + (maxPara * i)), p10,
                                        millisSinceEpoch, systemTime);

                                sourceContext.collect(event);
                                tupleCounter++;
                            } else {
                                int p2 = (int) Double.parseDouble(data[7].trim());
                                Tuple4<Integer, Integer, Long, Long> quaEvent = new Tuple4<>((this.getRuntimeContext().getIndexOfThisSubtask() + (maxPara * i)), p2,
                                        millisSinceEpoch, systemTime);

                                sourceContext.collect(quaEvent);
                                tupleCounter++;
                            }

                        }
                    } else {
                        run = false;
                        //TODO that code produces duplicates
                        /** int keyP = this.getRuntimeContext().getIndexOfThisSubtask();
                         if(keyP < this.sensors){
                         KeyedDataPointGeneral P1Event = new PartMatter10Event(Integer.toString(keyP),
                         millisSinceEpoch, p10, longitude, latitude);

                         sourceContext.collect(P1Event);
                         tupleCounter++;

                         KeyedDataPointGeneral P2Event = new PartMatter2Event(Integer.toString(keyP),
                         millisSinceEpoch, p2, longitude, latitude);

                         sourceContext.collect(P2Event);
                         tupleCounter++;
                         }else{
                         run = false;
                         }*/
                    }
                }

                if (!scan.hasNext() && loopCount < this.sourceLoops) {
                    scan = new Scanner(f);
                    loopCount++;
                    this.currentTime = millisSinceEpoch;
                }

                if (tupleCounter >= throughput && manipulateIngestionRate) {
                    long now = System.currentTimeMillis();
                    if ((1000 - (now - start)) > 0) {
                        Thread.sleep(1000 - (now - start));
                    }
                    if ((now - startTime) >= this.runtime * 60000L) {
                        run = false;
                    }
                    tupleCounter = 0;
                    start = System.currentTimeMillis();
                }
            }
            scan.close();

        } catch (NumberFormatException nfe) {
            nfe.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void cancel() {
        this.isRunning = false;
    }
}
