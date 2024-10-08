#!/usr/bin/env bash
startflink='/home/ziehn-ldap/flink-1.11.6_W/bin/start-cluster.sh'
stopflink='/home/ziehn-ldap/flink-1.11.6_W/bin/stop-cluster.sh'
flink='/home/ziehn-ldap/flink-1.11.6_W/bin/flink'
resultFile='/local-ssd/ziehn-ldap/BaselineExp_IVJoins.txt'
jar='/home/ziehn-ldap/flink-joinOrder-1.0-SNAPSHOT_W.jar'
output_path='/local-ssd/ziehn-ldap/result_IVJ'
# all new setting, except last setting of 10,20!
now=$(date +"%T")
today=$(date +%d.%m.%y)
echo "Current time : $today $now" >>$resultFile
echo "----------$today $now------------" >>$resultFile
for loop in 1 2 3 4 5; do
# Case lb > ub, window size 10
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    #2500 is too much: 2000 96009
    $flink run -c IVJClusterT4 $jar --output $output_path --tput 2000 --w1lB 10 --w1uB 0 --w2lB 10 --w2uB 20 --run 25 --order ABC --freqA 15 --freqB 15 --freqC 15 --para 16 --keys 16
    END=$(date +%s)
    DIFF=$((END - START))
    echo "IVJ_ABC run w1(10,0) w2(10,0) "$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.out' '/home/ziehn-ldap/BaselineExp/result_IVJ/FOut_IVJ_10_0_ABC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.out' '/home/ziehn-ldap/BaselineExp/result_IVJ/FOut_IVJ_10_0_ABC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.log' '/home/ziehn-ldap/BaselineExp/result_IVJ/FLog_IVJ_10_0_ABC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.log' '/home/ziehn-ldap/BaselineExp/result_IVJ/FLog_IVJ_10_0_ABC_'$loop'.txt'
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    #2550 is too much
    $flink run -c IVJClusterT4 $jar --output $output_path --tput 2000 --w1lB 10 --w1uB 0 --w2lB 10 --w2uB 20 --run 25 --order ACB --freqA 15 --freqB 15 --freqC 15 --para 16 --keys 16
    END=$(date +%s)
    DIFF=$((END - START))
    echo "IVJ_ACB run w1(10,0) w2(10,0) "$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.out' '/home/ziehn-ldap/BaselineExp/result_IVJ/FOut_IVJ_10_0_ACB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.out' '/home/ziehn-ldap/BaselineExp/result_IVJ/FOut_IVJ_10_0_ACB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.log' '/home/ziehn-ldap/BaselineExp/result_IVJ/FLog_IVJ_10_0_ACB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.log' '/home/ziehn-ldap/BaselineExp/result_IVJ/FLog_IVJ_10_0_ACB_'$loop'.txt'
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    #2550 is too much
    $flink run -c IVJClusterT4 $jar --output $output_path --tput 2000 --w1lB 10 --w1uB 0 --w2lB 10 --w2uB 20 --run 25 --order BAC --freqA 15 --freqB 15 --freqC 15 --para 16 --keys 16
    END=$(date +%s)
    DIFF=$((END - START))
    echo "IVJ_BAC run w1(10,0) w2(10,0) "$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.out' '/home/ziehn-ldap/BaselineExp/result_IVJ/FOut_IVJ_10_0_BAC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.out' '/home/ziehn-ldap/BaselineExp/result_IVJ/FOut_IVJ_10_0_BAC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.log' '/home/ziehn-ldap/BaselineExp/result_IVJ/FLog_IVJ_10_0_BAC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.log' '/home/ziehn-ldap/BaselineExp/result_IVJ/FLog_IVJ_10_0_BAC_'$loop'.txt'
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    #2500 is too much
    $flink run -c IVJClusterT4 $jar --output $output_path --tput 2000 --w1lB 10 --w1uB 0 --w2lB 10 --w2uB 20 --run 25 --order CAB --freqA 15 --freqB 15 --freqC 15 --para 16 --keys 16
    END=$(date +%s)
    DIFF=$((END - START))
    echo "IVJ_CAB run w1(10,0) w2(10,0) "$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.out' '/home/ziehn-ldap/BaselineExp/result_IVJ/FOut_IVJ_10_0_CAB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.out' '/home/ziehn-ldap/BaselineExp/result_IVJ/FOut_IVJ_10_0_CAB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.log' '/home/ziehn-ldap/BaselineExp/result_IVJ/FLog_IVJ_10_0_CAB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.log' '/home/ziehn-ldap/BaselineExp/result_IVJ/FLog_IVJ_10_0_CAB_'$loop'.txt'
# Case lb = ub, window size 20
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    #2000 okay but high
    $flink run -c IVJClusterT4 $jar --output $output_path --tput 1500 --w1lB 10 --w1uB 10 --w2lB 10 --w2uB 10 --run 25 --order ABC --freqA 15 --freqB 15 --freqC 15 --para 16 --keys 16
    END=$(date +%s)
    DIFF=$((END - START))
    echo "IVJ_ABC run w1(10,10) w2(10,10) "$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.out' '/home/ziehn-ldap/BaselineExp/result_IVJ/FOut_IVJ_10_10_ABC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.out' '/home/ziehn-ldap/BaselineExp/result_IVJ/FOut_IVJ_10_10_ABC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.log' '/home/ziehn-ldap/BaselineExp/result_IVJ/FLog_IVJ_10_10_ABC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.log' '/home/ziehn-ldap/BaselineExp/result_IVJ/FLog_IVJ_10_10_ABC_'$loop'.txt'
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    #2500 too high
    $flink run -c IVJClusterT4 $jar --output $output_path --tput 6100 --w1lB 10 --w1uB 10 --w2lB 10 --w2uB 10 --run 25 --order ACB --freqA 30 --freqB 15 --freqC 1 --para 16 --keys 16
    END=$(date +%s)
    DIFF=$((END - START))
    echo "IVJ_ACB run w1(10,10) w2(10,10) "$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.out' '/home/ziehn-ldap/BaselineExp/result_IVJ/FOut_IVJ_10_10_ACB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.out' '/home/ziehn-ldap/BaselineExp/result_IVJ/FOut_IVJ_10_10_ACB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.log' '/home/ziehn-ldap/BaselineExp/result_IVJ/FLog_IVJ_10_10_ACB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.log' '/home/ziehn-ldap/BaselineExp/result_IVJ/FLog_IVJ_10_10_ACB_'$loop'.txt'
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    #1850 too high
    $flink run -c IVJClusterT4 $jar --output $output_path --tput 1800 --w1lB 10 --w1uB 10 --w2lB 10 --w2uB 10 --run 25 --order BAC --freqA 15 --freqB 15 --freqC 15 --para 16 --keys 16
    END=$(date +%s)
    DIFF=$((END - START))
    echo "IVJ_BAC run w1(10,10) w2(10,10) "$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.out' '/home/ziehn-ldap/BaselineExp/result_IVJ/FOut_IVJ_10_10_BAC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.out' '/home/ziehn-ldap/BaselineExp/result_IVJ/FOut_IVJ_10_10_BAC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.log' '/home/ziehn-ldap/BaselineExp/result_IVJ/FLog_IVJ_10_10_BAC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.log' '/home/ziehn-ldap/BaselineExp/result_IVJ/FLog_IVJ_10_10_BAC_'$loop'.txt'
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    #3150 too high
    $flink run -c IVJClusterT4 $jar --output $output_path --tput 2550 --w1lB 10 --w1uB 10 --w2lB 10 --w2uB 10 --run 25 --order CAB --freqA 15 --freqB 15 --freqC 15 --para 16 --keys 16
    END=$(date +%s)
    DIFF=$((END - START))
    echo "IVJ_CAB run w1(10,10) w2(10,10) "$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.out' '/home/ziehn-ldap/BaselineExp/result_IVJ/FOut_IVJ_10_10_CAB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.out' '/home/ziehn-ldap/BaselineExp/result_IVJ/FOut_IVJ_10_10_CAB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.log' '/home/ziehn-ldap/BaselineExp/result_IVJ/FLog_IVJ_10_10_CAB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.log' '/home/ziehn-ldap/BaselineExp/result_IVJ/FLog_IVJ_10_10_CAB_'$loop'.txt'
# Case lb < ub, window size 30
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    # 1000 okay 47994
    $flink run -c IVJClusterT4 $jar --output $output_path --tput 1500 --w1lB 10 --w1uB 0 --w2lB 10 --w2uB 20 --run 25 --order ABC --freqA 15 --freqB 15 --freqC 15 --para 16 --keys 16
    END=$(date +%s)
    DIFF=$((END - START))
    echo "IVJ_ABC run w1(10,20) w2(10,20) "$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.out' '/home/ziehn-ldap/BaselineExp/result_IVJ/FOut_IVJ_10_20_ABC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.out' '/home/ziehn-ldap/BaselineExp/result_IVJ/FOut_IVJ_10_20_ABC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.log' '/home/ziehn-ldap/BaselineExp/result_IVJ/FLog_IVJ_10_20_ABC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.log' '/home/ziehn-ldap/BaselineExp/result_IVJ/FLog_IVJ_10_20_ABC_'$loop'.txt'
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    $flink run -c IVJClusterT4 $jar --output $output_path --tput 1400 --w1lB 10 --w1uB 0 --w2lB 10 --w2uB 20 --run 25 --order ACB --freqA 15 --freqB 15 --freqC 15 --para 16 --keys 16
    END=$(date +%s)
    DIFF=$((END - START))
    echo "IVJ_ACB run w1(10,20) w2(10,20) "$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.out' '/home/ziehn-ldap/BaselineExp/result_IVJ/FOut_IVJ_10_20_ACB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.out' '/home/ziehn-ldap/BaselineExp/result_IVJ/FOut_IVJ_10_20_ACB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.log' '/home/ziehn-ldap/BaselineExp/result_IVJ/FLog_IVJ_10_20_ACB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.log' '/home/ziehn-ldap/BaselineExp/result_IVJ/FLog_IVJ_10_20_ACB_'$loop'.txt'
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    $flink run -c IVJClusterT4 $jar --output $output_path --tput 1350 --w1lB 10 --w1uB 0 --w2lB 10 --w2uB 20 --run 25 --order BAC --freqA 15 --freqB 15 --freqC 15 --para 16 --keys 16
    END=$(date +%s)
    DIFF=$((END - START))
    echo "IVJ_BAC run w1(10,20) w2(10,20) "$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.out' '/home/ziehn-ldap/BaselineExp/result_IVJ/FOut_IVJ_10_20_BAC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.out' '/home/ziehn-ldap/BaselineExp/result_IVJ/FOut_IVJ_10_20_BAC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.log' '/home/ziehn-ldap/BaselineExp/result_IVJ/FLog_IVJ_10_20_BAC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.log' '/home/ziehn-ldap/BaselineExp/result_IVJ/FLog_IVJ_10_20_BAC_'$loop'.txt'
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    #7,4 is too high
    $flink run -c IVJClusterT4 $jar --output $output_path --tput 6900 --w1lB 10 --w1uB 0 --w2lB 10 --w2uB 20 --run 25 --order CAB --freqA 30 --freqB 15 --para 16 --keys 16
    END=$(date +%s)
    DIFF=$((END - START))
    echo "IVJ_CAB run w1(10,20) w2(10,20) "$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.out' '/home/ziehn-ldap/BaselineExp/result_IVJ/FOut_IVJ_10_20_CAB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.out' '/home/ziehn-ldap/BaselineExp/result_IVJ/FOut_IVJ_10_20_CAB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.log' '/home/ziehn-ldap/BaselineExp/result_IVJ/FLog_IVJ_10_20_CAB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.log' '/home/ziehn-ldap/BaselineExp/result_IVJ/FLog_IVJ_10_20_CAB_'$loop'.txt'
done
echo "Tasks executed"
