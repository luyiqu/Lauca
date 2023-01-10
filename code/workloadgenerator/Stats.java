package workloadgenerator;

import java.util.HashMap;
import java.util.Map;

public class Stats {

    private static Map<Integer,Integer> partitionPerTxn = new HashMap<>();

    public Stats(){
    }

    public synchronized static void addPartitionCnt(int cnt){
        if (partitionPerTxn.containsKey(cnt)){
            partitionPerTxn.put(cnt, partitionPerTxn.get(cnt) + 1);
        }else{
            partitionPerTxn.put(cnt,1);
        }
    }

    public static Map<Integer, Integer> getPartitionPerTxn() {
        return partitionPerTxn;
    }
    public static void printPartitionStats(){
        System.out.println("Partition Per Txn");
        for (Integer num: partitionPerTxn.keySet() ) {
            System.out.println("The number of txn overlapped " + num + "partition(s) is " + partitionPerTxn.get(num));
        }
    }
}
