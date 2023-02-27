package workloadgenerator;

import java.util.HashMap;
import java.util.Map;

public class Stats {

    private static Map<Integer,Integer> partitionPerTxn = new HashMap<>();
    private static Map<Integer,Integer> partitionPerSQL = new HashMap<>();

    public Stats(){
    }

    public synchronized static void addTxPartitionCnt(int cnt){
        partitionPerTxn.put(cnt, partitionPerTxn.getOrDefault(cnt,0) + 1);
    }

    public synchronized static void addSQLPartitionCnt(int cnt){
        partitionPerSQL.put(cnt, partitionPerSQL.getOrDefault(cnt,0) + 1);
    }

    public static Map<Integer, Integer> getPartitionPerTxn() {
        return partitionPerTxn;
    }
    public static void printTxnPartitionStats(){
        System.out.println("Partition Per Txn");
        for (Integer num: partitionPerTxn.keySet() ) {
            System.out.println("The number of txn overlapped " + num + " partition(s) is " + partitionPerTxn.get(num));
        }
    }

    public static void printSQLPartitionStats(){
        System.out.println("Partition Per SQL");
        for (Integer num: partitionPerSQL.keySet() ) {
            System.out.println("The number of SQL overlapped " + num + " partition(s) is " + partitionPerSQL.get(num));
        }
    }
}
