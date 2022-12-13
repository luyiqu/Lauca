package abstraction;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Partition<T extends Number> {
    public List<String> getPartitionNameList() {
        return partitionNameList;
    }

    public PartitionFunction getPartitionRule() {
        return partitionRule;
    }

    public List<List<T>> getPartitionParam() {
        return partitionParam;
    }

    public String getPartitionKey() {
        return partitionKey;
    }

    // 分区的名字
    private List<String> partitionNameList;
    // 分区规则
    private PartitionFunction partitionRule;
    // 分区规则对应的参数,hash的参数是mod值
    private List<List<T>> partitionParam;
    private String partitionKey;
    // 拷贝构造
    public Partition(Partition<T> partition) {
        partitionNameList = new ArrayList<>(partition.getPartitionNameList());
        partitionRule = partition.getPartitionRule();
        partitionParam = new ArrayList<>();
        for (List<T> params: partition.getPartitionParam()) {
            partitionParam.add(new ArrayList<>(params));
        }
        partitionKey = partition.getPartitionKey();
    }

    public Partition(List<String> partitionNameList, PartitionFunction partitionRule, List<List<T>> partitionParam, String partitionKey) {
        this.partitionNameList = partitionNameList;
        this.partitionRule = partitionRule;
        this.partitionParam = partitionParam;
        this.partitionKey = partitionKey;
    }
    // 返回key属于的分区名
    public String getPartition(T key){
        int length = getLength();
        for (int i = 0; i < length; i++) {
            if (keyInPartition(key, i)){
                return partitionNameList.get(i);
            }
        }
        return null;
    }

    private boolean keyInPartition(T key, int i) {
        List<T> params = partitionParam.get(i);
        switch (partitionRule){
            case HASH:
                return key.intValue() % getLength() == params.get(0).intValue();
            case LIST:
                for (T para:params ) {
                    if (Objects.equals(key, para)){
                        return true;
                    }
                }
                return false;
            case RANGE:
                return key.doubleValue() >= params.get(0).doubleValue() && key.doubleValue() <= params.get(1).doubleValue();
            default:
                return false;
        }
    }

    private int getLength() {
        return partitionNameList.size();
    }

}
