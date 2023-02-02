package accessdistribution;

import abstraction.Partition;

import java.util.List;

public class MultiPartitionDistribution<T extends Number> extends DataAccessDistribution{

    private final Partition<T> partition;

    private final List<DataAccessDistribution> partitionDistribution;

    private int partitionCount = 0;
    private int lastPartition = 0;

    public MultiPartitionDistribution(double[] hFItemFrequencies, long[] intervalCardinalities, double[] intervalFrequencies,
                                      Partition<T> partition, List<DataAccessDistribution> partitionDistribution) {
        super(hFItemFrequencies, intervalCardinalities, intervalFrequencies);
        this.partition = partition;
        this.partitionDistribution = partitionDistribution;
    }

    public MultiPartitionDistribution(MultiPartitionDistribution<T> multiPartitionDistribution) {
        super(multiPartitionDistribution.hFItemFrequencies, multiPartitionDistribution.intervalCardinalities, multiPartitionDistribution.intervalFrequencies);
        this.partition = multiPartitionDistribution.partition;
        this.partitionDistribution = multiPartitionDistribution.partitionDistribution;
    }

    /**
     * 因为hash分布下单个分区的参数分布可能会偏斜导致分区控制不准确，在外面多套一层去除实际上不是同个分区的参数
     * @param idx 需要生成的分区的索引
     * @return
     */
    private Object genePartitionValue(int idx){
        String partitionName = partition.getPartitionNameList().get(idx);
        Object para = partitionDistribution.get(idx).geneValue();

        // 只有一个分布的情况下不用选，因为对应不开启分区统计
        if (partitionDistribution.size() == 1) return para;
        while (!partition.getPartition((T) para).equals(partitionName)){
            para = partitionDistribution.get(idx).geneValue();
        }
        return para;
    }

    @Override
    public Object geneValue() {
        int idx = binarySearch();
        while (partitionDistribution.get(idx) == null) {
            idx = (idx+1) % partitionDistribution.size();
        }
//        if (partitionCount > 1) {
//            lastPartition = (lastPartition + 1) % partitionDistribution.size();
//        }
//
//        if (lastPartition == idx){
//            partitionCount ++;
//        }
//        else{
//            lastPartition = idx;
//            partitionCount = 1;
//        }


        return genePartitionValue(idx);
    }

    @Override
    public Object geneUniformValue() {
        return null;
    }

    @Override
    public Object geneValueInSamePartition(Object parameter){
        String partitionName = (String)getParaPartition(parameter);
        if (partitionName != null){
            int cnt = 0;
            for (String name: partition.getPartitionNameList()) {
                if (partitionName.equals(name)){
                    return genePartitionValue(cnt);
                }
                cnt ++;
            }
        }

        return geneValue();
    }

    @Override
    public boolean inDomain(Object parameter) {
        for (DataAccessDistribution d : partitionDistribution) {
            if (d !=null && d.inDomain(parameter)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Object getParaPartition(Object parameter){
        return partition.getPartition((T) parameter);
    }

    @Override
    public DataAccessDistribution copy() {
        return new MultiPartitionDistribution<>(this);
    }
}
