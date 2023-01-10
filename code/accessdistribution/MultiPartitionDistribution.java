package accessdistribution;

import abstraction.Partition;

import java.util.ArrayList;
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


        return partitionDistribution.get(idx).geneValue();
    }

    @Override
    public Object geneUniformValue() {
        return null;
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
