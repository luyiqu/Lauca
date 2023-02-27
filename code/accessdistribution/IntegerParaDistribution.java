package accessdistribution;

import java.util.*;

import abstraction.Column;
import config.Configurations;

/**
 * 整数类型的等值过滤参数，并且参数空间（或可指相应属性的数值空间）不是连续的，在参数生成时需要考虑miss情形
 * 两种情况下的参数生成适用于该类型数据分布：
 * 1.非键值属性上的整型等值过滤参数：参数的生成需依据相应整型属性的生成规则，以确保生成的参数在模拟数据库中存在。主要思路是根据
 *   基数索引生成过滤参数（这里要求属性在生成时需保证所有基数索引都出现）；
 *   -- 注意：目前属性在生成时基数索引是随机确定的，未保证所有基数索引都能出现~ TODO
 * 2.外键属性上的过滤参数：当外键所在数据表的size小于参照主键所在数据表的size，此时必然有些参照主键值不会在外键表中出现。我
 *   们在数据生成时可按一个确定的规则生成外键，因此相应外键上的过滤参数也需按此规则生成，以避免miss情况。
 * 当前工作的实现暂不考虑情况2，只处理情况1~ TODO
 * 
 * 这里有个假设：高频项的个数远远小于参数基数，因此在直方图概率的维护上无需考虑对高频项的影响（对于其他访问分布也有此假设）
 */
public class IntegerParaDistribution extends DataAccessDistribution {

	// 当前时间窗口的最小值和最大值
	private long windowMinValue, windowMaxValue;

	// 相应整型属性的具体信息
	private long columnMinValue, columnMaxValue;
	private long columnCardinality;
	private double coefficient;

	// 基数索引的最小值和最大值。因为测试数据库在整型属性的生成时一般不是连续的，是根据基数索引生成的，为了
	// 保证整型属性上生成的过滤参数不会出现miss情况，必须也按基数索引生成。
	private long minParaIndex, maxParaIndex;
	// 日志中统计的高频项参数可能在测试数据库中是不存在的，所以必须自己生成相应数量的高频项
	private long[] highFrequencyItems = null;



	public IntegerParaDistribution(long windowMinValue, long windowMaxValue, double[] hFItemFrequencies, 
			long[] intervalCardinalities, double[] intervalFrequencies) {
		super(hFItemFrequencies, intervalCardinalities, intervalFrequencies);
		this.windowMinValue = windowMinValue;
		this.windowMaxValue = windowMaxValue;
	}

	public IntegerParaDistribution(long windowMinValue, long windowMaxValue, double[] hFItemFrequencies,
								   long[] intervalCardinalities, double[] intervalFrequencies, ArrayList<ArrayList<Double>> quantilePerInterval) {
		super(hFItemFrequencies, intervalCardinalities, intervalFrequencies, quantilePerInterval);
		this.windowMinValue = windowMinValue;
		this.windowMaxValue = windowMaxValue;
	}

	public IntegerParaDistribution(IntegerParaDistribution integerParaDistribution){
		super(integerParaDistribution);
		this.windowMaxValue = integerParaDistribution.windowMaxValue;
		this.windowMinValue = integerParaDistribution.windowMinValue;
		this.highFrequencyItems = new long[integerParaDistribution.highFrequencyItems.length];

		for (int i = 0 ;i< highFrequencyItems.length; ++i){
			highFrequencyItems[i] = integerParaDistribution.highFrequencyItems[i];
		}

		setColumnInfo(integerParaDistribution.columnMinValue, integerParaDistribution.columnMaxValue,
				integerParaDistribution.columnCardinality, integerParaDistribution.coefficient);
	}

	public IntegerParaDistribution copy(){
		return new IntegerParaDistribution(this);
	}

	public void setColumnInfo(long columnMinValue, long columnMaxValue, 
			long columnCardinality, double coefficient) {
		this.columnMinValue = columnMinValue;
		this.columnMaxValue = columnMaxValue;
		this.columnCardinality = columnCardinality;
		this.coefficient = coefficient;
	}

	public void init4IntegerParaGene() {
		// 根据当前时间窗口的阈值生成相应的基数索引阈值
		minParaIndex = (long)((windowMinValue - columnMinValue) / coefficient);
		maxParaIndex = (long)((windowMaxValue - columnMinValue) / coefficient);
		// 正常情况下下面两个判断不可能为真
		if (minParaIndex < 0) {
			minParaIndex = 0;
		}
		if (maxParaIndex > columnCardinality - 1) {
			maxParaIndex = columnCardinality - 1;
		}
		
		
		// 针对"column = column + para"这种形式的谓词，当前这种处理方式不仅不合理，而且存在bug
		// 此时para的值往往较小，导致maxParaIndex和minParaIndex相距很近，甚至可能就是一样的值，
		//   这样便会导致后面的高频项以及各个区间的参数生成出现不合理的情况。
		// 本质上，此时para的生成应该是系数（coefficient）的倍数才对...
		// 注意：para的数值大小或者形式... 一般对评测性能没有什么影响
		// 下面仅为权宜之计
		if (maxParaIndex - minParaIndex < highFrequencyItemNum - 1) {
			minParaIndex = 0;
			maxParaIndex = highFrequencyItemNum - 1;
		} // 一定程度上规避了一些问题，以及在一定程度上保证了程序可运行...
		
		
		// 自己生成当前时间窗口的高频项
		highFrequencyItems = new long[highFrequencyItemNum];
		long windowIndexSize = maxParaIndex - minParaIndex + 1;
		
		// 有可能highFrequencyItemNum大于或者接近windowIndexSize
		if (windowIndexSize > highFrequencyItemNum * 2) {
			Set<Long> hFItemIndexSet = new HashSet<>();
			while (hFItemIndexSet.size() < highFrequencyItemNum) {
				long randomParaIndex = (long)(Math.random() * windowIndexSize) + minParaIndex;
				if (!hFItemIndexSet.contains(randomParaIndex)) {
					highFrequencyItems[hFItemIndexSet.size()] = (long)(randomParaIndex * coefficient + columnMinValue);
					hFItemIndexSet.add(randomParaIndex);
				}
			}
		} else {
			for (int i = 0; i < highFrequencyItemNum && i < windowIndexSize; i++) {
				highFrequencyItems[i] = (long)((i + minParaIndex) * coefficient + columnMinValue);
			}
		}
	}

	@Override
	public Long geneValue() {
//		System.out.println(this.getClass());
		int randomIndex = binarySearch();
		if (randomIndex < highFrequencyItemNum) {
			return highFrequencyItems[randomIndex];
		} else {
			return getIntervalInnerRandomValue(randomIndex);
		}
	}

	private long getIntervalInnerRandomValue(int randomIndex) {
		int intervalIndex = randomIndex - highFrequencyItemNum;
		long intervalCardinality = intervalCardinalities[intervalIndex];
		// 可保证区间内生成参数的基数
		// long intervalInnerIndex = intervalInnerIndexes[intervalIndex]++ % intervalCardinality;
		long intervalInnerIndex = (long)(Math.random() * intervalCardinality);
		
		double avgIntervalIndexSize = (maxParaIndex - minParaIndex) / (double)intervalNum;
		long randomParaIndex = (long)(((double)intervalInnerIndex / intervalCardinality + intervalIndex) * 
				avgIntervalIndexSize) + minParaIndex;
		return (long)(randomParaIndex * coefficient + columnMinValue);
	}

	@Override
	public String toString() {
		return "IntegerParaDistribution [windowMinValue=" + windowMinValue + ", windowMaxValue=" + windowMaxValue
				+ ", columnMinValue=" + columnMinValue + ", columnMaxValue=" + columnMaxValue + ", columnCardinality="
				+ columnCardinality + ", coefficient=" + coefficient + ", minParaIndex=" + minParaIndex
				+ ", maxParaIndex=" + maxParaIndex + ", highFrequencyItems=" + Arrays.toString(highFrequencyItems)
				+ ", time=" + time + ", highFrequencyItemNum=" + highFrequencyItemNum + ", hFItemFrequencies="
				+ Arrays.toString(hFItemFrequencies) + ", intervalNum=" + intervalNum + ", intervalCardinalities="
				+ Arrays.toString(intervalCardinalities) + ", intervalFrequencies="
				+ Arrays.toString(intervalFrequencies) + ", cumulativeFrequencies="
				+ Arrays.toString(cumulativeFrequencies) + ", intervalInnerIndexes="
				+ Arrays.toString(intervalInnerIndexes) + "]";
	}

	// for testing
	public static void main(String[] args) {
		long windowMinValue = 12, windowMaxValue = 329962;
		// 0.7214
		double[] hFItemFrequencies = {0.05, 0.1101, 0.065, 0.127, 0.087, 0.049, 0.1195, 0.023, 0.031, 0.0598};
		long[] intervalCardinalities = {52, 34, 123, 78, 45, 32, 901, 234, 41, 15, 34, 90, 210, 40, 98};
		// 0.2786
		double[] intervalFrequencies = {0.0175, 0.04024, 0.009808, 0.00874, 0.0245, 0.0257, 0.00754, 0.00695, 
				0.0325, 0.01871, 0.048147, 0.0147, 0.008585, 0.00258, 0.0124};

		IntegerParaDistribution distribution = new IntegerParaDistribution(windowMinValue, windowMaxValue, 
				hFItemFrequencies, intervalCardinalities, intervalFrequencies);
		distribution.setColumnInfo(-100000, 500000, 10000, 60);
		distribution.init4IntegerParaGene();
		
		for (int i = 0; i < 1000000; i++) {
			System.out.println(distribution.geneValue());
		}
	}

	// 用于判断生成的参数是否超过属性的阈值
	@Override
	public boolean inDomain(Object parameter) {
		long para = (Long)parameter;
		// 参数的值既不在属性阈值内，也不再当前时间窗口的参数阈值内
		if ((para < columnMinValue || para > columnMaxValue) && 
				(para < windowMinValue || para > windowMaxValue)) {
			return false;
		} else {
			return true;
		}
	}

	// 为了做实验后续添加的，生成完全随机的（即均匀分布）的参数
	@Override
	public Long geneUniformValue() {
		return (long)(Math.random() * (windowMaxValue - windowMinValue) + windowMinValue);
	}
}
