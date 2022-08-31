package accessdistribution;

import java.util.ArrayList;

/**
 * 当前工作中的数据访问分布由高频项和直方图共同表示
 */
public abstract class DataAccessDistribution implements Comparable<DataAccessDistribution> {

	// 该数据分布所属的时间窗口
	protected long time;

	// 高频项的个数
	protected int highFrequencyItemNum;
	// 高频项的频率
	protected double[] hFItemFrequencies = null;

	// 区间数，即参数空间的分段数：区间越多数据访问分布表示越精确，但相应的存储以及计算代价会越高
	protected int intervalNum;
	// 每个区间内参数的基数（出现的非重复值的个数）
	protected long[] intervalCardinalities = null;
	// 每个区间上数据访问的频率（在统计时会刨掉高频项），这里假设区间内每个元素出现的概率是相同的
	protected double[] intervalFrequencies = null;

	// 每个区间内频数统计的分段数，也即分位点数
	protected int quantileNum;
	// 每个区间频数统计的结果，这个二维数组的大小为intervalNum*quantileNum，每个区间的分段数都是一样的，记录了区间内各分位点的归一化位置
	protected ArrayList<ArrayList<Double>> quantilePerInterval = null;

	// 下面两个类成员不是数据分布的信息
	// cumulativeFrequencies：累积频率，用来支持数据的随机生成
	protected double[] cumulativeFrequencies = null;
	// 用来保证模拟负载中的参数基数（访问项非重复值个数）与期望值相符。注意：后面为了数据访问分布对象可以被多线程共享，该成员未被使用
	protected long[] intervalInnerIndexes = null;

	public DataAccessDistribution(double[] hFItemFrequencies, long[] intervalCardinalities,
			double[] intervalFrequencies) {
		super();
		this.hFItemFrequencies = hFItemFrequencies;
		this.intervalCardinalities = intervalCardinalities;
		this.intervalFrequencies = intervalFrequencies;
		init();
	}

	public DataAccessDistribution(double[] hFItemFrequencies, long[] intervalCardinalities,
								  double[] intervalFrequencies,ArrayList<ArrayList<Double>> quantilePerInterval) {
		super();
		this.hFItemFrequencies = hFItemFrequencies;
		this.intervalCardinalities = intervalCardinalities;
		this.intervalFrequencies = intervalFrequencies;
		this.quantilePerInterval = quantilePerInterval;
		init();
	}

	public DataAccessDistribution(DataAccessDistribution dataAccessDistribution){
		super();
		this.time = dataAccessDistribution.time;
		this.highFrequencyItemNum = dataAccessDistribution.highFrequencyItemNum;
		this.intervalNum = dataAccessDistribution.intervalNum;
		this.quantileNum = dataAccessDistribution.quantileNum;

		this.hFItemFrequencies = new double[highFrequencyItemNum];
		for (int i = 0;i < highFrequencyItemNum; ++i){
			this.hFItemFrequencies[i] = dataAccessDistribution.hFItemFrequencies[i];
		}

		this.intervalCardinalities = new long[this.intervalNum];
		this.intervalFrequencies = new double[this.intervalNum];
		for (int  i = 0;i < this.intervalNum; ++i){
			this.intervalFrequencies[i] = dataAccessDistribution.intervalFrequencies[i];
			this.intervalCardinalities[i] = dataAccessDistribution.intervalCardinalities[i];
		}

		this.quantilePerInterval = new ArrayList<>();
		for (ArrayList quantile: dataAccessDistribution.quantilePerInterval){
			this.quantilePerInterval.add(new ArrayList<>(quantile));
		}

		init();
	}

	// 利用构造函数传入的参数信息 初始化 其余类成员
	private void init() {
		highFrequencyItemNum = hFItemFrequencies.length;
		intervalNum = intervalCardinalities.length;

		quantileNum = (this.quantilePerInterval != null && this.quantilePerInterval.size() > 0)?
				(this.quantilePerInterval.get(0).size()):
				-1;

		cumulativeFrequencies = new double[highFrequencyItemNum + intervalNum];
		cumulativeFrequencies[0] = hFItemFrequencies[0]; // 默认是有高频项的
		for (int i = 1; i < cumulativeFrequencies.length; i++) {
			if (i < highFrequencyItemNum) {
				cumulativeFrequencies[i] = cumulativeFrequencies[i - 1] + hFItemFrequencies[i];
			} else {
				cumulativeFrequencies[i] = cumulativeFrequencies[i - 1] + intervalFrequencies[i - highFrequencyItemNum];
			}
		}

		intervalInnerIndexes = new long[intervalNum];
		for (int i = 0; i < intervalNum; i++) {
			intervalInnerIndexes[i] = (long)(Math.random() * intervalCardinalities[i]); // 可保证一定的随机性
		}
	}


	// 生成一个具体参数值（符合指定的数据访问分布：高频项+直方图）
	public abstract Object geneValue();

	// bug fix: 添加一个新的功能，可生成均匀分布的参数，对比实验需求
	// 随机生成一个参数数值，均匀分布
	public abstract Object geneUniformValue();
	
	// 利用二分搜索，基于cumulativeFrequencies随机生成一个参数位置
	protected int binarySearch() {
		double randomValue = Math.random();
		if (randomValue < cumulativeFrequencies[0]) {
			return 0;
		}
		
		// 避免因为double数据类型本身的误差（double是非精确数据类型）导致 return -1
		if (randomValue > 0.99999999) {
			randomValue = randomValue - 0.000000001;
		}
		
		int low = 1, high = cumulativeFrequencies.length - 1;
		while (low <= high) { // 这里其实和 while(true) 没有区别
			int middle = (low + high) / 2;
			if (randomValue < cumulativeFrequencies[middle - 1]) {
				high = middle - 1;
			} else if (randomValue >= cumulativeFrequencies[middle]) {
				low = middle + 1;
			} else {
				return middle;
			}
		}
		return -1; // 理论上不可能返回-1
	}

	public void setTime(long time) {
		this.time = time;
	}
	
	public long getTime() {
		return time;
	}

	@Override
	public int compareTo(DataAccessDistribution o) {
		if (this.time < o.time) {
			return -1;
		} else if (this.time > o.time) {
			return 1;
		} else {
			return 0;
		}
	}
	
	// bug fix：判断利用事务逻辑生成的参数是否在当前属性的阈值内
	public abstract boolean inDomain(Object parameter);

	// 将另一个数据分布的内容按一定的比例合并到当前分布
	public void merge(DataAccessDistribution dataAccessDistribution, double p){

	}

	public abstract DataAccessDistribution copy();

}
