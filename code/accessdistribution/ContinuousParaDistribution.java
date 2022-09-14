package accessdistribution;

import java.math.BigDecimal;
import java.util.*;

/**
 * 参数空间是连续的，此时参数的生成不需要考虑miss的情形（miss是指利用该参数值过滤无返回tuple，即没有记录满足谓词）
 * @param <T extends Number>： Long（Integer & DataTime）、Double（Real）、BigDecimal（Decimal）
 * 三种情况下的参数生成适用于该类型数据分布：
 * 1.主键属性上的等值过滤参数（目前主键仅支持整型，即Long）：主键属性在生成时是按序生成的，所以数值空间一定是连续的，而基于连续数值空
 *   间的参数生成是容易的，不需要考虑过滤参数是否miss的问题；
 * 2.外键属性上的等值过滤参数（外键肯定也是整型）：若外键所在数据表的size大于参照主键所在数据表的size（在测试数据库生成时需保证所有
 *   参照主键都能出现），可在参照主键阈值内随机生成； -- 注意：目前外键属性的生成仅是在参照主键阈值内随机确定的~ TODO （存疑）
 * 3.像DataTime、Real和Decimal型参数一般不会作为等值过滤参数，故无需考虑miss的问题，在数值空间内随机生成即可，同时Integer
 *   类型的非过滤型参数也可在阈值范围内随机生成（但最好还是按原整型属性的生成规则生成参数）。
 */
public class ContinuousParaDistribution <T extends Number> extends DataAccessDistribution {

	// 数值空间的最小值和最大值，即输入参数的阈值（运行日志中统计得到的数值）
	private T minValue = null;
	private T maxValue = null;

	// 具体高频项。因为这里是连续空间的参数，实际数据库中的高频项在模拟库中也必然存在，因此可直接使用日志中获取的高频项
	private T[] highFrequencyItems = null;

	public ContinuousParaDistribution(T minValue, T maxValue, T[] highFrequencyItems, double[] hFItemFrequencies, 
			long[] intervalCardinalities, double[] intervalFrequencies) {
		super(hFItemFrequencies, intervalCardinalities, intervalFrequencies);
		this.minValue = minValue;
		this.maxValue = maxValue;
		this.highFrequencyItems = highFrequencyItems;
	}

	public ContinuousParaDistribution(T minValue, T maxValue, T[] highFrequencyItems, double[] hFItemFrequencies,
									  long[] intervalCardinalities, double[] intervalFrequencies,
									  ArrayList<ArrayList<Double>> quantilePerInterval) {
		super(hFItemFrequencies, intervalCardinalities, intervalFrequencies, quantilePerInterval);
		this.minValue = minValue;
		this.maxValue = maxValue;
		this.highFrequencyItems = highFrequencyItems;
	}

	public ContinuousParaDistribution(ContinuousParaDistribution<T> continuousParaDistribution){
		super(continuousParaDistribution);
		this.minValue = continuousParaDistribution.minValue;
		this.maxValue = continuousParaDistribution.maxValue;
		ArrayList<T> freqItem = new ArrayList<>();// 泛型数组不能直接初始化，需要用arraylist强转一下

		for (int i = 0 ;i< highFrequencyItems.length; ++i){
			freqItem.add(continuousParaDistribution.highFrequencyItems[i]);
		}
		this.highFrequencyItems = (T[]) freqItem.toArray();
	}

	public ContinuousParaDistribution copy(){
		return new ContinuousParaDistribution(this);
	}

	@Override
	public T geneValue() {
//		System.out.println(this.getClass());
//		System.out.println("高频项："+highFrequencyItems[2]);
		int randomIndex = binarySearch();
		if (randomIndex < highFrequencyItemNum) {
//			System.out.println(highFrequencyItems);
			return highFrequencyItems[randomIndex];
		} else {
			return getIntervalInnerRandomValue(randomIndex);
		}
	}

	@SuppressWarnings("unchecked")
	// 获取指定区间中的随机参数值
	private T getIntervalInnerRandomValue(int randomIndex) {
		int intervalIndex = randomIndex - highFrequencyItemNum;
		long intervalCardinality = intervalCardinalities[intervalIndex];

		// 可保证区间内生成参数的基数
		// long intervalInnerIndex = intervalInnerIndexes[intervalIndex]++ % intervalCardinality;
		double intervalInnerIndex = Math.random();

		// 根据频数分位点先做一次映射，从均匀分布映射到基于频数的分段分布上
		if (this.quantileNum > intervalIndex){
			ArrayList<Double> quantile = this.quantilePerInterval.get(intervalIndex);
			for(int i = 1;i <= quantile.size() ; ++i){
				double cdfNow = ((double) i / quantile.size());
				if (intervalInnerIndex < cdfNow
						+ 1e-7){// eps for float compare
					// 概率上小于第i分位点的概率差
					// 需要将该概率差映射到分段分布上，变成距离第i分位点的长度
					double bias = cdfNow - intervalInnerIndex;
					// 第i-1到i分位点在新分布上的区间长度
					double intervalLength = (i == 0)? quantile.get(0) : quantile.get(i) - quantile.get(i-1);
					// 偏差概率bias : 区间总概率(1/quantile.size) = 新区间上的长度biasLength : 区间长度
					double biasLength = bias * quantile.size() * intervalLength;

					// 映射后的位置应该是第i分位点向左偏移biasLength
					intervalInnerIndex = quantile.get(i) - biasLength;
					break;
				}
			}
		}


		double avgIntervalLength = (maxValue.doubleValue() - minValue.doubleValue()) / intervalNum;
		double value = (intervalInnerIndex + intervalIndex) *
				avgIntervalLength + minValue.doubleValue();

		// 将 double value 转化成目标数据类型的参数
		String dataType = maxValue.getClass().getSimpleName();
		if (dataType.equals("Long")) {
			return (T)(new Long((long)value));
		} else if (dataType.equals("Double")) {
			return (T)(new Double(value));
		} else if (dataType.equals("BigDecimal")) {
			return (T)(new BigDecimal(value));
		} else {
			return null; // 理论上不可能进入该分支
		}
	}

	// 将本分布与传入的分布按概率p,(1-p)的形式加权合并
	// 在本函数中直接使用的位置，概率信息都是还原后的真实值
	@Override
	public void merge(DataAccessDistribution dataAccessDistribution, double p) throws Exception {
		super.merge(dataAccessDistribution, p);
		if (!(dataAccessDistribution instanceof ContinuousParaDistribution)){
			throw new Exception("It's not same access distribution type");
		}
		ContinuousParaDistribution mergeDistribution = (ContinuousParaDistribution) dataAccessDistribution;
		// 还原概率分布
		List<Map.Entry<Double,Double>> baseQuantile = getQuantile();
		List<Map.Entry<Double,Double>> mergeQuantile = mergeDistribution.getQuantile();
		// 按分位点切割全区间
		List<Double> allQuantilePos = new ArrayList<>();
		List<Double> allQuantileProb = new ArrayList<>();
		for (Map.Entry<Double, Double> quantile : baseQuantile) {
			allQuantilePos.add(quantile.getKey());
			allQuantileProb.add(0.0);
		}
		for (Map.Entry<Double, Double> quantile : mergeQuantile) {
			allQuantilePos.add(quantile.getKey());
			allQuantileProb.add(0.0);
		}

		allQuantilePos.sort(Comparator.comparing(BigDecimal::valueOf));
		// 分别将两个分布的概率投射到分位点切割的每个子区间内
		for (int i = 1;i < allQuantilePos.size(); ++i){
			for (int j = 1;j < baseQuantile.size(); ++j){
				double leftEndPoint =  allQuantilePos.get(i - 1) > baseQuantile.get(j - 1).getKey() ? allQuantilePos.get(i - 1) : baseQuantile.get(j - 1).getKey();
				double rightEndPoint = allQuantilePos.get(i) < baseQuantile.get(j).getKey() ? allQuantilePos.get(i) : baseQuantile.get(j).getKey();

				if (rightEndPoint < leftEndPoint){
					continue;
				}
				double prob = baseQuantile.get(j).getValue()*(rightEndPoint - leftEndPoint)/(baseQuantile.get(j).getKey() - baseQuantile.get(j - 1).getKey());
				allQuantileProb.set(i,allQuantileProb.get(i) + prob * p);
			}

			for (int j = 1;j < mergeQuantile.size(); ++j){
				double leftEndPoint =  allQuantilePos.get(i - 1) > mergeQuantile.get(j - 1).getKey() ? allQuantilePos.get(i - 1) : mergeQuantile.get(j - 1).getKey();
				double rightEndPoint = allQuantilePos.get(i) < mergeQuantile.get(j).getKey() ? allQuantilePos.get(i) : mergeQuantile.get(j).getKey();

				if (rightEndPoint < leftEndPoint){
					continue;
				}
				double prob = mergeQuantile.get(j).getValue()*(rightEndPoint - leftEndPoint)/(mergeQuantile.get(j).getKey() - mergeQuantile.get(j - 1).getKey());
				allQuantileProb.set(i,allQuantileProb.get(i) + prob * (1 - p));
			}
		}

		this.minValue = this.minValue.doubleValue() < mergeDistribution.minValue.doubleValue() ?
				this.minValue : (T) mergeDistribution.minValue;
		this.maxValue = this.maxValue.doubleValue() < mergeDistribution.maxValue.doubleValue() ?
				this.maxValue : (T) mergeDistribution.maxValue;

		double avgIntervalLength = (this.maxValue.doubleValue() - this.minValue.doubleValue())/ intervalNum;
		// 重构概率分布
		for (int i = 1;i < allQuantilePos.size();++i){
			// 当前分位点所在区间
			int idx = (int)((allQuantilePos.get(i) - this.minValue.doubleValue()) / avgIntervalLength);
			// 前一个分位点所在区间
			int lastIdx = (int)((allQuantilePos.get(i - 1) - this.minValue.doubleValue()) / avgIntervalLength);

			// 当前分位点对应的区间长度
			double length = allQuantilePos.get(i) - allQuantilePos.get(i - 1);

			// 从前一个分位点的位置扫过去
			while (lastIdx <= idx){
				// 当前所在区间的左端点
				double left = (lastIdx * avgIntervalLength + this.minValue.doubleValue());

				double leftEndPoint =  allQuantilePos.get(i - 1) > left ? allQuantilePos.get(i - 1) : left;
				double rightEndPoint = allQuantilePos.get(i) < (left + avgIntervalLength) ? allQuantilePos.get(i) : (left + avgIntervalLength);

				// 分位点应该分给当前区间的概率，根据两个区间的重合长度加权得到
				double prob = allQuantileProb.get(i)*(rightEndPoint - leftEndPoint)/length;
				this.intervalFrequencies[lastIdx] += prob;
				lastIdx ++;
			}
		}

		// 重构分位点
		this.quantilePerInterval = new ArrayList<>();
		for (int i = 0;i < this.intervalNum ; ++i){
			ArrayList<Double> quantiles = new ArrayList<>();
			quantiles.add(0.0);
			// 当前区间每个分位点实际占有的概率
			double prob = this.intervalFrequencies[i] / this.quantileNum;
			// 先获取当前区间
			Map<Double,Double> quantilesUsedNowAsMap = new HashMap<>();
			double left = i * avgIntervalLength + this.minValue.doubleValue();


			for (int j = 1;j < allQuantilePos.size(); ++j){
				double length = allQuantilePos.get(j) - allQuantilePos.get(j - 1);
				double leftEndPoint =  allQuantilePos.get(j - 1) > left ? allQuantilePos.get(j - 1) : left;
				double rightEndPoint = allQuantilePos.get(j) < (left + avgIntervalLength) ? allQuantilePos.get(j) : (left + avgIntervalLength);
				if (leftEndPoint < rightEndPoint){
					quantilesUsedNowAsMap.put(rightEndPoint, allQuantileProb.get(j) * (rightEndPoint - leftEndPoint) / length);
				}
			}
			if (!quantilesUsedNowAsMap.containsKey(left)){
				quantilesUsedNowAsMap.put(left,0.0);
			}
			ArrayList<Map.Entry<Double,Double>> quantilesUsedNow = new ArrayList<>(quantilesUsedNowAsMap.entrySet());
			quantilesUsedNow.sort(Comparator.comparing(o->BigDecimal.valueOf(o.getKey())));

			double sum = 0;
			double base = quantilesUsedNow.get(0).getKey();
			for (Map.Entry<Double,Double> quantile: quantilesUsedNow){
				while (sum + quantile.getValue() > prob + 1e-7){
					double delta = prob - sum;
					double length = quantile.getKey() - base;
					double pos = base + length * delta / quantile.getValue();
					quantiles.add((pos - left)/avgIntervalLength); // 保存归一化的分位点位置

					base = pos;
					quantile.setValue(quantile.getValue() - delta);
					sum = 0;
				}

				base = quantile.getKey();
				sum = quantile.getValue();
			}
		}
	}

	public List<Map.Entry<Double,Double>> getQuantile(){
		HashMap<Double,Double> quantiles = new HashMap<>();
		double avgIntervalLength = (maxValue.doubleValue() - minValue.doubleValue()) / intervalNum;
		for (int i = 0; i < intervalNum; i++) {
			// 当前区间的起始偏移量
			double bias = i * avgIntervalLength + minValue.doubleValue();
			// 当前区间每个分位点实际占有的概率
			double prob = this.intervalFrequencies[i] / this.quantileNum;
			for (int j = 0; j < quantileNum; j++) {
				// 当前分位点的实际位置
				double pos = bias + avgIntervalLength * this.quantilePerInterval.get(i).get(j);
				// 第一个分位点是左端点，不对应任何概率
				if (j == 0){
					quantiles.put(pos,0.0);
				}
				else {
					quantiles.put(pos,prob);
				}

			}
		}
		return new ArrayList<>(quantiles.entrySet());
	}

	public T getMinValue() {
		return minValue;
	}

	

	@Override
	public String toString() {
		return "ContinuousParaDistribution [minValue=" + minValue + ", maxValue=" + maxValue + ", highFrequencyItems="
				+ Arrays.toString(highFrequencyItems) + ", time=" + time + ", highFrequencyItemNum="
				+ highFrequencyItemNum + ", hFItemFrequencies=" + Arrays.toString(hFItemFrequencies) + ", intervalNum="
				+ intervalNum + ", intervalCardinalities=" + Arrays.toString(intervalCardinalities)
				+ ", intervalFrequencies=" + Arrays.toString(intervalFrequencies) + ", cumulativeFrequencies="
				+ Arrays.toString(cumulativeFrequencies) + ", intervalInnerIndexes="
				+ Arrays.toString(intervalInnerIndexes) + "]";
	}

	// for testing
	public static void main(String[] args) {
		long minValue = 12, maxValue = 329962;
		long[] highFrequencyItems = {234, 980, 62000, 41900, 7302, 220931, 120002, 218400, 38420, 1520};
		// 0.7214
		double[] hFItemFrequencies = {0.05, 0.1101, 0.065, 0.127, 0.087, 0.049, 0.1195, 0.023, 0.031, 0.0598};
		long[] intervalCardinalities = {52, 34, 123, 78, 45, 32, 901, 234, 41, 15, 34, 90, 210, 40, 98};
		// 0.2786
		double[] intervalFrequencies = {0.0175, 0.04024, 0.009808, 0.00874, 0.0245, 0.0257, 0.00754, 0.00695, 
				0.0325, 0.01871, 0.048147, 0.0147, 0.008585, 0.00258, 0.0124};

		Long[] highFrequencyItems2 = new Long[highFrequencyItems.length];
		for (int i = 0; i < highFrequencyItems.length; i++) {
			highFrequencyItems2[i] = highFrequencyItems[i];
		}

		ContinuousParaDistribution<Long> distribution = new ContinuousParaDistribution<Long>(minValue, maxValue, 
				highFrequencyItems2, hFItemFrequencies, intervalCardinalities, intervalFrequencies);
		for (int i = 0; i < 1000000; i++) {
			System.out.println(distribution.geneValue());
		}
	}

	// 判断依据事务逻辑生成的参数是否越界
	@Override
	public boolean inDomain(Object parameter) {
		String dataType = maxValue.getClass().getSimpleName();
		double para = 0;
		if (dataType.equals("Long")) {
			para = (Long)parameter;
		} else if (dataType.equals("Double")) {
			para = (Double)parameter;
		} else if (dataType.equals("BigDecimal")) {
			para = new BigDecimal(parameter.toString()).doubleValue();
		}
		if (para < minValue.doubleValue() || para > maxValue.doubleValue()) {
			return false;
		} else {
			return true;
		}
	}

	// 为了做实验后续添加的，生成完全随机的（即均匀分布）的参数
	@SuppressWarnings("unchecked")
	@Override
	public T geneUniformValue() {
		double value = Math.random() * (maxValue.doubleValue() - minValue.doubleValue()) 
				+ minValue.doubleValue();
		
		// 下面这段代码 copy from 函数 "getIntervalInnerRandomValue"
		// 将 double value 转化成目标数据类型的参数
		String dataType = maxValue.getClass().getSimpleName();
		if (dataType.equals("Long")) {
			return (T)(new Long((long)value));
		} else if (dataType.equals("Double")) {
			return (T)(new Double(value));
		} else if (dataType.equals("BigDecimal")) {
			return (T)(new BigDecimal(value));
		} else {
			return null; // 理论上不可能进入该分支
		}
	}
}
