package accessdistribution;

import java.math.BigDecimal;
import java.util.*;


/**
 * 针对对象：键值属性（必然是整型）上的等值过滤参数
 * 对于非键值属性（real；decimal；datetime），连续性没有物理意义~
 */
public class SequentialCtnsParaDistribution extends SequentialParaDistribution {

	private long minValue, maxValue;
	private long[] highFrequencyItems = null;

	// 当前时间窗口的候选输入参数集，第一层数组是针对区间的，第二层数组是针对区间内候选参数的
	private long[][] currentParaCandidates = null;

	public SequentialCtnsParaDistribution(long minValue, long maxValue, long[] highFrequencyItems, 
			double[] hFItemFrequencies, long[] intervalCardinalities, double[] intervalFrequencies, 
			double[] intervalParaRepeatRatios) {
		super(hFItemFrequencies, intervalCardinalities, intervalFrequencies, intervalParaRepeatRatios);
		this.minValue = minValue;
		this.maxValue = maxValue;
		this.highFrequencyItems = highFrequencyItems;

	}

	public SequentialCtnsParaDistribution(long minValue, long maxValue, long[] highFrequencyItems,
										  double[] hFItemFrequencies, long[] intervalCardinalities, double[] intervalFrequencies,
										  double[] intervalParaRepeatRatios, ArrayList<ArrayList<Double>> quantilePerInterval) {
		super(hFItemFrequencies, intervalCardinalities, intervalFrequencies, intervalParaRepeatRatios, quantilePerInterval);
		this.minValue = minValue;
		this.maxValue = maxValue;
		this.highFrequencyItems = highFrequencyItems;

	}

	public SequentialCtnsParaDistribution(SequentialCtnsParaDistribution sequentialCtnsParaDistribution){
		super(sequentialCtnsParaDistribution);
		this.minValue = sequentialCtnsParaDistribution.minValue;
		this.maxValue = sequentialCtnsParaDistribution.maxValue;
		this.highFrequencyItems = new long[sequentialCtnsParaDistribution.highFrequencyItems.length];

		for (int i = 0 ;i< highFrequencyItems.length; ++i){
			highFrequencyItems[i] = sequentialCtnsParaDistribution.highFrequencyItems[i];
		}

		geneCandidates(sequentialCtnsParaDistribution.currentParaCandidates);
		init();

	}

	public SequentialCtnsParaDistribution copy(){
		return new SequentialCtnsParaDistribution(this);
	}

	// long[][] priorParaCandidates：前一个时间窗口的候选参数集，这里的priorParaCandidates无需保存
	// 通过priorParaCandidates生成满足要求（intervalParaRepeatRatios & intervalCardinalities）的currentParaCandidates
	// 当intervalParaRepeatRatios & priorParaCandidates为Null时，即生成第一个（初始）时间窗口的currentParaCandidates
	public void geneCandidates(long[][] priorParaCandidates) {
		List<Long> priorParaCandidateList = new ArrayList<>();
		if (priorParaCandidates != null) {
			for (long[] tmpArr : priorParaCandidates) {
				for (long tmpItem : tmpArr) {
					priorParaCandidateList.add(tmpItem);
				}
			}
			Collections.shuffle(priorParaCandidateList);
		}

		currentParaCandidates = new long[intervalNum][];
		int[] repeatedParaNums = new int[intervalNum];
		for (int i = 0; i < intervalNum; i++) {
			// 对于区间内参数基数超过int最大值的情形暂不考虑~
			currentParaCandidates[i] = new long[(int)intervalCardinalities[i]];
			if (intervalParaRepeatRatios == null) {
				repeatedParaNums[i] = 0;
			} else {
				repeatedParaNums[i] = (int)(intervalCardinalities[i] * intervalParaRepeatRatios[i]);
			}
		}

		double avgIntervalLength = (maxValue - minValue) / (double)intervalNum;
		int[] repeatedParaNumsCopy = Arrays.copyOf(repeatedParaNums, repeatedParaNums.length);
		for (long para : priorParaCandidateList) {
			int intervalIndex = (int)((para - minValue) / avgIntervalLength);
			if (intervalIndex >= 0 && intervalIndex < intervalNum && 
					repeatedParaNumsCopy[intervalIndex] > 0) {
				int idx = repeatedParaNums[intervalIndex] - repeatedParaNumsCopy[intervalIndex];
				currentParaCandidates[intervalIndex][idx] = para;
				repeatedParaNumsCopy[intervalIndex]--;
			}
		}

		// for testing：大多数都为0才是正常现象。这里可以有个理论上的证明
		// System.out.println("SequentialCtnsParaDistribution.geneCandidates - repeatedParaNumsCopy: \n\t" + 
		// 		Arrays.toString(repeatedParaNumsCopy));

		Set<Long> priorParameterSet = new HashSet<>();
		priorParameterSet.addAll(priorParaCandidateList);

		// 补齐各个分区剩下的候选参数
		for (int i = 0; i < intervalNum; i++) {
			int idx = repeatedParaNums[i] - repeatedParaNumsCopy[i];
			Set<Long> existedParameterSet = new HashSet<>(); // 当前区间中已存在的候选参数
			for (int j = 0; j < idx; j++) {
				existedParameterSet.add(currentParaCandidates[i][j]);
			}

			while (idx < currentParaCandidates[i].length) {
				long randomParameter = (long)((Math.random() + i) * avgIntervalLength) + minValue;
				int retryCount = 1;
				while (priorParameterSet.contains(randomParameter) || 
						existedParameterSet.contains(randomParameter)) {
					if (retryCount++ > 5) {
						break;
					}
					randomParameter = (long)((Math.random() + i) * avgIntervalLength) + minValue;
				}
				// 这里有个假设：当前时间窗口中的参数基数是远小于参数阈值的，故这样处理引入的误差较小
				currentParaCandidates[i][idx] = randomParameter;
				existedParameterSet.add(randomParameter);
				idx++;
			}
		} // for intervalNum
	}

	// 将本分布与传入的分布按概率p,(1-p)的形式加权合并
	// 在本函数中直接使用的位置，概率信息都是还原后的真实值
	@Override
	public void merge(DataAccessDistribution dataAccessDistribution, double p) throws Exception {
		super.merge(dataAccessDistribution, p);
		if (p > 1 - 1e-7){
			return ;
		}
		if (!(dataAccessDistribution instanceof SequentialCtnsParaDistribution)){
			throw new Exception("It's not same access distribution type");
		}
		SequentialCtnsParaDistribution mergeDistribution = (SequentialCtnsParaDistribution) dataAccessDistribution;
		// 还原概率分布
		List<Map.Entry<Double,Double>> baseQuantile = getQuantile(p);
		baseQuantile.sort(Comparator.comparing(o-> BigDecimal.valueOf(o.getKey())));
		List<Map.Entry<Double,Double>> mergeQuantile = mergeDistribution.getQuantile(1-p);
		mergeQuantile.sort(Comparator.comparing(o->BigDecimal.valueOf(o.getKey())));
		// 按分位点切割全区间
		Set<Double> allQuantilePosAsSet = new HashSet<>();
		for (Map.Entry<Double,Double> quantile: baseQuantile){
			allQuantilePosAsSet.add(quantile.getKey());
		}
		for (Map.Entry<Double,Double> quantile: mergeQuantile){
			allQuantilePosAsSet.add(quantile.getKey());
		}

		List<Double> allQuantilePos = new ArrayList<>();
		for (Number n : allQuantilePosAsSet){
			allQuantilePos.add(n.doubleValue());
		}
		List<Double> allQuantileProb = new ArrayList<>();
		for (int i = 0;i < allQuantilePos.size(); ++i){
			allQuantileProb.add(0.0);
		}


		allQuantilePos.sort(Comparator.comparing(BigDecimal::valueOf));
		if (baseQuantile.size() > 1){
			baseQuantile.get(1).setValue( baseQuantile.get(1).getValue() + baseQuantile.get(0).getValue());
		}
		if (mergeQuantile.size() > 1){
			mergeQuantile.get(1).setValue( mergeQuantile.get(1).getValue() + mergeQuantile.get(0).getValue());
		}

		// 分别将两个分布的概率投射到分位点切割的每个子区间内
		for (int i = 1;i < allQuantilePos.size(); ++i){
			double left = allQuantilePos.get(i - 1);
			double right = allQuantilePos.get(i);
			double prob = 0.0;

			for (int j = 1;j < baseQuantile.size(); ++j){
				double leftEndPoint =  left > baseQuantile.get(j - 1).getKey() ? left : baseQuantile.get(j - 1).getKey();
				double rightEndPoint = right < baseQuantile.get(j).getKey() ? right : baseQuantile.get(j).getKey();

				if (rightEndPoint <= leftEndPoint){ // todo 没有完全消除浮点数精度的问题
					continue;
				}
				prob += baseQuantile.get(j).getValue()*(rightEndPoint - leftEndPoint)/(baseQuantile.get(j).getKey() - baseQuantile.get(j - 1).getKey());
			}

			for (int j = 1;j < mergeQuantile.size(); ++j){
				double leftEndPoint =  left > mergeQuantile.get(j - 1).getKey() ? left : mergeQuantile.get(j - 1).getKey();
				double rightEndPoint = right < mergeQuantile.get(j).getKey() ? right : mergeQuantile.get(j).getKey();

				if (rightEndPoint <= leftEndPoint){
					continue;
				}
				prob += mergeQuantile.get(j).getValue()*(rightEndPoint - leftEndPoint)/(mergeQuantile.get(j).getKey() - mergeQuantile.get(j - 1).getKey());
			}
			allQuantileProb.set(i,allQuantileProb.get(i) + prob);
		}

		this.minValue = Math.min(this.minValue, mergeDistribution.minValue);
		this.maxValue = Math.max(this.maxValue, mergeDistribution.maxValue);

		double avgIntervalLength = 1.0 * (this.maxValue - this.minValue)/ intervalNum;
		// 重构概率分布
		// 记录原始的直方图占有的概率,后续等比例还原
		double intervalProbSum = 0;
		for (int i = 0;i < this.intervalNum; ++i){
			intervalProbSum += this.intervalFrequencies[i];
			this.intervalFrequencies[i] = 0.0;
		}
		double  tmp_value = 0.0;
		for (int i = 1;i < allQuantilePos.size();++i){
			// 当前分位点所在区间
			int idx = (int)((allQuantilePos.get(i) - this.minValue) / avgIntervalLength);
			// 前一个分位点所在区间
			int lastIdx = (int)((allQuantilePos.get(i - 1) - this.minValue) / avgIntervalLength);

			// 当前分位点对应的区间长度
			double length = allQuantilePos.get(i) - allQuantilePos.get(i - 1);

			// 从前一个分位点的位置扫过去
			if (idx >= this.intervalNum){
				idx --;
			}
			tmp_value = 0.;
			if (length < 1e-6){
				lastIdx = idx;
				this.intervalFrequencies[lastIdx] += allQuantileProb.get(i);
			}
			else {
				while (lastIdx <= idx){
					// 当前所在区间的左端点
					double left = (lastIdx * avgIntervalLength + this.minValue);

					double leftEndPoint =  allQuantilePos.get(i - 1) > left ? allQuantilePos.get(i - 1) : left;
					double rightEndPoint = allQuantilePos.get(i) < (left + avgIntervalLength) ? allQuantilePos.get(i) : (left + avgIntervalLength);

					if (leftEndPoint > rightEndPoint){
						lastIdx ++;
						if ( allQuantileProb.get(i) - tmp_value > 1e-6 && lastIdx > idx){
							System.out.println("err");
						}
						continue;
					}
					// 分位点应该分给当前区间的概率，根据两个区间的重合长度加权得到
					double prob = allQuantileProb.get(i)*(rightEndPoint - leftEndPoint)/length;
					this.intervalFrequencies[lastIdx] += prob;
					lastIdx ++;
					tmp_value += prob;
					if ( allQuantileProb.get(i) - tmp_value > 1e-6 && lastIdx > idx){
						System.out.println("err");
					}
				}
			}
		}
		this.intervalFrequencies[this.intervalNum - 1] += 1.0 - tmp_value;


		// 构造候选参数集
		Map<Long,Double> oldCandidatesAsMap = new HashMap<>();
		for (int i = 0; i < mergeDistribution.intervalNum ; ++i){
			double prob = mergeDistribution.intervalFrequencies[i] / mergeDistribution.intervalCardinalities[i] ;
			prob *= (1-p);
			for (int j = 0; j < mergeDistribution.intervalCardinalities[i] ; ++j){
				long val = mergeDistribution.getCurrentParaCandidates()[i][j];
				if (oldCandidatesAsMap.containsKey(val)){
					oldCandidatesAsMap.put(val, oldCandidatesAsMap.get(val) + prob);
				}
				else{
					oldCandidatesAsMap.put(val,prob);
				}
			}
		}
		for (int i = 0; i < this.intervalNum ; ++i){
			double prob = this.intervalFrequencies[i] / this.intervalCardinalities[i] ;
			prob *= p;
			for (int j = 0; j < this.intervalCardinalities[i] ; ++j){
				long val = this.getCurrentParaCandidates()[i][j];
				if (oldCandidatesAsMap.containsKey(val)){
					oldCandidatesAsMap.put(val, oldCandidatesAsMap.get(val) + prob);
				}
				else{
					oldCandidatesAsMap.put(val,prob);
				}
			}
		}

		List<Map.Entry<Long,Double>> oldCandidates = new ArrayList<>(oldCandidatesAsMap.entrySet());
		oldCandidates.sort(Comparator.comparing(o->o.getKey()));
		int idx = 0;
		long[][] newParaCandidates = new long[this.intervalNum][];
		for (int i = 0;i < this.intervalNum ; ++i){
			// 先获取当前区间
			double left = i * avgIntervalLength + this.minValue;
			double right = left + avgIntervalLength;

			double sum = 0;
			List<Long> candidates = new ArrayList<>();
			List<Double> cdf = new ArrayList<>();
			while (oldCandidates.size() > idx && oldCandidates.get(idx).getKey() <= right){
				candidates.add(oldCandidates.get(idx).getKey());
				sum += oldCandidates.get(idx).getValue();
				cdf.add(sum);
				idx ++;
			}

			long cnt = this.intervalCardinalities[i];

			Set<Long> candidatesSet = new HashSet<>(candidates);
			while ((cnt--) > 0){
				double targetIdx = Math.random() * sum;
				for (int j = 0;j < candidates.size() ; ++j){
					if (targetIdx < cdf.get(j)){
						candidatesSet.add(candidates.get(j));
						break;
					}
				}
			}
			int j = 0;
			newParaCandidates[i] = new long[candidatesSet.size()];
			for (Long candidate : candidatesSet){
				newParaCandidates[i][j] = candidate;
				j ++;
			}
		}

		for( int i = 0; i < this.intervalNum ; ++i){
			this.intervalFrequencies[i] *= intervalProbSum;
		}
		init();
		geneCandidates(newParaCandidates);
	}

	// 提取每个分位点，得到分位点的<实际位置,实际概率 * 1/直方图全概率(按直方图部分的概率进行归一化) * 权重>
	private List<Map.Entry<Double,Double>> getQuantile(double p){
		HashMap<Double,Double> quantiles = new HashMap<>();
		double intervalProbSum = 0;
		for( int i = 0; i < this.intervalNum ; ++i){
			intervalProbSum += this.intervalFrequencies[i];
		}
		// 补充左端点
		quantiles.put(1.0 * minValue,0.0);
		double avgIntervalLength = 1.0 * (maxValue - minValue) / intervalNum;
		for (int i = 0; i < intervalNum; i++) {
			// 当前区间的起始偏移量
			double bias = i * avgIntervalLength + minValue;
			// 当前区间每个分位点实际占有的概率
			double prob = this.intervalFrequencies[i] / (this.quantileNum - 1);
			// 如果没有分位点，就补充1分位点，即右端点
			if (quantileNum == -1){
				quantiles.put(bias + avgIntervalLength, this.intervalFrequencies[i] / intervalProbSum * p);
			}
			for (int j = 1; j < this.quantilePerInterval.get(i).size(); j++) {
				// 当前分位点的实际位置
				double pos = bias + avgIntervalLength * this.quantilePerInterval.get(i).get(j);
				// 第一个分位点是左端点，不对应任何概率
				if (quantiles.containsKey(pos)){
					quantiles.put(pos,quantiles.get(pos) + prob / intervalProbSum * p);
				}
				else{
					quantiles.put(pos, prob / intervalProbSum * p);
				}

			}
		}
		return new ArrayList<>(quantiles.entrySet());
	}

	@Override
	public Long geneValue() {
//		System.out.println(this.getClass());
		try {
			int randomIndex = binarySearch();

			if (randomIndex < highFrequencyItemNum) {
				return highFrequencyItems[randomIndex];
			} else {
				int intervalIndex = randomIndex - highFrequencyItemNum;
				// long intervalInnerIndex = intervalInnerIndexes[intervalIndex]++ % intervalCardinalities[intervalIndex];
				int intervalInnerIndex = (int)(Math.random() * intervalCardinalities[intervalIndex]);
				return currentParaCandidates[intervalIndex][intervalInnerIndex];
			}
		}
		catch (Exception e){

			e.printStackTrace();
		}
		return -1L;

	}
	
	public void setCurrentParaCandidates(long[][] currentParaCandidates) {
		this.currentParaCandidates = currentParaCandidates;
	}

	public long[][] getCurrentParaCandidates() {
		return currentParaCandidates;
	}

	@Override
	public String toString() {
		return "SequentialCtnsParaDistribution [minValue=" + minValue + ", maxValue=" + maxValue
				+ ", highFrequencyItems=" + Arrays.toString(highFrequencyItems) + ", size of currentParaCandidates="
				+ currentParaCandidates.length + ", intervalParaRepeatRatios="
				+ Arrays.toString(intervalParaRepeatRatios) + ", time=" + time + ", highFrequencyItemNum="
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
		double[] intervalParaRepeatRatios = null;
		SequentialCtnsParaDistribution distribution1 = new SequentialCtnsParaDistribution(minValue, maxValue, 
				highFrequencyItems, hFItemFrequencies, intervalCardinalities, intervalFrequencies, intervalParaRepeatRatios);
		distribution1.geneCandidates(null);
		
		long minValue2 = 358, maxValue2 = 284156;
		long[] highFrequencyItems2 = {584, 980, 207458, 1520, 7302, 282410, 7302, 38420, 165887, 234};
		// 0.7214
		double[] hFItemFrequencies2 = {0.05, 0.1101, 0.065, 0.127, 0.087, 0.049, 0.1195, 0.023, 0.031, 0.0598};
		long[] intervalCardinalities2 = {152, 94, 87, 102, 65, 28, 305, 385, 65, 35, 120, 68, 158, 52, 67};
		// 0.2786
		double[] intervalFrequencies2 = {0.0175, 0.04024, 0.009808, 0.00874, 0.0245, 0.0257, 0.00754, 0.00695, 
				0.0325, 0.01871, 0.048147, 0.0147, 0.008585, 0.00258, 0.0124};
		double[] intervalParaRepeatRatios2 = {0.27, 0.24, 0.184, 0.274, 0.52, 0.348, 0.048, 0.287, 0.549, 
				0.724, 0.105, 0.121, 0.1874, 0.005, 0.00184};
		SequentialCtnsParaDistribution distribution2 = new SequentialCtnsParaDistribution(minValue2, maxValue2, 
				highFrequencyItems2, hFItemFrequencies2, intervalCardinalities2, intervalFrequencies2, intervalParaRepeatRatios2);
		distribution2.geneCandidates(distribution1.getCurrentParaCandidates());
		for (int i = 0; i < 1000000; i++) {
			System.out.println(distribution2.geneValue());
		}
	}

	@Override
	public boolean inDomain(Object parameter) {
		long para = (Long)parameter;
		if (para < minValue || para > maxValue) {
			return false;
		} else {
			return true;
		}
	}

	// 生成完全随机的（即均匀分布）的参数
	@Override
	public Long geneUniformValue() {
		return (long)(Math.random() * (maxValue - minValue) + minValue);
	}
}
