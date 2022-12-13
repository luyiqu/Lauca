package accessdistribution;

import java.util.*;


/**
 * 针对对象：键值属性（必然是整型）上的等值过滤参数
 * 对于非键值属性（real；decimal；datetime），连续性没有物理意义~
 */
public class SequentialCtnsParaDistribution extends SequentialParaDistribution {

	private long minValue, maxValue;
	private long[] highFrequencyItems = new long[0];

	// 当前时间窗口的候选输入参数集，第一层数组是针对区间的，第二层数组是针对区间内候选参数的
	private long[][] currentParaCandidates = new long[0][];

	private int[] innerIndex = null;


	public SequentialCtnsParaDistribution(long minValue, long maxValue, long[] highFrequencyItems, 
			double[] hFItemFrequencies, long[] intervalCardinalities, double[] intervalFrequencies,
										  double[][] intervalParaRepeatRatios) {
		super(hFItemFrequencies, intervalCardinalities, intervalFrequencies, intervalParaRepeatRatios);
		this.minValue = minValue;
		this.maxValue = maxValue;
		this.highFrequencyItems = highFrequencyItems;

	}
	public SequentialCtnsParaDistribution(long minValue, long maxValue, long[] highFrequencyItems,
										  double[] hFItemFrequencies, long[] intervalCardinalities, double[] intervalFrequencies,
										  double[] intervalParaRepeatRatios) {
		this(minValue, maxValue, highFrequencyItems, hFItemFrequencies, intervalCardinalities,
				intervalFrequencies, new double[1][0]);
		this.intervalParaRepeatRatios[0] = new double[intervalParaRepeatRatios.length];
		System.arraycopy(intervalParaRepeatRatios, 0, this.intervalParaRepeatRatios[0], 0, intervalParaRepeatRatios.length);
	}



	public SequentialCtnsParaDistribution(long minValue, long maxValue, long[] highFrequencyItems,
										  double[] hFItemFrequencies, long[] intervalCardinalities, double[] intervalFrequencies,
										  double[][] intervalParaRepeatRatios, ArrayList<ArrayList<Double>> quantilePerInterval) {
		super(hFItemFrequencies, intervalCardinalities, intervalFrequencies, intervalParaRepeatRatios, quantilePerInterval);
		this.minValue = minValue;
		this.maxValue = maxValue;
		this.highFrequencyItems = highFrequencyItems;
	}

	public SequentialCtnsParaDistribution(long minValue, long maxValue, long[] highFrequencyItems,
										  double[] hFItemFrequencies, long[] intervalCardinalities, double[] intervalFrequencies,
										  double[] intervalParaRepeatRatios, ArrayList<ArrayList<Double>> quantilePerInterval) {
		this(minValue, maxValue, highFrequencyItems, hFItemFrequencies, intervalCardinalities,
				intervalFrequencies, new double[1][0], quantilePerInterval);
		this.intervalParaRepeatRatios[0] = new double[intervalParaRepeatRatios.length];
		System.arraycopy(intervalParaRepeatRatios, 0, this.intervalParaRepeatRatios[0], 0, intervalParaRepeatRatios.length);
	}

	public SequentialCtnsParaDistribution(SequentialCtnsParaDistribution sequentialCtnsParaDistribution){
		super(sequentialCtnsParaDistribution);
		this.minValue = sequentialCtnsParaDistribution.minValue;
		this.maxValue = sequentialCtnsParaDistribution.maxValue;
		this.highFrequencyItems = new long[sequentialCtnsParaDistribution.highFrequencyItems.length];

		System.arraycopy(sequentialCtnsParaDistribution.highFrequencyItems, 0, highFrequencyItems, 0, highFrequencyItems.length);
		init();
		geneCandidates(sequentialCtnsParaDistribution.currentParaCandidates);


	}

	public SequentialCtnsParaDistribution copy(){
		return new SequentialCtnsParaDistribution(this);
	}

	/**
	 * 按照当前分布的范围，把前一个区间的候选参数加进统计里
	 * 这个过程是去重的
	 * @param priorParaCandidates 前一个区间的候选参数
	 * @param base 已经存储好的候选参数
	 * @return 合并后的结果
	 */
	public ArrayList<ArrayList<Long>> mergeCandidate(long[][] priorParaCandidates,ArrayList<ArrayList<Long>> base, int k ){
		if (k >= intervalParaRepeatRatios.length) return base;
		List<Long> priorParaCandidateList = new ArrayList<>();
		if (priorParaCandidates != null) {
			for (long[] tmpArr : priorParaCandidates) {
				for (long tmpItem : tmpArr) {
					priorParaCandidateList.add(tmpItem);
				}
			}
			Collections.shuffle(priorParaCandidateList);
		}

		int[] repeatedParaNums = new int[intervalNum];
		for (int i = 0; i < intervalNum; i++) {
			// 对于区间内参数基数超过int最大值的情形暂不考虑~
			if (intervalParaRepeatRatios == null) {
				repeatedParaNums[i] = 0;
			} else {
				try {
					repeatedParaNums[i] = (int)(intervalCardinalities[i] * intervalParaRepeatRatios[k][i]);
				}
				catch (Exception e){
					e.printStackTrace();
				}
			}
		}

		double avgIntervalLength = (maxValue - minValue) / (double)intervalNum;
		int[] repeatedParaNumsCopy = Arrays.copyOf(repeatedParaNums, repeatedParaNums.length);
		for (long para : priorParaCandidateList) {
			int intervalIndex = (int)((para - minValue) / avgIntervalLength);
			if (intervalIndex >= 0 && intervalIndex < intervalNum &&
					repeatedParaNumsCopy[intervalIndex] > 0 &&
					!base.get(intervalIndex).contains(para)) { // 去重
				base.get(intervalIndex).add(para);
				repeatedParaNumsCopy[intervalIndex] --;
			}
		}

//		for (ArrayList<Long> baseInterval:base ) {
//			Collections.shuffle(baseInterval);
//		}

		return base;
	}

	// long[][] priorParaCandidates：前一个时间窗口的候选参数集，这里的priorParaCandidates无需保存
	// 通过priorParaCandidates生成满足要求（intervalParaRepeatRatios & intervalCardinalities）的currentParaCandidates
	// 当intervalParaRepeatRatios & priorParaCandidates为Null时，即生成第一个（初始）时间窗口的currentParaCandidates
	public void geneCandidates(long[][] priorParaCandidates) {
		ArrayList<Integer> candidateSize = new ArrayList<>();
		ArrayList<Integer> allSize = new ArrayList<>();

		currentParaCandidates = new long[intervalNum][];
		for (int i = 0; i < intervalNum; i++) {
			// 对于区间内参数基数超过int最大值的情形暂不考虑~
			currentParaCandidates[i] = new long[(int)intervalCardinalities[i]];

			List<Long> existedParameterList = new ArrayList<>(); // 当前区间中已存在的候选参数
			int idx = 0;
			for (int j = 0; j < currentParaCandidates[i].length && (priorParaCandidates != null &&
					priorParaCandidates[i] != null && j < priorParaCandidates[i].length); j++) {
				existedParameterList.add(currentParaCandidates[i][j]);
				idx ++;
			}
			candidateSize.add(idx);
			allSize.add(currentParaCandidates[i].length);
			// 补齐各个分区剩下的候选参数
			while (idx < currentParaCandidates[i].length) {
				long randomParameter = (long)getIntervalInnerRandomValue(i)  ;//((Math.random() + i) * avgIntervalLength) + minValue;//
				int retryCount = 1;
				while (existedParameterList.contains(randomParameter)) {
					if (retryCount++ > 5) {
						break;
					}
					randomParameter = (long)getIntervalInnerRandomValue(i) ;//((Math.random() + i) * avgIntervalLength) + minValue;//
				}
				// 这里有个假设：当前时间窗口中的参数基数是远小于参数阈值的，故这样处理引入的误差较小
				existedParameterList.add(randomParameter);
				idx++;
			}

			Collections.shuffle(existedParameterList);
			for (int j = 0; j < existedParameterList.size(); j++) {
				currentParaCandidates[i][j] = existedParameterList.get(j);
			}
		}

//		System.out.println(candidateSize.toString());
//		System.out.println(allSize.toString());
//		System.out.println();
	}

	@Override
	public double getSimilarity(DataAccessDistribution dataAccessDistribution){

		SequentialCtnsParaDistribution mergeDistribution = (SequentialCtnsParaDistribution) dataAccessDistribution;
		// 还原概率分布
		List<Map.Entry<Double,Double>> baseQuantile = getQuantile(1.0);
		List<Map.Entry<Double,Double>> mergeQuantile = mergeDistribution.getQuantile(1.0);

		baseQuantile.sort(Map.Entry.comparingByKey());
		mergeQuantile.sort(Map.Entry.comparingByKey());
		// 按分位点切割全区间
		Set<Double> allQuantilePosAsSet = new HashSet<>();
		for (Map.Entry<Double,Double> quantile: baseQuantile){
			if (quantile.getValue().isNaN() ){
				continue;
			}
			allQuantilePosAsSet.add(quantile.getKey());
		}
		for (Map.Entry<Double,Double> quantile: mergeQuantile){
			if (quantile.getValue().isNaN() ){
				continue;
			}
			allQuantilePosAsSet.add(quantile.getKey());
		}

		List<Double> allQuantilePos = new ArrayList<>(allQuantilePosAsSet);


		Collections.sort(allQuantilePos);

		double sim = 0.0;

		// 分别将两个分布的概率投射到分位点切割的每个子区间内
		for (int i = 1;i < allQuantilePos.size(); ++i){
			double left = allQuantilePos.get(i - 1);
			double right = allQuantilePos.get(i);

			double prob1 = getOverlapProb(baseQuantile, left, right);
			double prob2 = getOverlapProb(mergeQuantile,left, right);

			sim += Math.min(prob1, prob2);
		}

		return sim;
	}

	// 提取每个分位点，得到分位点的<实际位置,实际概率 * 1/直方图全概率(按直方图部分的概率进行归一化) * 权重>
	private List<Map.Entry<Double,Double>> getQuantile(double p){
		HashMap<Double,Double> quantiles = new HashMap<>();
		double intervalProbSum = 0;
		for( int i = 0; i < this.intervalNum ; ++i){
			intervalProbSum += this.intervalFrequencies[i];
		}
		if (intervalProbSum < 1e-7) intervalProbSum = 1;
		// 补充左端点
		quantiles.put(1.0 * minValue,0.0);
		double avgIntervalLength = 1.0 * (maxValue - minValue) / intervalNum;
		for (int i = 0; i < intervalNum; i++) {
			// 当前区间的起始偏移量
			double bias = i * avgIntervalLength + minValue;
			// 如果没有分位点，就补充1分位点，即右端点
			if (quantileNum < 1 ){
				quantiles.put(bias + avgIntervalLength, this.intervalFrequencies[i] / intervalProbSum * p);
				continue;
			}
			// 当前区间每个分位点实际占有的概率
			double prob = this.intervalFrequencies[i] / (this.quantileNum);

			for (int j = 1; j <= quantileNum; j++) {
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
		if (innerIndex == null) innerIndex = new int[this.intervalNum];
		try {
			int randomIndex = binarySearch();

			if (randomIndex < highFrequencyItemNum) {
				return highFrequencyItems[randomIndex];
			} else {
				int intervalIndex = randomIndex - highFrequencyItemNum;
				// long intervalInnerIndex = intervalInnerIndexes[intervalIndex]++ % intervalCardinalities[intervalIndex];
				int intervalInnerIndex = innerIndex[intervalIndex] % currentParaCandidates[intervalIndex].length;
				innerIndex[intervalIndex] ++;
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
				+ ", highFrequencyItems=" + Arrays.toString(this.highFrequencyItems) + ", size of currentParaCandidates="
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

	// 获取指定区间中的随机参数值
	private double getIntervalInnerRandomValue(int randomIndex) {

		// 可保证区间内生成参数的基数
		// long intervalInnerIndex = intervalInnerIndexes[intervalIndex]++ % intervalCardinality;
		double intervalInnerIndex = Math.random();

		// 根据频数分位点先做一次映射，从均匀分布映射到基于频数的分段分布上
		if (this.quantileNum > 1){
			ArrayList<Double> quantile = this.quantilePerInterval.get(randomIndex);
			for(int i = 1;i < quantile.size() ; ++i){
				double cdfNow = (double) i / (quantile.size() - 1);
				if (intervalInnerIndex < cdfNow
						+ 1e-7){// eps for float compare
					// 概率上小于第i分位点的概率差
					// 需要将该概率差映射到分段分布上，变成距离第i分位点的长度
					double bias = cdfNow - intervalInnerIndex;
					// 第i-1到i分位点在新分布上的区间长度
					double intervalLength = quantile.get(i) - quantile.get(i-1);
					// 偏差概率bias : 区间总概率(1/quantile.size) = 新区间上的长度biasLength : 区间长度
					double biasLength = bias * (quantile.size() - 1) * intervalLength;

					// 映射后的位置应该是第i分位点向左偏移biasLength
					intervalInnerIndex = quantile.get(i) - biasLength;
					break;
				}
			}
		}


		double avgIntervalLength = 1.0*(maxValue - minValue) / intervalNum;
		double value = (intervalInnerIndex + randomIndex) *
				avgIntervalLength + minValue;

		return value;
	}
}
