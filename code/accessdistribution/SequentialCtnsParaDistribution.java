package accessdistribution;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


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

	@Override
	public Long geneValue() {
//		System.out.println(this.getClass());
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
