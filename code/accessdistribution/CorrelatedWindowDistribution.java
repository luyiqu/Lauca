package accessdistribution;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

// 分布三：基于连续时间窗口的多属性关联数据访问分布
// 体现多个参数之间的关联性 和 相邻时间窗口数据访问分布之间的连续性
// 这里的多个参数一般应位于同一个SQL的where条件中，或者应属于同一个主键（复合主键）

public class CorrelatedWindowDistribution {

	// 多个属性（输入参数）的域空间
	private int columnNum;
	private int[] minValues = null;
	private int[] maxValues = null;

	// 高频项个数
	private int highFrequencyItemNum;
	// 高频项以及相应的频率，现在一个数据项是一组元素（多个属性值）
	private long[][] highFrequencyItems = null;
	private double[] hFItemFrequencies = null;

	// 每个属性（输入参数）的分区数，一般针对不同数据类型的属性有不同的分区数默认值
	private int[] columnsIntervalNum = null;
	// 总区间数，等于columnsIntervalNum中所有值的连乘
	private int intervalNum;
	// 每个区间的基数。注意：这里将多维空间的编号转化成单维编号，编号规则：从右边属性依次累加进位（单维到多维）
	private int[] intervalCardinalitis = null;
	// 每个区间的的频率分布（同样也刨掉了高频项）
	private double[] intervalFrequencies = null;

	// 每个区间中当前时间窗口的 候选输入参数组 与 上一个时间窗口重复的比例
	private double[] repeatedItemRatios = null;
	// 现在每个candidate是一个数组（多个属性）。[][][]：每个区间的多个候选参数组
	private long[][][] currentParaCandidates = null;

	// 支持数据的随机生成（多维空间编号转单维编号表示）
	private double[] cumulativeFrequencies = null;
	// 保证基数与期望一致（多维空间编号转单维编号表示）
	private int[] cardinalityIndexes = null;

	public CorrelatedWindowDistribution(int[] minValues, int[] maxValues, long[][] highFrequencyItems,
			double[] hFItemFrequencies, int[] columnsIntervalNum, int[] intervalCardinalitis,
			double[] intervalFrequencies, double[] repeatedItemRatios) {
		super();
		this.minValues = minValues;
		this.maxValues = maxValues;
		this.highFrequencyItems = highFrequencyItems;
		this.hFItemFrequencies = hFItemFrequencies;
		this.columnsIntervalNum = columnsIntervalNum;
		this.intervalCardinalitis = intervalCardinalitis;
		this.intervalFrequencies = intervalFrequencies;
		this.repeatedItemRatios = repeatedItemRatios;

		columnNum = minValues.length;
		highFrequencyItemNum = highFrequencyItems.length;
		intervalNum = intervalCardinalitis.length;

		cumulativeFrequencies = new double[intervalNum];
		cumulativeFrequencies[0] = hFItemFrequencies[0];
		for (int i = 1; i < cumulativeFrequencies.length; i++) {
			if (i < highFrequencyItemNum) {
				cumulativeFrequencies[i] = cumulativeFrequencies[i - 1] + hFItemFrequencies[i];
			} else {
				cumulativeFrequencies[i] = cumulativeFrequencies[i - 1] + intervalFrequencies[i - highFrequencyItemNum];
			}
		}

		cardinalityIndexes = new int[intervalNum];
		for (int i = 0; i < intervalNum; i++) {
			cardinalityIndexes[i] = (int)(Math.random() * intervalCardinalitis[i]);
		}
	}

	public long[] geneValue() {
		double randomValue = Math.random();
		if (randomValue < cumulativeFrequencies[0]) {
			return highFrequencyItems[0];
		}
		int low = 1, high = cumulativeFrequencies.length - 1;
		while (low <= high) {
			int middle = (low + high) / 2;
			if (randomValue < cumulativeFrequencies[middle - 1]) {
				high = middle - 1;
			} else if (randomValue >= cumulativeFrequencies[middle]) {
				low = middle + 1;
			} else {
				if (middle < highFrequencyItemNum) {
					return highFrequencyItems[middle];
				} else {
					int intervalIndex = middle - highFrequencyItemNum;
					int cardinalityIndex = cardinalityIndexes[intervalIndex]++ % intervalCardinalitis[intervalIndex];
					return currentParaCandidates[intervalIndex][cardinalityIndex];
				}
			}
		}
		return null;
	}

	public void geneCandidates(long[][][] priorParaCandidates) {
		// 总分区数相同，同时也假设各属性分段数也完全相同
		currentParaCandidates = new long[priorParaCandidates.length][][];
		for (int i = 0; i < intervalNum; i++) {
			currentParaCandidates[i] = new long[intervalCardinalitis[i]][];
			int repeatedItemSize = (int)(intervalCardinalitis[i] * repeatedItemRatios[i]);

			List<long[]> tmpList = new ArrayList<long[]>();
			for (int j = 0; j < priorParaCandidates[i].length; j++) {
				tmpList.add(priorParaCandidates[i][j]);
			}
			Collections.shuffle(tmpList);

			// 如何用String表示占用内存过多，可用相应的hash code代替
			Set<String> currentParaCandidateSet = new HashSet<String>();
			Set<String> priorParaCandidateSet = new HashSet<String>();
			for (int j = 0; j < repeatedItemSize; j++) {
				currentParaCandidates[i][j] = tmpList.get(j);
				currentParaCandidateSet.add(Arrays.toString(tmpList.get(j)));
			}
			for (int j = repeatedItemSize; j < tmpList.size(); j++) {
				priorParaCandidateSet.add(Arrays.toString(tmpList.get(j)));
			}

			// 随机生成该分区其余的候选参数集
			int remainingSize = intervalCardinalitis[i] - repeatedItemSize;
			int idx = repeatedItemSize; // index of currentParaCandidates[i]

			// 转化：i (current interval index) -> all column interval indexes
			int[] columnIntervalIndexes = new int[columnNum];
			int columnsIntervalNumProduct = 1;
			for (int k = 0; k < columnNum; k++) {
				columnsIntervalNumProduct *= columnsIntervalNum[k];
			}
			int tmpi = i;
			for (int k = 0; k < columnNum; k++) {
				columnsIntervalNumProduct /= columnsIntervalNum[k];
				columnIntervalIndexes[k] = tmpi / columnsIntervalNumProduct;
				tmpi -= columnIntervalIndexes[k] * columnsIntervalNumProduct;
			}

			loop : for (int j = 0; j < remainingSize; j++) {
				long[] candidateItem = new long[columnNum];
				int retryCount = 0;
				while (retryCount == 0 || currentParaCandidateSet.contains(Arrays.toString(candidateItem)) 
						|| priorParaCandidateSet.contains(Arrays.toString(candidateItem))) {
					// 生成一组在当前区间中的随机输入参数组
					for (int k = 0; k < columnNum; k++) {
						double columnIntervalSize = (maxValues[k] - minValues[k]) / (double)columnsIntervalNum[k];
						long random = (long)((Math.random() + columnIntervalIndexes[k]) * columnIntervalSize) + minValues[k];
						candidateItem[k] = random;
					}

					// if (retryCount++ > 3) {
					//     break loop;
					// }

					// 下面的遍历未实现，故这里暂且这么处理~，正确性上可能有一点偏差
					if (retryCount++ > 5) {
						System.out.println("WARN: retryCount++ > 5!");
						break;
					}
				}

				currentParaCandidates[i][idx++] = candidateItem;
				currentParaCandidateSet.add(Arrays.toString(candidateItem));
			}

			// 遍历参数空间，以降低重试代价
			// TODO
			// if (idx < currentParaCandidates[i].length) {
			// }

		} //  for-intervalNum
	}
}
