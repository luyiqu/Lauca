package accessdistribution;

import abstraction.Column;
import config.Configurations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 基于连续时间窗口的数据访问分布
 * 针对对象：整型非键值属性上的等值过滤参数
 */
public class SequentialIntParaDistribution extends SequentialParaDistribution {

	// 当前时间窗口的最小值和最大值
	private long windowMinValue, windowMaxValue;
	// 高频项的重复率（即与上一个时间窗口中高频项重合的比例）
	private double hFItemRepeatRatio;

	// 相应整型属性的基本信息
	private long columnMinValue, columnMaxValue;
	private long columnCardinality;
	private double coefficient;

	// 基数索引的最小值和最大值
	private long minParaIndex, maxParaIndex;

	private long[] highFrequencyItems = null;
	private long[][] currentParaCandidates = null;

	public SequentialIntParaDistribution(long windowMinValue, long windowMaxValue,
										 double[] hFItemFrequencies, long[] intervalCardinalities, double[] intervalFrequencies,
										 double[][] intervalParaRepeatRatios, double hFItemRepeatRatio) {
		super(hFItemFrequencies, intervalCardinalities, intervalFrequencies, intervalParaRepeatRatios);
		this.windowMinValue = windowMinValue;
		this.windowMaxValue = windowMaxValue;
		this.hFItemRepeatRatio = hFItemRepeatRatio;
	}

	public SequentialIntParaDistribution(long windowMinValue, long windowMaxValue,
										 double[] hFItemFrequencies, long[] intervalCardinalities, double[] intervalFrequencies,
										 double[] intervalParaRepeatRatios, double hFItemRepeatRatio) {
		this(windowMinValue, windowMaxValue, hFItemFrequencies, intervalCardinalities, intervalFrequencies, new double[1][0], hFItemRepeatRatio);
		this.intervalParaRepeatRatios[0] = new double[intervalParaRepeatRatios.length];
		System.arraycopy(intervalParaRepeatRatios, 0, this.intervalParaRepeatRatios[0], 0, intervalParaRepeatRatios.length);
	}

	public SequentialIntParaDistribution(long windowMinValue, long windowMaxValue, 
			double[] hFItemFrequencies, long[] intervalCardinalities, double[] intervalFrequencies,
										 double[][] intervalParaRepeatRatios, double hFItemRepeatRatio, ArrayList<ArrayList<Double>> quantilePerInterval) {
		super(hFItemFrequencies, intervalCardinalities, intervalFrequencies, intervalParaRepeatRatios, quantilePerInterval);
		this.windowMinValue = windowMinValue;
		this.windowMaxValue = windowMaxValue;
		this.hFItemRepeatRatio = hFItemRepeatRatio;
	}

	public SequentialIntParaDistribution(SequentialIntParaDistribution sequentialIntParaDistribution){
		super(sequentialIntParaDistribution);
		this.windowMinValue = sequentialIntParaDistribution.windowMinValue;
		this.windowMaxValue = sequentialIntParaDistribution.windowMaxValue;
		this.highFrequencyItems = new long[sequentialIntParaDistribution.highFrequencyItems.length];

		System.arraycopy(sequentialIntParaDistribution.highFrequencyItems, 0, highFrequencyItems, 0, highFrequencyItems.length);
		if (sequentialIntParaDistribution.currentParaCandidates != null){
			geneCandidates(sequentialIntParaDistribution.currentParaCandidates);
		}

		setColumnInfo(sequentialIntParaDistribution.columnMinValue,sequentialIntParaDistribution.columnMaxValue,
				sequentialIntParaDistribution.columnCardinality,sequentialIntParaDistribution.coefficient);
		init();
	}

	public SequentialIntParaDistribution copy(){
		return new SequentialIntParaDistribution(this);
	}

	public void setColumnInfo(long columnMinValue, long columnMaxValue, 
			long columnCardinality, double coefficient) {
		this.columnMinValue = columnMinValue;
		this.columnMaxValue = columnMaxValue;
		this.columnCardinality = columnCardinality;
		this.coefficient = coefficient;

		// 根据当前时间窗口的最小值和最大值计算相应属性的基数索引范围
		minParaIndex = (long)((windowMinValue - columnMinValue) / coefficient);
		maxParaIndex = (long)((windowMaxValue - columnMinValue) / coefficient);
		if (minParaIndex < 0) {
			minParaIndex = 0;
		}
		if (maxParaIndex > columnCardinality - 1) {
			maxParaIndex = columnCardinality - 1;
		}

		// Bug fix: 针对"column = column + para"这种形式的谓词，当前这种处理方式不仅不合理，而且存在bug（生成出来的高频项不够，都是默认值）。
		//   注意此时para的数值大小或者形式... 一般对性能没有什么影响
		if (maxParaIndex - minParaIndex < highFrequencyItemNum - 1) {
			minParaIndex = 0;
			maxParaIndex = highFrequencyItemNum - 1;
		} // 一定程度上避免了一些问题，但仅是治标不治本   -- 先能把高频项生成出来再说...
	}

	// 基于前一个时间窗口的高频项生成当前时间窗口的高频项，需满足hFItemRepeatRatio
	public void geneHighFrequencyItems(long[] priorHighFrequencyItems) {
		List<Long> priorHighFrequencyItemList = new ArrayList<>();
		if (priorHighFrequencyItems != null) {
			for (int i = 0; i < priorHighFrequencyItems.length; i++) {
				if (priorHighFrequencyItems[i] == Long.MIN_VALUE) {
					break;
				}
				priorHighFrequencyItemList.add(priorHighFrequencyItems[i]);
			}
			Collections.shuffle(priorHighFrequencyItemList);
		}

		highFrequencyItems = new long[highFrequencyItemNum];
		Set<Long> highFrequencyItemSet = new HashSet<>();
		int hFItemRepeatNum = (int)(highFrequencyItemNum * hFItemRepeatRatio);
		for (int i = 0; i < hFItemRepeatNum && i < priorHighFrequencyItemList.size(); i++) {
			highFrequencyItems[i] = priorHighFrequencyItemList.get(i);
			highFrequencyItemSet.add(priorHighFrequencyItemList.get(i));
		}
		
		// 有可能highFrequencyItemNum大于或者接近windowIndexSize
		long windowIndexSize = maxParaIndex - minParaIndex + 1;
		if (windowIndexSize > highFrequencyItemNum * 2) {
			while (highFrequencyItemSet.size() < highFrequencyItemNum) {
				long randomParaIndex = (long)(Math.random() * windowIndexSize) + minParaIndex;
				long randomParameter = (long)(randomParaIndex * coefficient + columnMinValue);
				if (!highFrequencyItemSet.contains(randomParameter)) {
					highFrequencyItems[highFrequencyItemSet.size()] = randomParameter;
					highFrequencyItemSet.add(randomParameter);
				}
			}
		} else {
			for (int i = 0; i < windowIndexSize && highFrequencyItemSet.size() < highFrequencyItemNum ; i++) {
				long parameter = (long)(i * coefficient + columnMinValue);
				if (!highFrequencyItemSet.contains(parameter)) {
					highFrequencyItems[highFrequencyItemSet.size()] = parameter;
					highFrequencyItemSet.add(parameter);
				}
			}
			//todo: C_PAYMENT_CNT应该就是这里出现问题了。。20210106
			for (int i = highFrequencyItemSet.size(); i < highFrequencyItemNum; i++) {
				//modified by lyqu
//				highFrequencyItems[i] = Long.MIN_VALUE;
				int j = (int)(Math.random()*i);
				highFrequencyItems[i] = highFrequencyItems[j];
				//-----
			}
		}
	}

	// 基于前一个时间窗口的候选参数集生成当前时间窗口的候选参数集
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
			if (intervalParaRepeatRatios == null || intervalParaRepeatRatios.length == 0) {
				repeatedParaNums[i] = 0;
			} else {
				repeatedParaNums[i] = (int)(intervalCardinalities[i] * intervalParaRepeatRatios[intervalParaRepeatRatios.length - 1][i]);
			}
		}

		double avgIntervalIndexSize = (maxParaIndex - minParaIndex) / (double)intervalNum;
		int[] repeatedParaNumsCopy = Arrays.copyOf(repeatedParaNums, repeatedParaNums.length);
		for (long para : priorParaCandidateList) {
			long parameterIndex = (long)((para - columnMinValue) / coefficient);
			int intervalIndex = (int)((parameterIndex - minParaIndex) / avgIntervalIndexSize);
			if (intervalIndex >= 0 && intervalIndex < intervalNum && 
					repeatedParaNumsCopy[intervalIndex] > 0) {
				int idx = repeatedParaNums[intervalIndex] - repeatedParaNumsCopy[intervalIndex];
				currentParaCandidates[intervalIndex][idx] = para;
				repeatedParaNumsCopy[intervalIndex]--;
			}
		}

		// System.out.println("SequentialIntParaDistribution.geneCandidates - repeatedParaNumsCopy: \n\t" + 
		// 		Arrays.toString(repeatedParaNumsCopy));

		Set<Long> priorParameterSet = new HashSet<>();
		priorParameterSet.addAll(priorParaCandidateList);

		// 补齐各个分区剩下的候选参数集
		for (int i = 0; i < intervalNum; i++) {
			int idx = repeatedParaNums[i] - repeatedParaNumsCopy[i];
			Set<Long> existedParameterSet = new HashSet<>(); // 当前区间中已存在的候选参数
			for (int j = 0; j < idx; j++) {
				existedParameterSet.add(currentParaCandidates[i][j]);
			}

			while (idx < currentParaCandidates[i].length) {
				long randomParaIndex = (long)((Math.random() + i) * avgIntervalIndexSize) + minParaIndex;
				long randomParameter = (long)(randomParaIndex * coefficient + columnMinValue);
				int retryCount = 1;
				while (priorParameterSet.contains(randomParameter) || 
						existedParameterSet.contains(randomParameter)) {
					if (retryCount++ > 5) {
						break;
					}
					randomParaIndex = (long)((Math.random() + i) * avgIntervalIndexSize) + minParaIndex;
					randomParameter = (long)(randomParaIndex * coefficient + columnMinValue);
				}
				currentParaCandidates[i][idx] = randomParameter;
				existedParameterSet.add(randomParameter);
				idx++;
			}
		} // for intervalNum
	}

//	//added by qly:针对fake column生成假的分布,由于fake column不需要考虑前后，就没调用Seqential的，直接用了一般的。
//	public SequentialIntParaDistribution generateFakeColumnParaDistribution(Column column){
//		long windowMinValue =(long)column.getPara1(), windowMaxValue = (long)column.getPara2();
//		int hFItemNumber = Configurations.getHighFrequencyItemNum();
//		int intervalNumber = Configurations.getIntervalNum();
//		double[] hFItemFrequencies = new double[hFItemNumber];
//		double[] intervalFrequencies = new double[intervalNumber];
//		long[] intervalCardinalities = new long[intervalNumber];
//		double uniformFrequencies = 1.0/(hFItemNumber+intervalNumber);
//		for(int i=0;i < hFItemNumber;i++){
//			hFItemFrequencies[i] = uniformFrequencies;
//		}
//		for(int i = 0;i < intervalNumber;i++){
//			intervalFrequencies[i] = uniformFrequencies;
//			intervalCardinalities[i] = (long)(Math.random()*column.getCardinality());
//		}
//		double[] intervalParaRepeatRatios = null;
//		double hFItemRepeatRatio = 0;
//
//		SequentialIntParaDistribution distribution = new SequentialIntParaDistribution(windowMinValue, windowMaxValue,
//				hFItemFrequencies, intervalCardinalities, intervalFrequencies, intervalParaRepeatRatios, hFItemRepeatRatio);;
//		distribution.setColumnInfo((long)column.getPara1(),(long)column.getPara2(),column.getCardinality(), column.getCoefficient());
//		return distribution;  //注意：在生成分布的时候不要分配到这了，类型一直设为1把
//	}

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

	public void setHighFrequencyItems(long[] highFrequencyItems) {
		this.highFrequencyItems = highFrequencyItems;
	}
	
	public long[] getHighFrequencyItems() {
		return highFrequencyItems;
	}

	public void setCurrentParaCandidates(long[][] currentParaCandidates) {
		this.currentParaCandidates = currentParaCandidates;
	}
	
	public long[][] getCurrentParaCandidates() {
		return currentParaCandidates;
	}

	@Override
	public String toString() {
		return "SequentialIntParaDistribution [windowMinValue=" + windowMinValue + ", windowMaxValue=" + windowMaxValue
				+ ", hFItemRepeatRatio=" + hFItemRepeatRatio + ", columnMinValue=" + columnMinValue
				+ ", columnMaxValue=" + columnMaxValue + ", columnCardinality=" + columnCardinality + ", coefficient="
				+ coefficient + ", minParaIndex=" + minParaIndex + ", maxParaIndex=" + maxParaIndex
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
		long windowMinValue = 12, windowMaxValue = 329962;
		// 0.7214
		double[] hFItemFrequencies = {0.05, 0.1101, 0.065, 0.127, 0.087, 0.049, 0.1195, 0.023, 0.031, 0.0598};
		long[] intervalCardinalities = {52, 34, 123, 78, 45, 32, 901, 234, 41, 15, 34, 90, 210, 40, 98};
		// 0.2786
		double[] intervalFrequencies = {0.0175, 0.04024, 0.009808, 0.00874, 0.0245, 0.0257, 0.00754, 0.00695, 
				0.0325, 0.01871, 0.048147, 0.0147, 0.008585, 0.00258, 0.0124};
		double[] intervalParaRepeatRatios = null;
		double hFItemRepeatRatio = 0;
		SequentialIntParaDistribution distribution1 = new SequentialIntParaDistribution(windowMinValue, windowMaxValue, 
				hFItemFrequencies, intervalCardinalities, intervalFrequencies, intervalParaRepeatRatios, hFItemRepeatRatio);
		distribution1.setColumnInfo(-100000, 500000, 10000, 60);
		distribution1.geneHighFrequencyItems(null);
		distribution1.geneCandidates(null);

		long windowMinValue2 = 358, windowMaxValue2 = 344156;
		// 0.7214
		double[] hFItemFrequencies2 = {0.05, 0.1101, 0.065, 0.127, 0.087, 0.049, 0.1195, 0.023, 0.031, 0.0598};
		long[] intervalCardinalities2 = {152, 94, 87, 102, 65, 28, 305, 385, 65, 35, 120, 68, 158, 52, 67};
		// 0.2786
		double[] intervalFrequencies2 = {0.0175, 0.04024, 0.009808, 0.00874, 0.0245, 0.0257, 0.00754, 0.00695, 
				0.0325, 0.01871, 0.048147, 0.0147, 0.008585, 0.00258, 0.0124};
		double[] intervalParaRepeatRatios2 = {0.27, 0.24, 0.184, 0.274, 0.52, 0.348, 0.048, 0.287, 0.549, 
				0.724, 0.105, 0.121, 0.1874, 0.005, 0.02};
		double hFItemRepeatRatio2 = 0.4895;
		SequentialIntParaDistribution distribution2 = new SequentialIntParaDistribution(windowMinValue2, windowMaxValue2, 
				hFItemFrequencies2, intervalCardinalities2, intervalFrequencies2, intervalParaRepeatRatios2, hFItemRepeatRatio2);
		distribution2.setColumnInfo(-100000, 500000, 10000, 60);
		distribution2.geneHighFrequencyItems(distribution1.getHighFrequencyItems());
		distribution2.geneCandidates(distribution1.getCurrentParaCandidates());
		for (int i = 0; i < 1000000; i++) {
			System.out.println(distribution2.geneValue());
		}
	}

	@Override
	public boolean inDomain(Object parameter) {
		long para = (Long)parameter;
		if (para < columnMinValue || para > columnMaxValue) {
			return false;
		} else {
			return true;
		}
	}

	// bug fix: 为了做实验后续添加的，生成完全随机的（即均匀分布）的参数
	@Override
	public Long geneUniformValue() {
		return (long)(Math.random() * (windowMaxValue - windowMinValue) + windowMinValue);
	}
}
