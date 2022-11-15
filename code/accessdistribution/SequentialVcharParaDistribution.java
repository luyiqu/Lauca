package accessdistribution;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SequentialVcharParaDistribution extends SequentialParaDistribution {

	private double hFItemRepeatRatio;

	// 相应Varchar属性的具体信息
	private long columnCardinality;
	private int minLength, maxLength;
	private String[] seedStrings = null;

	private String[] highFrequencyItems = null;
	private String[][] currentParaCandidates = null;

	public SequentialVcharParaDistribution(double[] hFItemFrequencies, long[] intervalCardinalities,
			double[] intervalFrequencies, ArrayList<double[]> intervalParaRepeatRatios, double hFItemRepeatRatio) {
		super(hFItemFrequencies, intervalCardinalities, intervalFrequencies, intervalParaRepeatRatios);
		this.hFItemRepeatRatio = hFItemRepeatRatio;
	}

	public SequentialVcharParaDistribution(double[] hFItemFrequencies, long[] intervalCardinalities,
										   double[] intervalFrequencies, double[] intervalParaRepeatRatios, double hFItemRepeatRatio) {
		this(hFItemFrequencies, intervalCardinalities, intervalFrequencies, new ArrayList<double[]>() , hFItemRepeatRatio);
		this.intervalParaRepeatRatios.add(intervalParaRepeatRatios);
	}

	public SequentialVcharParaDistribution(SequentialVcharParaDistribution sequentialVcharParaDistribution){
		super(sequentialVcharParaDistribution);
		this.hFItemRepeatRatio = sequentialVcharParaDistribution.hFItemRepeatRatio;
		this.highFrequencyItems = new String[sequentialVcharParaDistribution.highFrequencyItems.length];

		for (int i = 0 ;i< highFrequencyItems.length; ++i){
			highFrequencyItems[i] = sequentialVcharParaDistribution.highFrequencyItems[i];
		}


		setColumnInfo(sequentialVcharParaDistribution.columnCardinality,sequentialVcharParaDistribution.minLength,
				sequentialVcharParaDistribution.maxLength,sequentialVcharParaDistribution.seedStrings);
		geneHighFrequencyItems(sequentialVcharParaDistribution.highFrequencyItems);
		if (sequentialVcharParaDistribution.currentParaCandidates != null){
			geneCandidates(sequentialVcharParaDistribution.currentParaCandidates);
		}
	}

	// not realize now, return itself
	public SequentialVcharParaDistribution copy(){
		return this;
	}

	public void setColumnInfo(long columnCardinality, int minLength, int maxLength, String[] seedStrings) {
		this.columnCardinality = columnCardinality;
		this.minLength = minLength;
		this.maxLength = maxLength;
		this.seedStrings = new String[seedStrings.length];
		for (int i = 0; i < seedStrings.length; i++) {
			this.seedStrings[i] = seedStrings[i];
		}
	}

	public void geneHighFrequencyItems(String[] priorHighFrequencyItems) {
		List<String> priorHighFrequencyItemList = new ArrayList<>();
		if (priorHighFrequencyItems != null) {
			for (int i = 0; i < priorHighFrequencyItems.length; i++) {
				if (priorHighFrequencyItems[i] == null) {
					break;
				}
				priorHighFrequencyItemList.add(priorHighFrequencyItems[i]);
			}
			Collections.shuffle(priorHighFrequencyItemList);
		}

		highFrequencyItems = new String[highFrequencyItemNum];
		Set<String> highFrequencyItemSet = new HashSet<>();
		int hFItemRepeatNum = (int)(highFrequencyItemNum * hFItemRepeatRatio);
		for (int i = 0; i < hFItemRepeatNum && i < priorHighFrequencyItemList.size(); i++) {
			highFrequencyItems[i] = priorHighFrequencyItemList.get(i);
			highFrequencyItemSet.add(priorHighFrequencyItemList.get(i));
		}

		if (columnCardinality > highFrequencyItemNum * 2) {
			while (highFrequencyItemSet.size() < highFrequencyItems.length) {
				long randomParaIndex = (long)(Math.random() * columnCardinality);
				String randomParameter = getVarcharValue(randomParaIndex);
				if (!highFrequencyItemSet.contains(randomParameter)) {
					highFrequencyItems[highFrequencyItemSet.size()] = randomParameter;
					highFrequencyItemSet.add(randomParameter);
				}
			}
		} else {
			for (int i = 0; i < columnCardinality && highFrequencyItemSet.size() < highFrequencyItemNum ; i++) {
				String parameter = getVarcharValue(i);
				if (!highFrequencyItemSet.contains(parameter)) {
					highFrequencyItems[highFrequencyItemSet.size()] = parameter;
					highFrequencyItemSet.add(parameter);
				}
			}
		}
	}

	public void geneCandidates(String[][] priorParaCandidates) {
		List<String> priorParaCandidateList = new ArrayList<>();
		if (priorParaCandidates != null) {
			for (String[] tmpArr : priorParaCandidates) {
				for (String tmpItem : tmpArr) {
					priorParaCandidateList.add(tmpItem);
				}
			}
			Collections.shuffle(priorParaCandidateList);
		}

		currentParaCandidates = new String[intervalNum][];
		int[] repeatedParaNums = new int[intervalNum];
		for (int i = 0; i < intervalNum; i++) {
			// 对于区间内参数基数超过int最大值的情形暂不考虑~
			currentParaCandidates[i] = new String[(int)intervalCardinalities[i]];
			if (intervalParaRepeatRatios == null) {
				repeatedParaNums[i] = 0;
			} else {
				repeatedParaNums[i] = (int)(intervalCardinalities[i] * intervalParaRepeatRatios.get(intervalParaRepeatRatios.size() - 1)[i]);
			}
		}

		//lyqu: 根据重复率，把之前时间窗口的参数放入这个时间窗口中
		double avgIntervalIndexSize = columnCardinality / (double)intervalNum;
		int[] repeatedParaNumsCopy = Arrays.copyOf(repeatedParaNums, repeatedParaNums.length);
		for (String para : priorParaCandidateList) {
			long parameterIndex = Long.parseLong(para.split("#")[0]);
			int intervalIndex = (int)(parameterIndex / avgIntervalIndexSize); // intervalIndex必然存在
			if (repeatedParaNumsCopy[intervalIndex] > 0) {
				int idx = repeatedParaNums[intervalIndex] - repeatedParaNumsCopy[intervalIndex];
				currentParaCandidates[intervalIndex][idx] = para;
				repeatedParaNumsCopy[intervalIndex]--;
			}
		}

		// System.out.println("SequentialVcharParaDistribution.geneCandidates - repeatedParaNumsCopy: \n\t" + 
		// 		Arrays.toString(repeatedParaNumsCopy));

		Set<String> priorParameterSet = new HashSet<>();
		priorParameterSet.addAll(priorParaCandidateList);

		for (int i = 0; i < intervalNum; i++) {
			int idx = repeatedParaNums[i] - repeatedParaNumsCopy[i];
			Set<String> existedParameterSet = new HashSet<>();
			for (int j = 0; j < idx; j++) {
				existedParameterSet.add(currentParaCandidates[i][j]);
			}

			while (idx < currentParaCandidates[i].length) {
				long randomParaIndex = (long)((Math.random() + i) * avgIntervalIndexSize);
				String randomParameter = getVarcharValue(randomParaIndex);
				int retryCount = 1;
				while (priorParameterSet.contains(randomParameter) || 
						existedParameterSet.contains(randomParameter)) {
					if (retryCount++ > 5) {
						break;
					}
					randomParaIndex = (long)((Math.random() + i) * avgIntervalIndexSize);
					randomParameter = getVarcharValue(randomParaIndex);
				}
				currentParaCandidates[i][idx] = randomParameter;
				existedParameterSet.add(randomParameter);
				idx++;
			}
		} // for intervalNum

	}

	private String getVarcharValue(long randomParaIndex) {
		String value = randomParaIndex + "#" + seedStrings[(int)(randomParaIndex % seedStrings.length)];
		if (value.length() > maxLength) {
			return value.substring(0, maxLength);
		} else {
			return value;
		}
	}

	@Override
	public String geneValue() {
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

	public void setHighFrequencyItems(String[] highFrequencyItems) {
		this.highFrequencyItems = highFrequencyItems;
	}

	public String[] getHighFrequencyItems() {
		return highFrequencyItems;
	}

	public void setCurrentParaCandidates(String[][] currentParaCandidates) {
		this.currentParaCandidates = currentParaCandidates;
	}

	public String[][] getCurrentParaCandidates() {
		return currentParaCandidates;
	}

	@Override
	public String toString() {
		return "SequentialVcharParaDistribution [hFItemRepeatRatio=" + hFItemRepeatRatio + ", columnCardinality="
				+ columnCardinality + ", minLength=" + minLength + ", maxLength=" + maxLength + ", size of seedStrings="
				+ seedStrings.length + ", highFrequencyItems=" + Arrays.toString(highFrequencyItems)
				+ ", size of currentParaCandidates=" + currentParaCandidates.length + ", intervalParaRepeatRatios="
				+ Arrays.toString(intervalParaRepeatRatios.get(intervalParaRepeatRatios.size() - 1)) + ", time=" + time + ", highFrequencyItemNum="
				+ highFrequencyItemNum + ", hFItemFrequencies=" + Arrays.toString(hFItemFrequencies) + ", intervalNum="
				+ intervalNum + ", intervalCardinalities=" + Arrays.toString(intervalCardinalities)
				+ ", intervalFrequencies=" + Arrays.toString(intervalFrequencies) + ", cumulativeFrequencies="
				+ Arrays.toString(cumulativeFrequencies) + ", intervalInnerIndexes="
				+ Arrays.toString(intervalInnerIndexes) + "]";
	}

	// for testing
	public static void main(String[] args) {
		// 0.7214
		double[] hFItemFrequencies = {0.05, 0.1101, 0.065, 0.127, 0.087, 0.049, 0.1195, 0.023, 0.031, 0.0598};
		long[] intervalCardinalities = {52, 34, 123, 78, 45, 32, 280, 184, 41, 31, 34, 90, 210, 40, 98};
		// 0.2786
		double[] intervalFrequencies = {0.0175, 0.04024, 0.009808, 0.00874, 0.0245, 0.0257, 0.00754, 0.00695, 
				0.0325, 0.01871, 0.048147, 0.0147, 0.008585, 0.00258, 0.0124};
		double[] intervalParaRepeatRatios = null;
		double hFItemRepeatRatio = 0;
		SequentialVcharParaDistribution distribution1 = new SequentialVcharParaDistribution(hFItemFrequencies, 
				intervalCardinalities, intervalFrequencies, intervalParaRepeatRatios, hFItemRepeatRatio);

		char[] chars = ("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz").toCharArray();
		String[] seedStrings = new String[100];
		for (int i = 0; i < seedStrings.length; i++) {
			int length = (int)(Math.random() * 31 + 20);
			char[] buffer = new char[length];
			for (int j = 0; j < length; j++) {
				buffer[j] = chars[(int)(Math.random() * 62)];
			}
			seedStrings[i] = new String(buffer);
		}
		distribution1.setColumnInfo(10000, 20, 50, seedStrings);
		distribution1.geneHighFrequencyItems(null);
		distribution1.geneCandidates(null);

		// 0.7214
		double[] hFItemFrequencies2 = {0.05, 0.1101, 0.065, 0.127, 0.087, 0.049, 0.1195, 0.023, 0.031, 0.0598};
		long[] intervalCardinalities2 = {152, 94, 87, 102, 65, 28, 194, 234, 65, 25, 120, 68, 158, 52, 105};
		// 0.2786
		double[] intervalFrequencies2 = {0.0175, 0.04024, 0.009808, 0.00874, 0.0245, 0.0257, 0.00754, 0.00695, 
				0.0325, 0.01871, 0.048147, 0.0147, 0.008585, 0.00258, 0.0124};
		double[] intervalParaRepeatRatios2 = {0.27, 0.24, 0.184, 0.274, 0.52, 0.348, 0.348, 0.387, 0.549, 
				0.724, 0.105, 0.121, 0.1874, 0.005, 0.5};
		double hFItemRepeatRatio2 = 0.4895;
		SequentialVcharParaDistribution distribution2 = new SequentialVcharParaDistribution(hFItemFrequencies2, 
				intervalCardinalities2, intervalFrequencies2, intervalParaRepeatRatios2, hFItemRepeatRatio2);
		distribution2.setColumnInfo(10000, 20, 50, seedStrings);
		distribution2.geneHighFrequencyItems(distribution1.getHighFrequencyItems());
		distribution2.geneCandidates(distribution1.getCurrentParaCandidates());
		for (int i = 0; i < 10000000; i++) {
			// System.out.println(distribution2.geneValue());
			distribution2.geneValue();
		}
	}

	@Override
	public boolean inDomain(Object parameter) {
		// TODO Auto-generated method stub
		return true;
	}

	// bug fix: 为了做实验后续添加的，生成完全随机的（即均匀分布）的参数
	@Override
	public String geneUniformValue() {
		return getVarcharValue((long)(Math.random() * columnCardinality));
	}
}
