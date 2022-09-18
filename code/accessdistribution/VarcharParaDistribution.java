package accessdistribution;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * 字符串类型参数的数据访问分布
 * 测试数据库中字符串属性上的模拟数据与实际线上数据库往往完全不同，所以在生成字符串类型的等值过滤参数时需要
 *   依据相关属性的基数索引和种子字符串数组。
 */
public class VarcharParaDistribution extends DataAccessDistribution {

	// 相应Varchar属性的具体信息
	private long columnCardinality;
	private int minLength, maxLength;
	private String[] seedStrings = null;

	// 高频项需自己生成
	private String[] highFrequencyItems = null;

	public VarcharParaDistribution(double[] hFItemFrequencies, long[] intervalCardinalities,
			double[] intervalFrequencies) {
		super(hFItemFrequencies, intervalCardinalities, intervalFrequencies);
	}

	public VarcharParaDistribution(VarcharParaDistribution distribution){
		super(distribution);

		String[] seedStrings = new String[distribution.seedStrings.length];
		for (int i = 0;i < seedStrings.length; ++i){
			seedStrings[i] = distribution.seedStrings[i];
		}

		setColumnInfo(distribution.columnCardinality, distribution.minLength, distribution.maxLength, seedStrings);
	}

	public VarcharParaDistribution copy(){
		return new VarcharParaDistribution(this);
	}

	public void setColumnInfo(long columnCardinality, int minLength, int maxLength, String[] seedStrings) {
		this.columnCardinality = columnCardinality;
		this.minLength = minLength;
		this.maxLength = maxLength;
		this.seedStrings = seedStrings;
	}

	public void init4VarcharParaGene() {
		highFrequencyItems = new String[highFrequencyItemNum];
		// 假设每个时间窗口中字符串的参数空间是相应属性的全域（故无需根据当前时间窗口的参数值确定一个基数索引的范围）
		if (columnCardinality > highFrequencyItemNum * 2) {
			Set<Long> hFItemIndexSet = new HashSet<>();
			while (hFItemIndexSet.size() < highFrequencyItemNum) {
				long randomParaIndex = (long)(Math.random() * columnCardinality);
				if (!hFItemIndexSet.contains(randomParaIndex)) {
					highFrequencyItems[hFItemIndexSet.size()] = getVarcharValue(randomParaIndex);
					hFItemIndexSet.add(randomParaIndex);
				}
			}
		} else {
			for (int i = 0; i < highFrequencyItemNum && i < columnCardinality; i++) {
				highFrequencyItems[i] = getVarcharValue(i);
			}
		}
	}

	@Override
	public String geneValue() {
		int randomIndex = binarySearch();
		if (randomIndex < highFrequencyItemNum) {
			return highFrequencyItems[randomIndex];
		} else {
			return getIntervalInnerRandomValue(randomIndex);
		}
	}

	private String getIntervalInnerRandomValue(int randomIndex) {
		int intervalIndex = randomIndex - highFrequencyItemNum;
		long intervalCardinality = intervalCardinalities[intervalIndex];
		// long intervalInnerIndex = intervalInnerIndexes[intervalIndex]++ % intervalCardinality;
		long intervalInnerIndex = (long)(Math.random() * intervalCardinality);
		double avgIntervalIndexSize = (double)columnCardinality / intervalNum;
		long randomParaIndex = (long)(((double)intervalInnerIndex / intervalCardinality + intervalIndex) * 
				avgIntervalIndexSize);
		return getVarcharValue(randomParaIndex);
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
	public String toString() {
		return "VarcharParaDistribution [columnCardinality=" + columnCardinality + ", minLength=" + minLength
				+ ", maxLength=" + maxLength + ", size of seedStrings=" + seedStrings.length + ", highFrequencyItems="
				+ Arrays.toString(highFrequencyItems) + ", time=" + time + ", highFrequencyItemNum="
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
		long[] intervalCardinalities = {52, 34, 123, 78, 45, 32, 901, 234, 41, 15, 34, 90, 210, 40, 98};
		// 0.2786
		double[] intervalFrequencies = {0.0175, 0.04024, 0.009808, 0.00874, 0.0245, 0.0257, 0.00754, 0.00695, 
				0.0325, 0.01871, 0.048147, 0.0147, 0.008585, 0.00258, 0.0124};

		VarcharParaDistribution distribution = new VarcharParaDistribution(hFItemFrequencies, 
				intervalCardinalities, intervalFrequencies);
		
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
		
		distribution.setColumnInfo(10000, 20, 50, seedStrings);
		distribution.init4VarcharParaGene();
		
		for (int i = 0; i < 1000000; i++) {
			System.out.println(distribution.geneValue());
		}
	}

	// 无需具体实现
	@Override
	public boolean inDomain(Object parameter) {
		// TODO Auto-generated method stub
		return true;
	}

	// 为了做实验后续添加的，生成完全随机的（即均匀分布）的参数
	@Override
	public String geneUniformValue() {
		return getVarcharValue((long)(Math.random() * columnCardinality));
	}
}
