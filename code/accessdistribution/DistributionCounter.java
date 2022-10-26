package accessdistribution;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;

import config.Configurations;
import serializable.DistributionCounter4Serial;

/*
 * 参数的数据访问分布统计器，针对每一种访问分布都构建了相应的静态统计方法~
 * 号外：对于insert、replace和delete的参数生成，其主键属性上的参数一般都可通过事务逻辑确定，无需考虑主键唯一性和主键miss情况
 */
public class DistributionCounter {

	// 针对所有事务中所有参数 统计好的访问分布都存放在该数据结构中。事务名称 -> 参数标示符 -> 数据分布信息列表
	// 参数标示符格式：operationId_index index从0开始
	// 注意在目前的机制下（同一个参数的数据访问分布肯定由同一个线程按序分析得到），下面的数据结构不用Vector，可直接用ArrayList
	// 需要序列化
	private static Map<String, Map<String, Vector<DataAccessDistribution>>> txName2ParaId2DistributionList = null;

	// 下面四个数据结构服务于基于连续时间窗口数据访问分布的统计，下面这四个不用序列化
	// 保存最近一个时间窗口的原始数据， 事务名称 -> 参数标示符 -> 数据（强转好的数据），用于统计intervalParaRepeatRatios
	private static Map<String, Map<String, Object>> txName2ParaId2Data = null;
	// 保存最近一个时间窗口的候选参数集，事务名称 -> 参数标示符 -> 候选参数集
	private static Map<String, Map<String, Object>> txName2ParaId2ParaCandidates = null;

	// 保存日志中最近一个时间窗口的高频项，用于统计hFItemRepeatRatio。事务名称 -> 参数标示符 -> 高频项集
	private static Map<String, Map<String, Object>> txName2ParaId2LogHFItems = null;
	// 保存自己生成的最近一个时间窗口的高频项，事务名称 -> 参数标示符 -> 高频项集
	private static Map<String, Map<String, Object>> txName2ParaId2GeneHFItems = null;

	// 在分析数据访问分布的同时，获取各个事务的吞吐
	// 事务名称 -> 参数标示符 ->
	// 所属操作的平均运行次数（可能是branch和multiple逻辑中的操作），注意这里针对每个事务仅相应地维护了一个参数标示符的信息
	// 该成员信息是由外界设置进来的
	private static Map<String, Map<String, Double>> txName2ParaId2AvgRunTimes = null;
	// 事务名称 -> 各个时间窗口的吞吐信息，需要序列化
	private static Map<String, Vector<Throughput>> txName2ThroughputList = null;

	// 支持全负载周期数据访问分布统计的sampling数据
	private static Map<String, Map<String, List<String>>> txName2ParaId2SamplingData = null;
	// 每个参数的累积数据量大小，支持数据均匀采样机制
	private static Map<String, Map<String, Long>> txName2ParaId2CumulativeSize = null;
	// 利用采样的数据 统计 得到全负载周期数据访问分布，需要序列化
	private static Map<String, Map<String, DataAccessDistribution>> txName2ParaId2FullLifeCycleDistribution = null;
	// 记录每个参数的数据分布类型
	private static Map<String, Map<String, DistributionTypeInfo>> txName2ParaId2DistributionType = null;

	// 初始化所有成员变量
	public static void init(Map<String, List<List<String>>> txName2StatParameters) {
		txName2ParaId2DistributionList = new HashMap<>();

		txName2ParaId2Data = new HashMap<>();
		txName2ParaId2ParaCandidates = new HashMap<>();
		txName2ParaId2LogHFItems = new HashMap<>();
		txName2ParaId2GeneHFItems = new HashMap<>();

		txName2ThroughputList = new HashMap<>();

		txName2ParaId2SamplingData = new HashMap<>();
		txName2ParaId2CumulativeSize = new HashMap<>();
		txName2ParaId2FullLifeCycleDistribution = new HashMap<>();
		txName2ParaId2DistributionType = new HashMap<>();

		Iterator<Entry<String, List<List<String>>>> iter = txName2StatParameters.entrySet().iterator();
		while (iter.hasNext()) {
			Entry<String, List<List<String>>> entry = iter.next();

			Map<String, Vector<DataAccessDistribution>> paraId2DistributionList = new HashMap<>();
			txName2ParaId2DistributionList.put(entry.getKey(), paraId2DistributionList);

			txName2ParaId2Data.put(entry.getKey(), new HashMap<>());
			txName2ParaId2ParaCandidates.put(entry.getKey(), new HashMap<>());
			txName2ParaId2LogHFItems.put(entry.getKey(), new HashMap<>());
			txName2ParaId2GeneHFItems.put(entry.getKey(), new HashMap<>());

			txName2ThroughputList.put(entry.getKey(), new Vector<>());

			Map<String, List<String>> paraId2SamplingData = new HashMap<>();
			txName2ParaId2SamplingData.put(entry.getKey(), paraId2SamplingData);
			Map<String, Long> paraId2CumulativeSize = new HashMap<>();
			txName2ParaId2CumulativeSize.put(entry.getKey(), paraId2CumulativeSize);
			txName2ParaId2FullLifeCycleDistribution.put(entry.getKey(), new HashMap<>());
			txName2ParaId2DistributionType.put(entry.getKey(), new HashMap<>());

			for (List<String> parameters : entry.getValue()) {
				for (String parameter : parameters) {
					String[] arr = parameter.split("_");
					int operationId = Integer.parseInt(arr[0]);
					int paraIndex = Integer.parseInt(arr[2]);
					String identifier = operationId + "_" + paraIndex;

					paraId2DistributionList.put(identifier, new Vector<>());
					paraId2SamplingData.put(identifier, new ArrayList<>());
					paraId2CumulativeSize.put(identifier, 0L);
				}
			}
		}
	}

	public static void setTxName2ParaId2AvgRunTimes(Map<String, Map<String, Double>> txName2ParaId2AvgRunTimes) {
		DistributionCounter.txName2ParaId2AvgRunTimes = txName2ParaId2AvgRunTimes;
	}

	public static void count(String txName, String paraIdentifier, long windowTime, DistributionTypeInfo distTypeInfo,
			List<String> data) {

		if (data.size() == 0) {
			return;
		}

		// 获取事务吞吐信息
		//todo 20210127 先看一下这个模块对不对，再追溯到data.size的统计
		if (txName2ParaId2AvgRunTimes.get(txName).containsKey(paraIdentifier)) {

			int throughput = (int) Math.round(data.size() / txName2ParaId2AvgRunTimes.get(txName).get(paraIdentifier));

//			if(txName.contains("2")){
////				System.out.println("dataSize: "+data.size() );
////				System.out.println(txName2ParaId2AvgRunTimes.get(txName).get(paraIdentifier));
////				System.out.println("************");
////			}


			txName2ThroughputList.get(txName).add(new Throughput(txName, windowTime, throughput));
		}

		//todo 这里吧data=#@#的值删掉吧！
		data.removeIf(d -> d.equals("#@#"));
		if(data.isEmpty()){
//			txName2ParaId2DistributionList.get(txName).get(paraIdentifier).add(null);
			return;
		}

//		System.out.println(data);
//
		switch (distTypeInfo.distributionType) {
		case 0:
			DataAccessDistribution distribution0 = countContinuousParaDistribution(distTypeInfo.dataType, data);
			distribution0.setTime(windowTime);
//			System.out.println("DistributionCounter "+"windowTime: "+windowTime);
			txName2ParaId2DistributionList.get(txName).get(paraIdentifier).add(distribution0);
			// System.out.println(txName + " " + paraIdentifier + "\n" + distribution0);
			break;
		case 1:
			IntegerParaDistribution distribution1 = countIntegerParaDistribution(data);
			distribution1.setColumnInfo(distTypeInfo.columnMinValue, distTypeInfo.columnMaxValue,
					distTypeInfo.columnCardinality, distTypeInfo.coefficient);
			distribution1.setTime(windowTime);
			distribution1.init4IntegerParaGene();
			txName2ParaId2DistributionList.get(txName).get(paraIdentifier).add(distribution1);
//			System.out.println(txName + " " + paraIdentifier + "\n" + distribution1);
			break;
		case 2:
			VarcharParaDistribution distribution2 = countVarcharParaDistribution(data);
			distribution2.setColumnInfo(distTypeInfo.columnCardinality, distTypeInfo.minLength, distTypeInfo.maxLength,
					distTypeInfo.seedStrings);
			distribution2.setTime(windowTime);
			distribution2.init4VarcharParaGene();
			txName2ParaId2DistributionList.get(txName).get(paraIdentifier).add(distribution2);
			// System.out.println(txName + " " + paraIdentifier + "\n" + distribution2);
			break;
		case 3: // 分布3、4、5是有写盘需求的（暂不实现，目前全部存储在内存中） TODO
			SequentialCtnsParaDistribution distribution3 = countSequentialCtnsParaDistribution(data, txName,
					paraIdentifier);
			distribution3.setTime(windowTime);
			long[][] priorParaCandidates = (long[][]) txName2ParaId2ParaCandidates.get(txName).get(paraIdentifier);
			distribution3.geneCandidates(priorParaCandidates);
			txName2ParaId2ParaCandidates.get(txName).put(paraIdentifier, distribution3.getCurrentParaCandidates());
			txName2ParaId2DistributionList.get(txName).get(paraIdentifier).add(distribution3);
			// System.out.println(txName + " " + paraIdentifier + "\n" + distribution3);
			break;
		case 4:
			SequentialIntParaDistribution distribution4 = countSequentialIntParaDistribution(data, txName,
					paraIdentifier);
			distribution4.setColumnInfo(distTypeInfo.columnMinValue, distTypeInfo.columnMaxValue,
					distTypeInfo.columnCardinality, distTypeInfo.coefficient);
			distribution4.setTime(windowTime);
			long[] priorHighFrequencyItems = (long[]) txName2ParaId2GeneHFItems.get(txName).get(paraIdentifier);
			distribution4.geneHighFrequencyItems(priorHighFrequencyItems);
			txName2ParaId2GeneHFItems.get(txName).put(paraIdentifier, distribution4.getHighFrequencyItems());
			priorParaCandidates = (long[][]) txName2ParaId2ParaCandidates.get(txName).get(paraIdentifier);
			distribution4.geneCandidates(priorParaCandidates);
			txName2ParaId2ParaCandidates.get(txName).put(paraIdentifier, distribution4.getCurrentParaCandidates());
			txName2ParaId2DistributionList.get(txName).get(paraIdentifier).add(distribution4);
//			 System.out.println(txName + " " + paraIdentifier + "\n" + distribution4);
			break;
		case 5:
			SequentialVcharParaDistribution distribution5 = countSequentialVcharParaDistribution(data, txName,
					paraIdentifier);
			distribution5.setColumnInfo(distTypeInfo.columnCardinality, distTypeInfo.minLength, distTypeInfo.maxLength,
					distTypeInfo.seedStrings);
			distribution5.setTime(windowTime);
			String[] priorHighFrequencyItems2 = (String[]) txName2ParaId2GeneHFItems.get(txName).get(paraIdentifier);
			distribution5.geneHighFrequencyItems(priorHighFrequencyItems2);
			txName2ParaId2GeneHFItems.get(txName).put(paraIdentifier, distribution5.getHighFrequencyItems());
			String[][] priorParaCandidates2 = (String[][]) txName2ParaId2ParaCandidates.get(txName).get(paraIdentifier);
			distribution5.geneCandidates(priorParaCandidates2);
			txName2ParaId2ParaCandidates.get(txName).put(paraIdentifier, distribution5.getCurrentParaCandidates());
			txName2ParaId2DistributionList.get(txName).get(paraIdentifier).add(distribution5);
			// System.out.println(txName + " " + paraIdentifier + "\n" + distribution5);
			break;
		default:
			System.err.println("尚不支持的数据分布类型！ -- " + distTypeInfo.distributionType);
		}

		// 数据采样 -- 支持全负载周期数据访问分布统计
		List<String> samplingData = txName2ParaId2SamplingData.get(txName).get(paraIdentifier);
		long cumulativeSize = txName2ParaId2CumulativeSize.get(txName).get(paraIdentifier);
		int samplingSize = Configurations.getSamplingSize();
		for (String item : data) {
			cumulativeSize++;
			if (cumulativeSize <= samplingSize) {
				samplingData.add(item);
			} else {
				if (Math.random() < samplingSize / cumulativeSize) {
					samplingData.set((int) (Math.random() * samplingSize), item);
				}
			}
		}
		txName2ParaId2CumulativeSize.get(txName).put(paraIdentifier, cumulativeSize);
		if (!txName2ParaId2DistributionType.get(txName).containsKey(paraIdentifier)) {
			txName2ParaId2DistributionType.get(txName).put(paraIdentifier, distTypeInfo);
		}
	}

	public static void countFullLifeCycleDistribution() {
		Iterator<Entry<String, Map<String, List<String>>>> iter1 = txName2ParaId2SamplingData.entrySet().iterator();
		while (iter1.hasNext()) {
			Entry<String, Map<String, List<String>>> entry1 = iter1.next();
			String txName = entry1.getKey();
			Map<String, List<String>> paraId2SamplingData = entry1.getValue();
			Iterator<Entry<String, List<String>>> iter2 = paraId2SamplingData.entrySet().iterator();
			while (iter2.hasNext()) {
				Entry<String, List<String>> entry2 = iter2.next();
				String paraIdentifier = entry2.getKey();
				List<String> data = entry2.getValue();
				DistributionTypeInfo distTypeInfo = txName2ParaId2DistributionType.get(txName).get(paraIdentifier);

				// bug fix: 有些事务模板可能没有实例数据，故distTypeInfo可能为空
				// txName2ParaId2DistributionType是在分析日志中维护得到的，而txName2ParaId2SamplingData在从事务模板输入中得来的
				if (distTypeInfo == null) {
					continue;
				}

				int distributionType = distTypeInfo.distributionType;
				int dataType = distTypeInfo.dataType;
				if (distributionType == 0 || distributionType == 3) {
					DataAccessDistribution distribution = countContinuousParaDistribution(dataType, data);
					txName2ParaId2FullLifeCycleDistribution.get(txName).put(paraIdentifier, distribution);
				} else if (distributionType == 1 || distributionType == 4) {
					IntegerParaDistribution distribution = countIntegerParaDistribution(data);
					distribution.setColumnInfo(distTypeInfo.columnMinValue, distTypeInfo.columnMaxValue,
							distTypeInfo.columnCardinality, distTypeInfo.coefficient);
					distribution.init4IntegerParaGene();
					txName2ParaId2FullLifeCycleDistribution.get(txName).put(paraIdentifier, distribution);
				} else if (distributionType == 2 || distributionType == 5) {
					VarcharParaDistribution distribution = countVarcharParaDistribution(data);
					distribution.setColumnInfo(distTypeInfo.columnCardinality, distTypeInfo.minLength,
							distTypeInfo.maxLength, distTypeInfo.seedStrings);
					distribution.init4VarcharParaGene();
					txName2ParaId2FullLifeCycleDistribution.get(txName).put(paraIdentifier, distribution);
				}
			}
		}
	}

	private static DataAccessDistribution countContinuousParaDistribution(int dataType, List<String> data) {
		// 三个分支中的代码非常相似，没办法~
		if (dataType == 0 || dataType == 3) { // Long（Integer、DateTime）
			List<Long> values = new ArrayList<>();
			for (int i = 0; i < data.size(); i++) {
				values.add(Long.parseLong(data.get(i)));
			}
			Long[] highFrequencyItems = new Long[Configurations.getHighFrequencyItemNum()];

			return getContinuousParaDistribution(values, highFrequencyItems);
		} else if (dataType == 1) { // Double
			List<Double> values = new ArrayList<>();
			for (int i = 0; i < data.size(); i++) {
				values.add(Double.parseDouble(data.get(i)));
			}
			Double[] highFrequencyItems = new Double[Configurations.getHighFrequencyItemNum()];

			return getContinuousParaDistribution(values, highFrequencyItems);
		} else if (dataType == 2) { // BigDecimal
			List<BigDecimal> values = new ArrayList<>();
			for (int i = 0; i < data.size(); i++) {
				values.add(new BigDecimal(data.get(i)));
			}
			BigDecimal[] highFrequencyItems = new BigDecimal[Configurations.getHighFrequencyItemNum()];

			return getContinuousParaDistribution(values, highFrequencyItems);
		} else {
			System.out.println("针对ContinuousParaDistribution尚不支持的数据类型！ -- " + dataType);
			return null;
		}


	}

	// 该函数中的代码与countContinuousParaDistribution函数中第一个分支的代码基本相同~
	private static IntegerParaDistribution countIntegerParaDistribution(List<String> data) {
		List<Long> values = new ArrayList<>();
		for (int i = 0; i < data.size(); i++) {
			values.add(Long.parseLong(data.get(i)));
		}
		Long windowMinValue = Collections.min(values);
		Long windowMaxValue = Collections.max(values);

		List<Entry<Long, Integer>> valueNumEntryList = getValueNumEntryList(values);

		double[] hFItemFrequencies = getHighFrequencyItemInfo(valueNumEntryList, values.size(), null);

		Object[] result = getIntervalCardiFrequInfo(valueNumEntryList, windowMaxValue, windowMinValue, values.size());
		long[] intervalCardinalities = (long[]) result[0];
		double[] intervalFrequencies = (double[]) result[1];

		ArrayList<ArrayList<Double>> quantilePerInterval = getQuantilePerInterval(valueNumEntryList, intervalCardinalities,
				intervalFrequencies, windowMaxValue, windowMinValue, values.size());

		return new IntegerParaDistribution(windowMinValue, windowMaxValue, hFItemFrequencies, intervalCardinalities,
				intervalFrequencies, quantilePerInterval);
	}

	private static VarcharParaDistribution countVarcharParaDistribution(List<String> data) {
		// data不需要数据类型的转化
		List<Entry<String, Integer>> valueNumEntryList = getValueNumEntryList(data);

		double[] hFItemFrequencies = getHighFrequencyItemInfo(valueNumEntryList, data.size(), null);

		Object[] result = getIntervalCardiFrequInfo(valueNumEntryList, data.size());
		long[] intervalCardinalities = (long[]) result[0];
		double[] intervalFrequencies = (double[]) result[1];

		// TODO hash code 分段的情况下要怎么统计频数？

		return new VarcharParaDistribution(hFItemFrequencies, intervalCardinalities, intervalFrequencies);
	}

	private static SequentialCtnsParaDistribution countSequentialCtnsParaDistribution(List<String> data, String txName,
			String paraIdentifier) {
		List<Long> values = new ArrayList<>();
		for (int i = 0; i < data.size(); i++) {
			values.add(Long.parseLong(data.get(i)));
		}
		Long minValue = Collections.min(values);
		Long maxValue = Collections.max(values);

		List<Entry<Long, Integer>> valueNumEntryList = getValueNumEntryList(values);

		Long[] highFrequencyItems = new Long[Configurations.getHighFrequencyItemNum()];
		double[] hFItemFrequencies = getHighFrequencyItemInfo(valueNumEntryList, values.size(), highFrequencyItems);

		// priorData是前一个时间窗口的参数数据，用来统计intervalParaRepeatRatios
		Object priorData = txName2ParaId2Data.get(txName).get(paraIdentifier);
		txName2ParaId2Data.get(txName).put(paraIdentifier, values);
		Object[] result = getIntervalCardiFrequInfo(valueNumEntryList, maxValue, minValue, values.size(), priorData);
		long[] intervalCardinalities = (long[]) result[0];
		double[] intervalFrequencies = (double[]) result[1];
		double[] intervalParaRepeatRatios = (double[]) result[2];

		// 仅仅是转化数据类型：Long[] -> long[]
		long[] highFrequencyItems2 = new long[highFrequencyItems.length];
		for (int i = 0; i < highFrequencyItems.length; i++) {
			if (highFrequencyItems[i] != null) {
				highFrequencyItems2[i] = highFrequencyItems[i];
			}
		}

		ArrayList<ArrayList<Double>> quantilePerInterval = getQuantilePerInterval(valueNumEntryList, intervalCardinalities,
				intervalFrequencies, maxValue, minValue, values.size());

		return new SequentialCtnsParaDistribution(minValue.longValue(), maxValue.longValue(), highFrequencyItems2,
				hFItemFrequencies, intervalCardinalities, intervalFrequencies, intervalParaRepeatRatios, quantilePerInterval);
	}

	private static SequentialIntParaDistribution countSequentialIntParaDistribution(List<String> data, String txName,
			String paraIdentifier) {
		List<Long> values = new ArrayList<>();
		for (int i = 0; i < data.size(); i++) {
			values.add(Long.parseLong(data.get(i)));
		}
		Long windowMinValue = Collections.min(values);
		Long windowMaxValue = Collections.max(values);

		List<Entry<Long, Integer>> valueNumEntryList = getValueNumEntryList(values);

		// 这里获取的高频项是用来统计hFItemRepeatRatio的。针对非键值整型属性参数的生成，高频项是由程序生成的，并非从日志中统计而来
		Long[] highFrequencyItems = new Long[Configurations.getHighFrequencyItemNum()];
		double[] hFItemFrequencies = getHighFrequencyItemInfo(valueNumEntryList, values.size(), highFrequencyItems);
//
//		System.out.println("打印出C_PAYMENT_CNT统计时的高频项");
//		System.out.println(highFrequencyItems);
//		System.out.println("****end*******");
		Object priorData = txName2ParaId2Data.get(txName).get(paraIdentifier);
		txName2ParaId2Data.get(txName).put(paraIdentifier, values);
		Object[] result = getIntervalCardiFrequInfo(valueNumEntryList, windowMaxValue, windowMinValue, values.size(),
				priorData);
		long[] intervalCardinalities = (long[]) result[0];
		double[] intervalFrequencies = (double[]) result[1];
		double[] intervalParaRepeatRatios = (double[]) result[2];

		// 上一个时间窗口 从日志中统计得到的高频项
		Long[] priorHighFrequencyItems = (Long[]) txName2ParaId2LogHFItems.get(txName).get(paraIdentifier);
		txName2ParaId2LogHFItems.get(txName).put(paraIdentifier, highFrequencyItems);

		int hFItemRepeatNum = 0, currentHFItemNum = 0;
		if (priorHighFrequencyItems != null) {
			for (int i = 0; i < highFrequencyItems.length; i++) {
				if (highFrequencyItems[i] != null) {
					currentHFItemNum++;
					for (int j = 0; j < priorHighFrequencyItems.length; j++) {
						if (priorHighFrequencyItems[j] != null
								&& priorHighFrequencyItems[j].longValue() == highFrequencyItems[i].longValue()) {
							hFItemRepeatNum++;
							break;
						}
					}
				}
			}
		}
		double hFItemRepeatRatio = 0;
		if (currentHFItemNum != 0) {
			hFItemRepeatRatio = (double) hFItemRepeatNum / currentHFItemNum;
		}

		ArrayList<ArrayList<Double>> quantilePerInterval = getQuantilePerInterval(valueNumEntryList, intervalCardinalities,
				intervalFrequencies, windowMaxValue, windowMinValue, values.size());

		return new SequentialIntParaDistribution(windowMinValue, windowMaxValue, hFItemFrequencies,
				intervalCardinalities, intervalFrequencies, intervalParaRepeatRatios, hFItemRepeatRatio, quantilePerInterval);
	}

	private static SequentialVcharParaDistribution countSequentialVcharParaDistribution(List<String> data,
			String txName, String paraIdentifier) {
		List<Entry<String, Integer>> valueNumEntryList = getValueNumEntryList(data);

		String[] highFrequencyItems = new String[Configurations.getHighFrequencyItemNum()];
		double[] hFItemFrequencies = getHighFrequencyItemInfo(valueNumEntryList, data.size(), highFrequencyItems);

		Object priorData = txName2ParaId2Data.get(txName).get(paraIdentifier);
		txName2ParaId2Data.get(txName).put(paraIdentifier, data);
		Object[] result = getIntervalCardiFrequInfo(valueNumEntryList, data.size(), priorData);
		long[] intervalCardinalities = (long[]) result[0];
		double[] intervalFrequencies = (double[]) result[1];
		double[] intervalParaRepeatRatios = (double[]) result[2];

		String[] priorHighFrequencyItems = (String[]) txName2ParaId2LogHFItems.get(txName).get(paraIdentifier);
		txName2ParaId2LogHFItems.get(txName).put(paraIdentifier, highFrequencyItems);

		int hFItemRepeatNum = 0, currentHFItemNum = 0;
		if (priorHighFrequencyItems != null) {
			for (int i = 0; i < highFrequencyItems.length; i++) {
				if (highFrequencyItems[i] != null) {
					currentHFItemNum++;
					for (int j = 0; j < priorHighFrequencyItems.length; j++) {
						if (priorHighFrequencyItems[j] != null
								&& priorHighFrequencyItems[j].equals(highFrequencyItems[i])) {
							hFItemRepeatNum++;
							break;
						}
					}
				}
			}
		}
		double hFItemRepeatRatio = 0;
		if (currentHFItemNum != 0) {
			hFItemRepeatRatio = (double) hFItemRepeatNum / currentHFItemNum;
		}

		return new SequentialVcharParaDistribution(hFItemFrequencies, intervalCardinalities, intervalFrequencies,
				intervalParaRepeatRatios, hFItemRepeatRatio);
	}

	private static <T extends Number> DataAccessDistribution getContinuousParaDistribution(List<T> values, T[] highFrequencyItems){
		T minValue = Collections.min(values, Comparator.comparing(o -> BigDecimal.valueOf(o.doubleValue())));
		T maxValue = Collections.max(values, Comparator.comparing(o -> BigDecimal.valueOf(o.doubleValue())));

		// Entry：<参数，参数出现的个数>；所有Entry按照出现个数升序排列
		List<Entry<T, Integer>> valueNumEntryList = getValueNumEntryList(values);

		// 因为一个函数无法有两个返回值，并且有些访问分布中不需要统计highFrequencyItems（此时传入一个null即可），故采用如下处理形式
		double[] hFItemFrequencies = getHighFrequencyItemInfo(valueNumEntryList, values.size(), highFrequencyItems);

		// Object[] result -> 为了可以有多个函数返回值~
		Object[] result = getIntervalCardiFrequInfo(valueNumEntryList, maxValue, minValue, values.size());
		long[] intervalCardinalities = (long[]) result[0];
		double[] intervalFrequencies = (double[]) result[1];

		ArrayList<ArrayList<Double>> quantilePerInterval = getQuantilePerInterval(valueNumEntryList, intervalCardinalities,
				intervalFrequencies, maxValue, minValue, values.size());
		return new ContinuousParaDistribution<T>(minValue, maxValue, highFrequencyItems, hFItemFrequencies,
				intervalCardinalities, intervalFrequencies, quantilePerInterval);
	}

	// 统计values中所有非重复值的出现个数，并按出现次数升序排列
	private static <T> List<Entry<T, Integer>> getValueNumEntryList(List<T> values) {
		Map<T, Integer> value2Num = new HashMap<>();
		for (int i = 0; i < values.size(); i++) {
			if (value2Num.containsKey(values.get(i))) {
				int num = value2Num.get(values.get(i)) + 1;
				value2Num.put(values.get(i), num);
			} else {
				value2Num.put(values.get(i), 1);
			}
		}

		Iterator<Entry<T, Integer>> iter = value2Num.entrySet().iterator();
		List<Entry<T, Integer>> valueNumEntryList = new ArrayList<>();
		while (iter.hasNext()) {
			valueNumEntryList.add(iter.next());
		}
		Collections.sort(valueNumEntryList, new EntryComparator<T>());

		return valueNumEntryList;
	}

	private static <T> double[] getHighFrequencyItemInfo(List<Entry<T, Integer>> valueNumEntryList, int valueSize,
			T[] highFrequencyItems) {
		int highFrequencyItemNum = Configurations.getHighFrequencyItemNum();
		double[] hFItemFrequencies = new double[highFrequencyItemNum];

		// valueNumEntryList中的参数是按出现次数升序存放的~
		for (int i = valueNumEntryList.size() - 1, j = 0; i >= 0 && j < highFrequencyItemNum; i--, j++) {
			if (highFrequencyItems != null) {
				highFrequencyItems[j] = valueNumEntryList.get(i).getKey();
			}
			hFItemFrequencies[j] = valueNumEntryList.get(i).getValue() / (double) valueSize;
			valueNumEntryList.remove(i);
		}

		return hFItemFrequencies;
	}

	private static <T extends Number> ArrayList<ArrayList<Double>> getQuantilePerInterval(List<Entry<T, Integer>> valueNumEntryList,
															long[] intervalCardinalities, double[]intervalFrequencies,
																						  T maxValue, T minValue, int valuesSize) {
		int intervalNum = Configurations.getIntervalNum();
		double avgIntervalLength = (maxValue.doubleValue() - minValue.doubleValue() + 0.000000001) / intervalNum;

		ArrayList<ArrayList<Double>> quantilePerInterval = new ArrayList<ArrayList<Double>>();
		int binNum = Configurations.getQuantileNum();
		// 单个区间内每段的容量
		double[] intervalFreqLength = new double[intervalNum];
		double[] cdfPerInterval = new double[intervalNum];
		for (int i = 0;i < intervalNum; ++i){
			intervalFreqLength[i] = (intervalFrequencies[i] / binNum);
			cdfPerInterval[i] = 0;
			quantilePerInterval.add(new ArrayList<Double>());
			quantilePerInterval.get(i).add(0.0);
		}
		// 按键值升序排列
		valueNumEntryList.sort(Comparator.comparingInt(o -> o.getKey().intValue()));


		//

		for (int i = 0; i < valueNumEntryList.size(); i++) {
			int idx = (int) ((valueNumEntryList.get(i).getKey().doubleValue() - minValue.doubleValue())
					/ avgIntervalLength);

			// bug fix: 同下面函数中的 bug fix 说明
			if (idx >= intervalCardinalities.length) {
				idx = intervalCardinalities.length - 1;
			}
			// -------------------
			// 在单个区间idx内，计算当前的分位点
			cdfPerInterval[idx] += valueNumEntryList.get(i).getValue() / (double) valuesSize;
			int freqInx = (int) (cdfPerInterval[idx] / intervalFreqLength[idx]);

			double idxBase = ((valueNumEntryList.get(i).getKey().doubleValue() - minValue.doubleValue())
					- idx * avgIntervalLength);
			double posInInterval = idxBase / avgIntervalLength;
			// 如果这个数占据极大的频数，可能会直接跳过某个分位点，这种情况下进行补齐，即认为跳过的分位点也是当前数值
			while (quantilePerInterval.get(idx).size() <= freqInx ){
				quantilePerInterval.get(idx).add(posInInterval);
			}
		}
		// 补齐最后一个段，其值必然是1
		for (int i = 0; i < intervalNum; i++){
			int freqInx = binNum;
			// 如果这个数占据极大的频数，可能会直接跳过某个分位点，这种情况下进行补齐，即认为跳过的分位点和前一个是一样的位置
			while (quantilePerInterval.get(i).size() < freqInx){
				quantilePerInterval.get(i).add(quantilePerInterval.get(i).get(quantilePerInterval.get(i).size() - 1));
			}
			if (quantilePerInterval.get(i).size() == freqInx){
				quantilePerInterval.get(i).add(1.0);
			}
		}

//		for (int i = 0; i < intervalNum; ++i){
//			System.out.printf("\n interval %d with bin num %d: ",i, binNum);
//
//			for (int j = 0; j < binNum; ++j) {
//				System.out.printf("%f, ",quantilePerInterval.get(i).get(j));
//			}
//
//		}

		return quantilePerInterval;
	}

	private static <T extends Number> Object[] getIntervalCardiFrequInfo(List<Entry<T, Integer>> valueNumEntryList,
			T maxValue, T minValue, int valueSize) {
		int intervalNum = Configurations.getIntervalNum();
		long[] intervalCardinalities = new long[intervalNum];
		double[] intervalFrequencies = new double[intervalNum];

		// 这里加 0.000000001 是为了避免针对maxValue的idx越界
		double avgIntervalLength = (maxValue.doubleValue() - minValue.doubleValue() + 0.000000001) / intervalNum;
		for (int i = 0; i < valueNumEntryList.size(); i++) {
			int idx = (int) ((valueNumEntryList.get(i).getKey().doubleValue() - minValue.doubleValue())
					/ avgIntervalLength);

			// bug fix: 同下面函数中的 bug fix 说明
			if (idx >= intervalCardinalities.length) {
				idx = intervalCardinalities.length - 1;
			}
			// -------------------

			intervalCardinalities[idx]++;
			intervalFrequencies[idx] += valueNumEntryList.get(i).getValue() / (double) valueSize;
		}

		Object[] result = new Object[2];
		result[0] = intervalCardinalities;
		result[1] = intervalFrequencies;
		return result;
	}

	@SuppressWarnings("unchecked")
	private static <T extends Number> Object[] getIntervalCardiFrequInfo(List<Entry<T, Integer>> valueNumEntryList,
			T maxValue, T minValue, int valueSize, Object priorData) {
		// priorData是前一个时间窗口的参数数据，用来统计intervalParaRepeatRatios
		Set<T> priorDataSet = new HashSet<T>();
		if (priorData != null) {
			priorDataSet.addAll((List<T>) priorData);
		}

		int intervalNum = Configurations.getIntervalNum();
		long[] intervalCardinalities = new long[intervalNum];
		double[] intervalFrequencies = new double[intervalNum];
		double[] intervalParaRepeatRatios = new double[intervalNum];

		double avgIntervalLength = (maxValue.doubleValue() - minValue.doubleValue() + 0.000000001) / intervalNum;
		for (int i = 0; i < valueNumEntryList.size(); i++) {
			int idx = (int) ((valueNumEntryList.get(i).getKey().doubleValue() - minValue.doubleValue())
					/ avgIntervalLength);

			// bug fix: idx 还是可能出现越界 （smallbank-mysql-sf20-tn20 实验时发现的，此时idx刚好等于区间数）
			// 按理说有上面的处理（" + 0.000000001"），应该不会有越界的情况了，狗头...
			if (idx >= intervalCardinalities.length) {
				idx = intervalCardinalities.length - 1;
			}
			// -------------------

			intervalCardinalities[idx]++;
			intervalFrequencies[idx] += valueNumEntryList.get(i).getValue() / (double) valueSize;

			if (priorDataSet.contains(valueNumEntryList.get(i).getKey())) {
				intervalParaRepeatRatios[idx] += valueNumEntryList.get(i).getValue() / (double) valueSize;
			}
		}

		for (int i = 0; i < intervalParaRepeatRatios.length; i++) {
			if (intervalFrequencies[i] == 0) {
				continue;
			}
			intervalParaRepeatRatios[i] /= intervalFrequencies[i];
		}

		Object[] result = new Object[3];
		result[0] = intervalCardinalities;
		result[1] = intervalFrequencies;
		result[2] = intervalParaRepeatRatios;
		return result;
	}

	private static Object[] getIntervalCardiFrequInfo(List<Entry<String, Integer>> valueNumEntryList, int valueSize) {

		int intervalNum = Configurations.getIntervalNum();
		long[] intervalCardinalities = new long[intervalNum];
		double[] intervalFrequencies = new double[intervalNum];

		// 对于字符串类型属性（参数），为了可用直方图来表示其数据分布，我们采用其hashcode进行数据分段
		for (int i = 0; i < valueNumEntryList.size(); i++) {
			int idx = Math.abs(valueNumEntryList.get(i).getKey().hashCode()) % intervalNum;
			intervalCardinalities[idx]++;
			intervalFrequencies[idx] += valueNumEntryList.get(i).getValue() / (double) valueSize;
		}

		Object[] result = new Object[2];
		result[0] = intervalCardinalities;
		result[1] = intervalFrequencies;
		return result;
	}

	@SuppressWarnings("unchecked")
	private static Object[] getIntervalCardiFrequInfo(List<Entry<String, Integer>> valueNumEntryList, int valueSize,
			Object priorData) {

		Set<String> priorDataSet = new HashSet<String>();
		if (priorData != null) {
			priorDataSet.addAll((List<String>) priorData);
		}

		int intervalNum = Configurations.getIntervalNum();
		long[] intervalCardinalities = new long[intervalNum];
		double[] intervalFrequencies = new double[intervalNum];
		double[] intervalParaRepeatRatios = new double[intervalNum];

		for (int i = 0; i < valueNumEntryList.size(); i++) {
			int idx = Math.abs(valueNumEntryList.get(i).getKey().hashCode()) % intervalNum;
			intervalCardinalities[idx]++;
			intervalFrequencies[idx] += valueNumEntryList.get(i).getValue() / (double) valueSize;

			if (priorDataSet.contains(valueNumEntryList.get(i).getKey())) {
				intervalParaRepeatRatios[idx] += valueNumEntryList.get(i).getValue() / (double) valueSize;
			}
		}

		for (int i = 0; i < intervalParaRepeatRatios.length; i++) {
			if (intervalFrequencies[i] == 0) {
				continue;
			}
			intervalParaRepeatRatios[i] /= intervalFrequencies[i];
		}

		Object[] result = new Object[3];
		result[0] = intervalCardinalities;
		result[1] = intervalFrequencies;
		result[2] = intervalParaRepeatRatios;
		return result;
	}

	//TODO: 应该就是这段代码的事情了！ 20201223
	// 转化txName2ParaId2DistributionList的数据结构以方便WorkloadGenerator使用
	public static List<Map<String, Map<String, DataAccessDistribution>>> getWindowDistributionList() {
		// 当前函数获取的workloadStartTime应与函数getTxName2ThroughputList获取的值一致
		long workloadStartTime = Long.MAX_VALUE;
//		System.out.println("I am in getWindowDistributionList");
		// allParaDistributionInfo：存放了所有事务所有参数的数据访问信息
		List<Vector<DataAccessDistribution>> allParaDistributionInfo = new ArrayList<>();
		// 存放与allParaDistributionInfo一一对应的标识符信息（含事务名和参数标识符）
		List<String> identifiers = new ArrayList<>();

		Iterator<Entry<String, Map<String, Vector<DataAccessDistribution>>>> iter1 = txName2ParaId2DistributionList
				.entrySet().iterator();
		while (iter1.hasNext()) {
			Entry<String, Map<String, Vector<DataAccessDistribution>>> entry1 = iter1.next();
			Iterator<Entry<String, Vector<DataAccessDistribution>>> iter2 = entry1.getValue().entrySet().iterator();
			while (iter2.hasNext()) {
				Entry<String, Vector<DataAccessDistribution>> entry2 = iter2.next();

				// 同一个参数的数据访问分布肯定由同一个线程按序分析得到的，故下面可不排序~
				// Collections.sort(entry2.getValue());

				//TODO: 有些参数没有访问分布，是[]空的，但是这里直接忽视掉，怀疑是这里导致了windowDistribution读到别人身上 ~
//				// bug fix: 事务模板中的某些事务可能没有实例数据
				if (entry2.getValue().size() == 0) {
					continue;
				}
				identifiers.add(entry1.getKey() + "##" + entry2.getKey());  //lyqu: txName##paraId

				if (entry2.getValue().get(0).getTime() < workloadStartTime) {
					workloadStartTime = entry2.getValue().get(0).getTime();
				}
				allParaDistributionInfo.add(entry2.getValue());
			}
		}
		

		// 待返回数据，list中的每一项就是某一个时间窗口的所有数据分布信息
		List<Map<String, Map<String, DataAccessDistribution>>> windowDistributionList = new ArrayList<>();
		// 标识每个参数的数据分布信息列表位置（可能在某些时间窗口上，没有某些参数的数据分布信息）
		int[] indexes = new int[allParaDistributionInfo.size()];
		Arrays.fill(indexes, 0); // 有没有无所谓~
		long currentTime = workloadStartTime;
		int timeWindowMillis = Configurations.getTimeWindowSize() * 1000;
//		System.out.println("I am in getWindowDistributionList~~~");
		while (true) {

			// 当前时间窗口中所有参数的数据分布信息
			Map<String, Map<String, DataAccessDistribution>> txName2ParaId2Distribution = new HashMap<>();
			boolean flag = true; // 标示数据分布信息是否已处理完
			for (int i = 0; i < allParaDistributionInfo.size(); i++) {
				if (allParaDistributionInfo.get(i).size() > indexes[i]) {  //qly: size()是统计该事务&参数的 有多少个时间窗口的访问分布
					DataAccessDistribution distribution = allParaDistributionInfo.get(i).get(indexes[i]);
					if (distribution.time == currentTime) {
						String[] arr = identifiers.get(i).split("##"); // arr[0]是事务名，arr[1]是参数标识符
						if (!txName2ParaId2Distribution.containsKey(arr[0])) {
							txName2ParaId2Distribution.put(arr[0], new HashMap<>());
						}
						txName2ParaId2Distribution.get(arr[0]).put(arr[1], distribution); //TODO:lyqu: 这里完全没有绑定！！！
						indexes[i]++;
					}
					flag = false;
				}
			}
			currentTime += timeWindowMillis;
			if (flag) {
				break; // 所有数据分布信息已处理完成
			}
			windowDistributionList.add(txName2ParaId2Distribution);
		}

//		System.out.println(windowDistributionList);
		return windowDistributionList;
	}

	// 需要保证所有事务在所有时间窗口上都有确定的事务吞吐
	public static Map<String, List<Integer>> getTxName2ThroughputList() {
		Map<String, List<Integer>> txName2ThroughputList2 = new HashMap<>();
		Iterator<Entry<String, Vector<Throughput>>> iter = txName2ThroughputList.entrySet().iterator();
		long workloadStartTime = Long.MAX_VALUE;
		while (iter.hasNext()) {
			Entry<String, Vector<Throughput>> entry = iter.next();
			// 同一个参数的数据访问分布肯定由同一个线程按序分析得到的，故下面可不排序~
			// Collections.sort(entry.getValue());

			// bug fix: 事务模板中的某些事务可能没有实例数据
			if (entry.getValue().size() == 0) {
				continue;
			}

			if (entry.getValue().get(0).time < workloadStartTime) {
				workloadStartTime = entry.getValue().get(0).time;
			}
		}
		// System.out.println("DistributionCounter.getTxName2ThroughputList ->
		// workloadStartTime: " + workloadStartTime);

		int timeWindowMillis = Configurations.getTimeWindowSize() * 1000;
		iter = txName2ThroughputList.entrySet().iterator();
		while (iter.hasNext()) {
			Entry<String, Vector<Throughput>> entry = iter.next();
			long currentTime = workloadStartTime;
			List<Integer> throughputList2 = new ArrayList<>();

//			System.out.println("处理的时候的throughput数： "+entry.getValue().size());
			for (int i = 0; i < entry.getValue().size(); i++) {
				if (entry.getValue().get(i).time == currentTime) {
					throughputList2.add(entry.getValue().get(i).throughput);
				} else {
					throughputList2.add(0);
					i--;
				}
				currentTime += timeWindowMillis;
			}
			txName2ThroughputList2.put(entry.getKey(), throughputList2);
		}

		return txName2ThroughputList2;
	}

	public static void deserialInit(DistributionCounter4Serial dcs) {

		txName2ParaId2DistributionList =dcs.txName2ParaId2DistributionList;
//		txName2ParaId2DistributionList = new HashMap<String, Map<String, Vector<DataAccessDistribution>>>();
//		Map<String, Map<String, Vector<ContinuousParaDistribution<Number>>>> tmpmap1 = dcs.txName2ParaId2DistributionList;
//		for (Entry<String, Map<String, Vector<ContinuousParaDistribution<Number>>>> entry1 : tmpmap1.entrySet()) {
//			Map<String, Vector<DataAccessDistribution>> mymap = new HashMap<String, Vector<DataAccessDistribution>>();
//			for (Entry<String, Vector<ContinuousParaDistribution<Number>>> entry2 : entry1.getValue().entrySet()) {
//				Vector<DataAccessDistribution> myvector = new Vector<DataAccessDistribution>();
//				for (DataAccessDistribution da : entry2.getValue()) {
//					myvector.add(da);
//				}
//				mymap.put(entry2.getKey(), myvector);
//			}
//			txName2ParaId2DistributionList.put(entry1.getKey(), mymap);
//		}

		txName2ThroughputList = dcs.txName2ThroughputList;

		txName2ParaId2FullLifeCycleDistribution= dcs.txName2ParaId2FullLifeCycleDistribution;

//		txName2ParaId2FullLifeCycleDistribution = new HashMap<String, Map<String, DataAccessDistribution>>();
//		Map<String, Map<String, ContinuousParaDistribution<Number>>> tmpmap2 = dcs.txName2ParaId2FullLifeCycleDistribution;
//		for (Entry<String, Map<String, ContinuousParaDistribution<Number>>> entry1 : tmpmap2.entrySet()) {
//			Map<String, DataAccessDistribution> mymap = new HashMap<String, DataAccessDistribution>();
//			for (Entry<String, ContinuousParaDistribution<Number>> entry2 : entry1.getValue().entrySet()) {
//				mymap.put(entry2.getKey(), entry2.getValue());
//			}
//			txName2ParaId2FullLifeCycleDistribution.put(entry1.getKey(), mymap);
//		}

	}

	public static Map<String, Vector<Throughput>> getOriginTxName2ThroughputList() {
		return txName2ThroughputList;
	}

	public static Map<String, Map<String, DataAccessDistribution>> getTxName2ParaId2GlobalDistribution() {
		return txName2ParaId2FullLifeCycleDistribution;
	}

	public static Map<String, Map<String, Vector<DataAccessDistribution>>> getTxName2ParaId2DistributionList() {
		return txName2ParaId2DistributionList;
	}

	public static List<Map<String, Map<String, DataAccessDistribution>>> windowDistributionAverage(List<Map<String, Map<String, DataAccessDistribution>>> windowDistributionList) {
		List<Map<String, Map<String, DataAccessDistribution>>> averageList = new ArrayList<>();
		averageList.add(windowDistributionList.get(0));

		for (int i = 1; i < windowDistributionList.size(); ++i){
			averageList.add(mergeDistribution(windowDistributionList.get(i), averageList.get(i-1),averageList.get(i-1)));
		}
		System.out.println("merge and");
		return averageList;
	}

	private static Map<String, Map<String, DataAccessDistribution>> mergeDistribution(Map<String, Map<String, DataAccessDistribution>> baseDistribution,
																					  Map<String, Map<String, DataAccessDistribution>> mergeDistribution,
																					  Map<String, Map<String, DataAccessDistribution>> oldDistribution) {
		Map<String, Map<String, DataAccessDistribution>> trueDistribution = new HashMap<>();
		double p = Configurations.getMergeWeight();
		int sum = 0;
		int mergeSum = 0;
		for (String txId : baseDistribution.keySet()){
			Map<String, DataAccessDistribution> txParaDistribution = new HashMap<>();
			for (String paraId : baseDistribution.get(txId).keySet()){
				DataAccessDistribution paraDistribution = baseDistribution.get(txId).get(paraId);
				sum ++;
				if (mergeDistribution.containsKey(txId) && mergeDistribution.get(txId).containsKey(paraId)){
					try {
						mergeSum++;
						paraDistribution = baseDistribution.get(txId).get(paraId).copy();
						paraDistribution.merge(mergeDistribution.get(txId).get(paraId), p);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}

				txParaDistribution.put(paraId, paraDistribution);
			}
			trueDistribution.put(txId, txParaDistribution);
		}
		System.out.printf("%d/%d%n",mergeSum,sum);
		return trueDistribution;
	}

	public void setTxName2ParaId2DistributionList
			(String txName, String paraIdentifier, long windowTime, IntegerParaDistribution distribution){
		distribution.setTime(windowTime);
		this.txName2ParaId2DistributionList.get(txName).get(paraIdentifier).add(distribution);
	}
	//txName2ParaId2DistributionList.get(txName).get(paraIdentifier).add(distribution1);
	public static Map<String, Map<String, DataAccessDistribution>> getTxName2ParaId2FullLifeCycleDistribution() {
		return txName2ParaId2FullLifeCycleDistribution;
	}

}

class EntryComparator<T> implements Comparator<Entry<T, Integer>> {

	@Override
	public int compare(Entry<T, Integer> o1, Entry<T, Integer> o2) {
		if (o1.getValue() < o2.getValue()) {
			return -1;
		} else if (o1.getValue() > o2.getValue()) {
			return 1;
		} else {
			return 0;
		}
	}
}
