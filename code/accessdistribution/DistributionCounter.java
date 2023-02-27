package accessdistribution;

import java.math.BigDecimal;
import java.util.*;
import java.util.Map.Entry;

import abstraction.Partition;
import abstraction.Table;
import abstraction.Transaction;
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
	// 保存最近一段时间窗口的候选参数集，事务名称 -> 参数标示符 -> 候选参数集
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

	private static Map<String, Map<String, Partition>> txName2ParaId2PartitionRule = null;

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
		txName2ParaId2PartitionRule = new HashMap<>();

		for (Entry<String, List<List<String>>> entry : txName2StatParameters.entrySet()) {
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
			txName2ParaId2PartitionRule.put(entry.getKey(), new HashMap<>());

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

	/**
	 * 构造访问分布
	 * @param txName 当前事务名
	 * @param paraIdentifier 当前参数名，operationId_No
	 * @param distTypeInfo 分布类型
	 * @param data 数据
	 * @return 构建得到的不含分区的访问分布
	 */
	private static DataAccessDistribution countDistribution(String txName, String paraIdentifier, DistributionTypeInfo distTypeInfo,
										  List<String> data){
		DataAccessDistribution distribution = null;
		switch (distTypeInfo.distributionType) {
			case 0:
				DataAccessDistribution distribution0 = countContinuousParaDistribution(distTypeInfo.dataType, data);
				assert distribution0 != null;
				distribution = distribution0;
				break;
			case 1:
				IntegerParaDistribution distribution1 = countIntegerParaDistribution(data);
				distribution1.setColumnInfo(distTypeInfo.columnMinValue, distTypeInfo.columnMaxValue,
						distTypeInfo.columnCardinality, distTypeInfo.coefficient);
				distribution1.init4IntegerParaGene();
				distribution = distribution1;
//			System.out.println(txName + " " + paraIdentifier + "\n" + distribution1);
				break;
			case 2:
				VarcharParaDistribution distribution2 = countVarcharParaDistribution(data);
				distribution2.setColumnInfo(distTypeInfo.columnCardinality, distTypeInfo.minLength, distTypeInfo.maxLength,
						distTypeInfo.seedStrings);
				distribution2.init4VarcharParaGene();
				distribution = distribution2;
				// System.out.println(txName + " " + paraIdentifier + "\n" + distribution2);
				break;
			case 3: // 分布3、4、5是有写盘需求的（暂不实现，目前全部存储在内存中） TODO
				SequentialCtnsParaDistribution distribution3 = countSequentialCtnsParaDistribution(data, txName,
						paraIdentifier);

				long[][] priorParaCandidates = (long[][]) txName2ParaId2ParaCandidates.get(txName).get(paraIdentifier);
				distribution3.geneCandidates(priorParaCandidates);
				txName2ParaId2ParaCandidates.get(txName).put(paraIdentifier, distribution3.getCurrentParaCandidates());

				distribution = distribution3;
				// System.out.println(txName + " " + paraIdentifier + "\n" + distribution3);
				break;
			case 4:
				SequentialIntParaDistribution distribution4 = countSequentialIntParaDistribution(data, txName,
						paraIdentifier);
				distribution4.setColumnInfo(distTypeInfo.columnMinValue, distTypeInfo.columnMaxValue,
						distTypeInfo.columnCardinality, distTypeInfo.coefficient);

				long[] priorHighFrequencyItems = (long[]) txName2ParaId2GeneHFItems.get(txName).get(paraIdentifier);
				distribution4.geneHighFrequencyItems(priorHighFrequencyItems);
				txName2ParaId2GeneHFItems.get(txName).put(paraIdentifier, distribution4.getHighFrequencyItems());

				priorParaCandidates = (long[][]) txName2ParaId2ParaCandidates.get(txName).get(paraIdentifier);
				distribution4.geneCandidates(priorParaCandidates);
				txName2ParaId2ParaCandidates.get(txName).put(paraIdentifier, distribution4.getCurrentParaCandidates());

				distribution = distribution4;
				break;
			case 5:
				SequentialVcharParaDistribution distribution5 = countSequentialVcharParaDistribution(data, txName,
						paraIdentifier);
				distribution5.setColumnInfo(distTypeInfo.columnCardinality, distTypeInfo.minLength, distTypeInfo.maxLength,
						distTypeInfo.seedStrings);

				String[] priorHighFrequencyItems2 = (String[]) txName2ParaId2GeneHFItems.get(txName).get(paraIdentifier);
				distribution5.geneHighFrequencyItems(priorHighFrequencyItems2);
				txName2ParaId2GeneHFItems.get(txName).put(paraIdentifier, distribution5.getHighFrequencyItems());

				String[][] priorParaCandidates2 = (String[][]) txName2ParaId2ParaCandidates.get(txName).get(paraIdentifier);
				distribution5.geneCandidates(priorParaCandidates2);
				txName2ParaId2ParaCandidates.get(txName).put(paraIdentifier, distribution5.getCurrentParaCandidates());

				distribution = distribution5;
				break;
			default:
				System.err.println("尚不支持的数据分布类型！ -- " + distTypeInfo.distributionType);
		}

		return distribution;
	}

	public static void count(String txName, String paraIdentifier, long windowTime, DistributionTypeInfo distTypeInfo,
			List<String> data) {

		if (data.size() == 0) {
			return;
		}

		Partition partition = txName2ParaId2PartitionRule.get(txName).get(paraIdentifier);

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

		data.removeIf(d -> d.equals("#@#"));
		if(data.isEmpty()){
//			txName2ParaId2DistributionList.get(txName).get(paraIdentifier).add(null);
			return;
		}

//		System.out.println(data);
		// 没有分区键或者不是数值类型的情况下，不建分区的分布
		if (partition == null || distTypeInfo.dataType >= 3){
			DataAccessDistribution distribution = countDistribution(txName, paraIdentifier, distTypeInfo, data);
			distribution.setTime(windowTime);
			txName2ParaId2DistributionList.get(txName).get(paraIdentifier).add(distribution);
		}
		else {
			DataAccessDistribution distribution = countMultiDistribution(partition,txName, paraIdentifier, distTypeInfo, data);
			distribution.setTime(windowTime);
			txName2ParaId2DistributionList.get(txName).get(paraIdentifier).add(distribution);
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
				if (Math.random() < 1.0* samplingSize / cumulativeSize) {
					samplingData.set((int) (Math.random() * samplingSize), item);
				}
			}
		}
		txName2ParaId2CumulativeSize.get(txName).put(paraIdentifier, cumulativeSize);
		if (!txName2ParaId2DistributionType.get(txName).containsKey(paraIdentifier)) {
			txName2ParaId2DistributionType.get(txName).put(paraIdentifier, distTypeInfo);
		}
	}

	/**
	 * 当前版本只支持int型
	 * @param partition
	 * @param txName
	 * @param paraIdentifier
	 * @param distTypeInfo
	 * @param data
	 * @return
	 */
	private static DataAccessDistribution countMultiDistribution(Partition<Long> partition,String txName, String paraIdentifier, DistributionTypeInfo distTypeInfo,List<String> data) {
		int length = partition.getLength();

		List<DataAccessDistribution> distributions = new ArrayList<>();
		double[] hFItemFrequencies = new double[0];
		long[] intervalCardinalities = new long[length];
		double[] intervalFrequencies = new double[length];

		// 如果不使用分区规则的话，全部放在第一个分区对应的直方图里
		if (!Configurations.isUsePartitionRule()){
			distributions.add(countDistribution(txName, paraIdentifier, distTypeInfo, data));
			intervalCardinalities[0] = getValueNumEntryList(data).size();
			intervalFrequencies[0] = 1;
			return new MultiPartitionDistribution<>(hFItemFrequencies, intervalCardinalities, intervalFrequencies, partition, distributions);
		}

		// 分到不同分区
		Map<String, List<String>> dataInPartition = new HashMap<>();
		for (String partitionName : partition.getPartitionNameList()){
			dataInPartition.put(partitionName,new ArrayList<>());
		}
		String partitionName = null;
		for (String d: data) {
			try {
				partitionName = partition.getPartition(Long.parseLong(d));
				dataInPartition.get(partitionName).add(d);
			}
			catch (Exception e){
				e.printStackTrace();
			}
		}

		for (int i = 0; i < length; i++) {
			partitionName = partition.getPartitionNameList().get(i);

			List<String> values = dataInPartition.get(partitionName);
			List<Entry<String, Integer>> valueNumEntryList = getValueNumEntryList(values);

			intervalCardinalities[i] = valueNumEntryList.size();

			int sum = 0;
			for (Entry<String, Integer> value : valueNumEntryList){
				sum += value.getValue();
			}
			intervalFrequencies[i] = sum;
			if (sum == 0){
				distributions.add(null);
			}
			else{
				distributions.add(countDistribution(txName, paraIdentifier, distTypeInfo, values));
			}

		}
		int tot = 0;
		for (int i = 0; i < length; i++) {
			tot += intervalFrequencies[i];
		}
		for (int i = 0; i < length; i++) {
			intervalFrequencies[i] /= tot;
		}

		return new MultiPartitionDistribution<>(hFItemFrequencies, intervalCardinalities, intervalFrequencies, partition, distributions);
	}

	public static void countFullLifeCycleDistribution() {
		for (Entry<String, Map<String, List<String>>> entry1 : txName2ParaId2SamplingData.entrySet()) {
			String txName = entry1.getKey();
			Map<String, List<String>> paraId2SamplingData = entry1.getValue();
			for (Entry<String, List<String>> entry2 : paraId2SamplingData.entrySet()) {
				String paraIdentifier = entry2.getKey();
				List<String> data = new ArrayList<>(entry2.getValue());
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
			for (String datum : data) {
				values.add(Long.parseLong(datum));
			}
			Long[] highFrequencyItems = new Long[Configurations.getHighFrequencyItemNum()];

			return getContinuousParaDistribution(values, highFrequencyItems);
		} else if (dataType == 1) { // Double
			List<Double> values = new ArrayList<>();
			for (String datum : data) {
				values.add(Double.parseDouble(datum));
			}
			Double[] highFrequencyItems = new Double[Configurations.getHighFrequencyItemNum()];

			return getContinuousParaDistribution(values, highFrequencyItems);
		} else if (dataType == 2) { // BigDecimal
			List<BigDecimal> values = new ArrayList<>();
			for (String datum : data) {
				values.add(new BigDecimal(datum));
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
		for (String datum : data) {
			values.add(Long.parseLong(datum));
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
		for (String datum : data) {
			values.add(Long.parseLong(datum));
		}
		Long minValue = Collections.min(values);
		Long maxValue = Collections.max(values);

		List<Entry<Long, Integer>> valueNumEntryList = getValueNumEntryList(values);

		Long[] highFrequencyItems = new Long[Configurations.getHighFrequencyItemNum()];
		double[] hFItemFrequencies = getHighFrequencyItemInfo(valueNumEntryList, values.size(), highFrequencyItems);

		// priorData是之前一段时间窗口的参数数据，用来统计intervalParaRepeatRatios
		ArrayList<Object> priorData = (ArrayList<Object>)txName2ParaId2Data.get(txName).get(paraIdentifier);
		int k = Configurations.getBackwardLength();
		if (priorData == null) priorData = new ArrayList<>();

		Object[] result = getIntervalCardiFrequInfo(valueNumEntryList, maxValue, minValue, values.size(), priorData);

		priorData.add(values);
		if (priorData.size() > k){
			priorData.subList(1,priorData.size());
		}
		txName2ParaId2Data.get(txName).put(paraIdentifier, priorData);

		long[] intervalCardinalities = (long[]) result[0];
		double[] intervalFrequencies = (double[]) result[1];
		double[][] intervalParaRepeatRatios = (double[][]) result[2];

		// 仅仅是转化数据类型：Long[] -> long[]
		long[] highFrequencyItems2 = new long[highFrequencyItems.length];
		for (int i = 0; i < highFrequencyItems.length; i++) {
			if (highFrequencyItems[i] != null) {
				highFrequencyItems2[i] = highFrequencyItems[i];
			}
		}

		ArrayList<ArrayList<Double>> quantilePerInterval = getQuantilePerInterval(valueNumEntryList, intervalCardinalities,
				intervalFrequencies, maxValue, minValue, values.size());

		return new SequentialCtnsParaDistribution(minValue, maxValue, highFrequencyItems2,
				hFItemFrequencies, intervalCardinalities, intervalFrequencies, intervalParaRepeatRatios, quantilePerInterval);
	}

	private static SequentialIntParaDistribution countSequentialIntParaDistribution(List<String> data, String txName,
			String paraIdentifier) {
		List<Long> values = new ArrayList<>();
		for (String datum : data) {
			values.add(Long.parseLong(datum));
		}
		Long windowMinValue = Collections.min(values);
		Long windowMaxValue = Collections.max(values);

		List<Entry<Long, Integer>> valueNumEntryList = getValueNumEntryList(values);

		// 这里获取的高频项是用来统计hFItemRepeatRatio的。针对非键值整型属性参数的生成，高频项是由程序生成的，并非从日志中统计而来
		Long[] highFrequencyItems = new Long[Configurations.getHighFrequencyItemNum()];
		double[] hFItemFrequencies = getHighFrequencyItemInfo(valueNumEntryList, values.size(), highFrequencyItems);
//
		// priorData是之前一段时间窗口的参数数据，用来统计intervalParaRepeatRatios
		ArrayList<Object> priorData = (ArrayList<Object>)txName2ParaId2Data.get(txName).get(paraIdentifier);
		int k = Configurations.getBackwardLength();
		if (priorData == null) priorData = new ArrayList<>();

		Object[] result = getIntervalCardiFrequInfo(valueNumEntryList, windowMaxValue, windowMinValue, values.size(), priorData);

		priorData.add(values);
		if (priorData.size() > k){
			priorData.subList(1,priorData.size());
		}
		txName2ParaId2Data.get(txName).put(paraIdentifier, priorData);
		long[] intervalCardinalities = (long[]) result[0];
		double[] intervalFrequencies = (double[]) result[1];
		double[][] intervalParaRepeatRatios = (double[][]) result[2];

		// 上一个时间窗口 从日志中统计得到的高频项
		Long[] priorHighFrequencyItems = (Long[]) txName2ParaId2LogHFItems.get(txName).get(paraIdentifier);
		txName2ParaId2LogHFItems.get(txName).put(paraIdentifier, highFrequencyItems);

		int hFItemRepeatNum = 0, currentHFItemNum = 0;
		if (priorHighFrequencyItems != null) {
			for (Long highFrequencyItem : highFrequencyItems) {
				if (highFrequencyItem != null) {
					currentHFItemNum++;
					for (Long priorHighFrequencyItem : priorHighFrequencyItems) {
						if (priorHighFrequencyItem != null
								&& priorHighFrequencyItem.longValue() == highFrequencyItem.longValue()) {
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
			for (String highFrequencyItem : highFrequencyItems) {
				if (highFrequencyItem != null) {
					currentHFItemNum++;
					for (String priorHighFrequencyItem : priorHighFrequencyItems) {
						if (priorHighFrequencyItem != null
								&& priorHighFrequencyItem.equals(highFrequencyItem)) {
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
		for (T value : values) {
			value2Num.put(value, value2Num.get(value) == null ? 1 : value2Num.get(value) + 1);
		}

		Iterator<Entry<T, Integer>> iter = value2Num.entrySet().iterator();
		List<Entry<T, Integer>> valueNumEntryList = new ArrayList<>();
		while (iter.hasNext()) {
			valueNumEntryList.add(iter.next());
		}
		valueNumEntryList.sort(new EntryComparator<T>());

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
			quantilePerInterval.add(new ArrayList<>());
			quantilePerInterval.get(i).add(0.0);
		}
		// 按键值升序排列
		valueNumEntryList.sort(Comparator.comparingInt(o -> o.getKey().intValue()));


		//

		for (Entry<T, Integer> tIntegerEntry : valueNumEntryList) {
			int idx = (int) ((tIntegerEntry.getKey().doubleValue() - minValue.doubleValue())
					/ avgIntervalLength);

			// bug fix: 同下面函数中的 bug fix 说明
			if (idx >= intervalCardinalities.length) {
				idx = intervalCardinalities.length - 1;
			}
			// -------------------
			// 在单个区间idx内，计算当前的分位点
			cdfPerInterval[idx] += tIntegerEntry.getValue() / (double) valuesSize;
			int freqInx = (int) (cdfPerInterval[idx] / intervalFreqLength[idx]);

			double idxBase = ((tIntegerEntry.getKey().doubleValue() - minValue.doubleValue())
					- idx * avgIntervalLength);
			double posInInterval = idxBase / avgIntervalLength;
			// 如果这个数占据极大的频数，可能会直接跳过某个分位点，这种情况下进行补齐，即认为跳过的分位点也是当前数值
			while (quantilePerInterval.get(idx).size() <= freqInx) {
				quantilePerInterval.get(idx).add(posInInterval);
			}
		}
		// 补齐最后一个段，其值必然是1
		for (int i = 0; i < intervalNum; i++){
			while (quantilePerInterval.get(i).size() <= binNum){
				quantilePerInterval.get(i).add(1.0);
			}
			quantilePerInterval.get(i).set(binNum, 1.0);
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
		for (Entry<T, Integer> tIntegerEntry : valueNumEntryList) {
			int idx = (int) ((tIntegerEntry.getKey().doubleValue() - minValue.doubleValue())
					/ avgIntervalLength);

			// bug fix: 同下面函数中的 bug fix 说明
			if (idx >= intervalCardinalities.length) {
				idx = intervalCardinalities.length - 1;
			}
			// -------------------

			intervalCardinalities[idx]++;
			intervalFrequencies[idx] += tIntegerEntry.getValue() / (double) valueSize;
		}

		Object[] result = new Object[2];
		result[0] = intervalCardinalities;
		result[1] = intervalFrequencies;
		return result;
	}

	@SuppressWarnings("unchecked")
	private static <T extends Number> Object[] getIntervalCardiFrequInfo(List<Entry<T, Integer>> valueNumEntryList,
			T maxValue, T minValue, int valueSize, Object priorData) {

		int intervalNum = Configurations.getIntervalNum();
		double avgIntervalLength = (maxValue.doubleValue() - minValue.doubleValue() + 0.000000001) / intervalNum;
		int k = Configurations.getBackwardLength();

		double[][] intervalParaRepeatRatios = null;

		ArrayList<Object> priorDataList = (ArrayList<Object>) priorData;
		if (priorDataList.size() > 0){
			intervalParaRepeatRatios = new double[Math.min(k, priorDataList.size())][];
		}

		// 统计已经重复的值，从而实现差分的统计
		Set<T> priorDataSet = new HashSet<T>();
		for (int i=priorDataList.size() - 1; i >= priorDataList.size() - k && i >= 0; i--){

			double[] intervalSum = new double[intervalNum];

			int index = (priorDataList.size() - 1 - i);

			Object pData = priorDataList.get(i);
			// pData是前面某个时间窗口的参数数据，用来统计intervalParaRepeatRatios，重复率是倒序存储的
			intervalParaRepeatRatios[index] = new double[intervalNum];
			Set<T> frontDataSet = new HashSet<T>((List<T>) pData);

			for (Entry<T, Integer> tIntegerEntry : valueNumEntryList) {
				T keyValue = tIntegerEntry.getKey() ;
				int idx = (int) ((keyValue.doubleValue() - minValue.doubleValue())
						/ avgIntervalLength);

				if (idx >= intervalNum) { // 处理越界
					idx = intervalNum - 1;
				}

				// cond1 如果当前统计的周期的数据和之前的时间窗口的数据重了，cond2 而且是新发现的重合值
				if (frontDataSet.contains(keyValue) && !priorDataSet.contains(keyValue)) {
					intervalParaRepeatRatios[index][idx] += tIntegerEntry.getValue();
				}

				intervalSum[idx] += tIntegerEntry.getValue();
			}
			priorDataSet.addAll((List<T>) pData);

			// 除以总数，变成重复率
			for (int j = 0; j < intervalNum; j++) {
				if (intervalSum[j] == 0) {
					continue;
				}
				intervalParaRepeatRatios[index][j] /= intervalSum[j];
			}

		}




		long[] intervalCardinalities = new long[intervalNum];
		double[] intervalFrequencies = new double[intervalNum];


		for (Entry<T, Integer> tIntegerEntry : valueNumEntryList) {
			int idx = (int) ((tIntegerEntry.getKey().doubleValue() - minValue.doubleValue())
					/ avgIntervalLength);

			// bug fix: idx 还是可能出现越界 （smallbank-mysql-sf20-tn20 实验时发现的，此时idx刚好等于区间数）
			// 按理说有上面的处理（" + 0.000000001"），应该不会有越界的情况了，狗头...
			if (idx >= intervalCardinalities.length) {
				idx = intervalCardinalities.length - 1;
			}
			// -------------------

			intervalCardinalities[idx]++;
			intervalFrequencies[idx] += tIntegerEntry.getValue() / (double) valueSize;
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
		for (Entry<String, Integer> stringIntegerEntry : valueNumEntryList) {
			int idx = Math.abs(stringIntegerEntry.getKey().hashCode()) % intervalNum;
			intervalCardinalities[idx]++;
			intervalFrequencies[idx] += stringIntegerEntry.getValue() / (double) valueSize;
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

		for (Entry<String, Integer> stringIntegerEntry : valueNumEntryList) {
			int idx = Math.abs(stringIntegerEntry.getKey().hashCode()) % intervalNum;
			intervalCardinalities[idx]++;
			intervalFrequencies[idx] += stringIntegerEntry.getValue() / (double) valueSize;

			if (priorDataSet.contains(stringIntegerEntry.getKey())) {
				intervalParaRepeatRatios[idx] += stringIntegerEntry.getValue() / (double) valueSize;
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

		for (Entry<String, Map<String, Vector<DataAccessDistribution>>> entry1 : txName2ParaId2DistributionList
				.entrySet()) {
			for (Entry<String, Vector<DataAccessDistribution>> entry2 : entry1.getValue().entrySet()) {
				// 同一个参数的数据访问分布肯定由同一个线程按序分析得到的，故下面可不排序~
				Collections.sort(entry2.getValue());

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
						txName2ParaId2Distribution.get(arr[0]).put(arr[1], distribution);
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

		long startTime = System.currentTimeMillis();
		List<Map<String, Map<String, DataAccessDistribution>>> averageList = new ArrayList<>();
		averageList.add(windowDistributionList.get(0));

		for (int i = 1; i < windowDistributionList.size(); ++i){
			averageList.add(mergeDistribution(i, windowDistributionList));
		}
//		System.out.println("merge and with time(ms):" + (System.currentTimeMillis() - startTime));
		return averageList;
	}

	private static Map<String, Map<String, DataAccessDistribution>> mergeDistribution(int baseDistributionPos,
																					  List<Map<String, Map<String, DataAccessDistribution>>> mergeDistributionList
																					  ) {
		Map<String, Map<String, DataAccessDistribution>> trueDistribution = new HashMap<>();
		int k = Configurations.getBackwardLength();
		// 当前的分布集合
		Map<String, Map<String, DataAccessDistribution>> baseDistribution = mergeDistributionList.get(baseDistributionPos);


		for (String txId : baseDistribution.keySet()){
			Map<String, DataAccessDistribution> txParaDistribution = new HashMap<>();
			for (String paraId : baseDistribution.get(txId).keySet()){ // 遍历每个参数的访问分布
				if (!(baseDistribution.get(txId).get(paraId) instanceof SequentialCtnsParaDistribution)){ // 只合并ctn的候选参数
					txParaDistribution.put(paraId, baseDistribution.get(txId).get(paraId));
					continue;
				}
				SequentialCtnsParaDistribution paraDistribution = (SequentialCtnsParaDistribution)baseDistribution.get(txId).get(paraId);

				// 初始化存储列表
				ArrayList<ArrayList<Long>> candidates = new ArrayList<>();
				for (int i = 0; i < paraDistribution.intervalNum; ++i){
					candidates.add(new ArrayList<>());
				}

				// 计算前k个区间
				for (int i = 0;i < k && i < baseDistributionPos ;i ++){
					int mergeDistPos = baseDistributionPos - i - 1;
					if (mergeDistPos < 0){
						continue;
					}

					Map<String, Map<String, DataAccessDistribution>> mergeDistribution = mergeDistributionList.get(mergeDistPos);
					if (mergeDistribution.containsKey(txId) && mergeDistribution.get(txId).containsKey(paraId)){
						SequentialCtnsParaDistribution mergeParaDistribution = (SequentialCtnsParaDistribution)mergeDistribution.get(txId).get(paraId);
						// 合并前面区间的候选参数
						candidates = paraDistribution.mergeCandidate(mergeParaDistribution.getCurrentParaCandidates(), candidates, i);
					}
				}

				// 转为数组
				long[][] newParaCandidates = new long[paraDistribution.intervalNum][];
				for (int i = 0; i < paraDistribution.intervalNum; ++i){
					newParaCandidates[i] = new long[candidates.get(i).size()];
					for (int j = 0; j < candidates.get(i).size(); ++j) {
						newParaCandidates[i][j] = candidates.get(i).get(j);
					}
				}

				// 更新结果
				paraDistribution.geneCandidates(newParaCandidates);
				txParaDistribution.put(paraId, paraDistribution);
			}
			trueDistribution.put(txId, txParaDistribution);
		}

		return trueDistribution;
	}

	public static void mapPara2PartitionRule(List<Table> tables, List<Transaction> transactions) {
		Map<String, Partition> tableName2PartitionRule = new HashMap<>();
		for (Table table: tables){
			tableName2PartitionRule.put(table.getName().toLowerCase(),table.getPartition());
		}

		for (Transaction transaction : transactions) {
			Map<String,String> paraId2Name = transaction.getParaId2Name();
			for (String paraId : paraId2Name.keySet()){
				int idx = paraId2Name.get(paraId).indexOf(Partition.PARA_SCHEMA_SEPARATOR);
				String tableName = paraId2Name.get(paraId).substring(0,idx);
				String columnName = paraId2Name.get(paraId).substring(idx+1);

				if (tableName2PartitionRule.get(tableName) != null &&
						tableName2PartitionRule.get(tableName).getPartitionKey().equals(columnName)){
					txName2ParaId2PartitionRule.get(transaction.getName()).put(paraId, tableName2PartitionRule.get(tableName));
				}
			}
		}
	}

	public void setTxName2ParaId2DistributionList
			(String txName, String paraIdentifier, long windowTime, IntegerParaDistribution distribution){
		distribution.setTime(windowTime);
		txName2ParaId2DistributionList.get(txName).get(paraIdentifier).add(distribution);
	}
	//txName2ParaId2DistributionList.get(txName).get(paraIdentifier).add(distribution1);
	public static Map<String, Map<String, DataAccessDistribution>> getTxName2ParaId2FullLifeCycleDistribution() {
		return txName2ParaId2FullLifeCycleDistribution;
	}

}

class EntryComparator<T> implements Comparator<Entry<T, Integer>> {

	@Override
	public int compare(Entry<T, Integer> o1, Entry<T, Integer> o2) {
		return o1.getValue().compareTo(o2.getValue());
	}
}
