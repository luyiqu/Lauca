package transactionlogic;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

public class EqualRelationAnalyzer {

	/**
	 * @param txDataList: 一个事务（同一个事务模板）的所有运行数据（多个事务实例数据）
	 * @return para2ParaOrResult2EqualCounter：等于依赖关系的计数器
	 * @function: 统计事务中参数以及返回结果集元素之间的等于依赖关系。目前假设参数之间是相互独立的，暂不考虑多参数的组合逻辑关系。
	 *     具体策略是依次统计事务中所有操作的输入参数与前面操作的输入参数和返回结果集元素以及当前操作前面的输入参数之间的等于关联关系。
	 *     针对Multiple内部操作的多次执行数据，这里仅取其第一次执行的数据供统计之用，其多次执行输入参数之间的关系会进行独立分析。
	 *     参数的等于关联（依赖）关系对于资源竞争强度，死锁发生的可能性和分布式事务的比例有很大的影响，是事务逻辑的重要组成部分。
	 */
	@SuppressWarnings("unchecked")
	public Map<String, Map<String, Integer>> countEqualInfo(List<TransactionData> txDataList) {
		// 等于依赖关系的计数器
		// 数据结构解释：当前参数的标识符 -> Map(前一个参数或返回结果集元素的标识符 -> 相等的次数，即计数器)
		// operationId_"para"_paraIndex -> Map(operationId_"para"/"result"_paraIndex/resultIndex -> counter)
		Map<String, Map<String, Integer>> para2ParaOrResult2EqualCounter = new HashMap<>();

		for (TransactionData txData : txDataList) {
			int[] operationTypes = txData.getOperationTypes();
			Object[] operationDatas = txData.getOperationDatas();

			for (int i = 0; i < operationDatas.length; i++) {

				OperationData operationData = null;
				if (operationTypes[i] == -1) {
					continue;
				} else if (operationTypes[i] == 1) {
					// 对于multiple中操作的多次运行数据，这里只取其第一次运行的数据作统计分析，同时假设multiple中操作至少运行一次
					//TODO: 只取第一次运行的数据分析应该是没问题的，但是在生成中却不能如此，还是要讲究这是第几次循环。20210102
					//TODO: 一般情况下，依赖只在本次循环中发生，这里不考虑第三次循环中某个参数依赖第二次循环中某个参数的情况，且这两个参数属于不同参数。
					operationData = ((ArrayList<OperationData>)operationDatas[i]).get(0);
				} else if (operationTypes[i] == 0) {
					operationData = (OperationData)operationDatas[i];
				}

				int operationId = operationData.getOperationId();
				int[] paraDataTypes = operationData.getParaDataTypes();
				Object[] parameters = operationData.getParameters();

				// 针对每个参数进行分析，统计其与 前面操作中的参数和返回结果集元素 以及 当前操作中前面的参数 之间的等于关联关系
				for (int j = 0; j < parameters.length; j++) {

					// 当前操作的输入参数
					Object parameter = parameters[j];
					int dataType = paraDataTypes[j];
					String paraIdentifier = operationId + "_para_" + j; // 当前参数的标识符
					if (!para2ParaOrResult2EqualCounter.containsKey(paraIdentifier)) {
						para2ParaOrResult2EqualCounter.put(paraIdentifier, new HashMap<>());
					}

					// 针对当前参数 前面操作的数据 依次进行分析
					for (int k = 0; k < i; k++) {
						OperationData frontOperationData = null;
						if (operationTypes[k] == -1) {
							continue;
						} else if (operationTypes[k] == 1) {
							frontOperationData = ((ArrayList<OperationData>)operationDatas[k]).get(0);
						} else if (operationTypes[k] == 0) {
							frontOperationData = (OperationData)operationDatas[k];
						}
						int frontOperationId = frontOperationData.getOperationId();

						// ------ 统计与前面操作返回结果集元素之间的等于关联关系 ------
						// 只有当返回结果集仅有一个tuple时，才需判断Equal关系。当返回结果集是多个tuple时，需判断的是Include关系
						if (frontOperationData.isFilterPrimaryKey()) {
							Object[] returnItems = frontOperationData.getReturnItems();
							if (returnItems != null) {
								// 针对返回结果tuple中的每一个属性依次进行判断是否相等
								int[] frontReturnDataTypes = frontOperationData.getReturnDataTypes();
								for (int m = 0; m < returnItems.length; m++) {
									String frontResultIdentifier = frontOperationId + "_result_" + m;

									addParaIfEqual(para2ParaOrResult2EqualCounter, parameter, dataType, paraIdentifier, returnItems[m], frontReturnDataTypes[m], frontResultIdentifier);
								} // for returnItems
							} // returnItems != null
						} // only one tuple

						// ------ 统计与前面操作输入参数之间的等于关联关系 ------
						int[] frontParaDataTypes = frontOperationData.getParaDataTypes();
						Object[] frontParameters = frontOperationData.getParameters();
						for (int m = 0; m < frontParameters.length; m++) {
							String frontParaIdentifier = frontOperationId + "_para_" + m;

							addParaIfEqual(para2ParaOrResult2EqualCounter, parameter, dataType, paraIdentifier, frontParameters[m], frontParaDataTypes[m], frontParaIdentifier);
						}

					} // 针对当前参数前面操作数据的遍历

					// ------ 统计当前参数与当前操作中前面输入参数（位于同一个SQL中）之间的等于关联关系 ------
					for (int k = 0; k < j; k++) {
						String frontParaIdentifier = operationId + "_para_" + k;

						addParaIfEqual(para2ParaOrResult2EqualCounter, parameter, dataType, paraIdentifier, parameters[k], paraDataTypes[k], frontParaIdentifier);
					}

				} // 针对当前操作中所有输入参数的遍历
			} // 针对当前事务中所有操作的遍历
		} // 针对一类事务中所有事务实例数据 的遍历

//		System.out.println("EqualRelationAnalyzer.countEqualInfo -> para2ParaOrResult2EqualCounter: \n\t" 
//				+ para2ParaOrResult2EqualCounter);
		return para2ParaOrResult2EqualCounter;
	}

	/**
	 * 检查当前参数与之前的参数是否相等，如果是，更新对应的计数器
	 *
	 * @param para2ParaOrResult2EqualCounter 参数相等的计数器
	 * @param parameter                      参数
	 * @param dataType                       参数的数据类型
	 * @param paraIdentifier                 参数的标识符
	 * @param frontItem                      之前的参数/返回值
	 * @param frontDataType                  之前参数的数据类型
	 * @param frontIdentifier 之前参数的标识符
	 */
	private void addParaIfEqual(Map<String, Map<String, Integer>> para2ParaOrResult2EqualCounter, Object parameter, int dataType,
								String paraIdentifier, Object frontItem, int frontDataType, String frontIdentifier) {
		if (Util.isEqual(frontItem, frontDataType, parameter, dataType)) {

			if (!para2ParaOrResult2EqualCounter.get(paraIdentifier).containsKey(frontIdentifier)) {
				para2ParaOrResult2EqualCounter.get(paraIdentifier).put(frontIdentifier, 1);
			} else {
				int tmp = para2ParaOrResult2EqualCounter.get(paraIdentifier).get(frontIdentifier);
				para2ParaOrResult2EqualCounter.get(paraIdentifier).put(frontIdentifier, tmp + 1);
			}
		}
	}

	/**
	 * @param formatedCounter：经格式化后的等于依赖关系的计数器（新计数器的特征：事务实例个数转化成相应比例 & 按照一定规则进行了排序）
	 * @return identicalSets：数值大小完全相等的输入参数和返回结果集元素的集合，即等于依赖关系的比例为1。可能存在多个这样的集合。
	 */
	public List<Set<String>> obtainIdenticalSets(List<Entry<String, List<Entry<String, Double>>>> formatedCounter) {
		// return的数据结构
		List<Set<String>> identicalSets = new ArrayList<>();

		List<String> tmpIdenticalSet = new ArrayList<>();
		for (Entry<String, List<Entry<String, Double>>> stringListEntry : formatedCounter) {
			tmpIdenticalSet.clear();
			tmpIdenticalSet.add(stringListEntry.getKey());
			for (Entry<String, Double> entry : stringListEntry.getValue()) {
				if (entry.getValue() >= 1) { //== 1? >= 0.9999999999?
					tmpIdenticalSet.add(entry.getKey());
				}
			}

			if (tmpIdenticalSet.size() == 1) {
				continue;
			} else {
				// 标示tmpIdenticalSet这个集合中的元素是否已属于之前某个已添加的集合
				boolean flag = false;
				loop:
				for (Set<String> tmpSet : identicalSets) {
					for (String item : tmpIdenticalSet) {
						if (tmpSet.contains(item)) {
							tmpSet.addAll(tmpIdenticalSet);
							flag = true;
							break loop;
						}
					}
				}

				if (!flag) {
					Set<String> identicalSet = new HashSet<>(tmpIdenticalSet);
					identicalSets.add(identicalSet);
				}
			}
		} // for formatedCounter中每一个输入参数的统计信息

//		System.out.println("EqualRelationAnalyzer.obtainIdenticalSets -> identicalSets: \n\t" + identicalSets);
		return identicalSets;
	}

	/**
	 * @param parameterNodeMap：该参数必须已实例化（等于、包含和线性依赖关系都会维护在ParameterNode中）
	 * @param formattedCounter：经格式化后的等于依赖关系的计数器（新计数器的特征：事务实例个数转化成相应比例 & 按照一定规则进行了排序）
	 * @param identicalSets：数值大小完全相等的输入参数和返回结果集元素的集合
	 * @function：依据formatedCounter中的统计信息 以及 identicalSets，构建参数与 参数以及返回结果集元素 之间的等于依赖关系。
	 */
	public void constructDependency(Map<String, ParameterNode> parameterNodeMap, 
			List<Entry<String, List<Entry<String, Double>>>> formattedCounter, List<Set<String>> identicalSets) {

		// 因为formattedCounter中的输入参数（即entry.key）是有序的，所以下面的顺序遍历相当于从事务中的前面参数向后面参数依次处理
		for (Entry<String, List<Entry<String, Double>>> paraEqualInfo : formattedCounter) {

			if (parameterNodeMap.containsKey(paraEqualInfo.getKey())) {
				// 这个参数必然与前面某个参数的值完全相同，故可直接pass过去
				continue;
			}

			// 为当前参数构建一个ParameterNode。在构建ParameterNode之前，我们需要查看当前参数是否属于某个完全值相等的参数集合中（集合中
			//     可能含返回结果集元素），若属于则将整个集合看成一个整体，构建成一个节点。
			boolean flag = false; // 标示当前参数是否属于某个值完全相等的集合中
			for (Set<String> identicalSet : identicalSets) {

				if (identicalSet.contains(paraEqualInfo.getKey())) {
					Iterator<String> iter = identicalSet.iterator();
					// 提取出集合中的所有参数标识符
					List<String> paraIdentifierList = new ArrayList<>();
					while (iter.hasNext()) {
						String tmpIdentifier = iter.next();
						if (tmpIdentifier.contains("_para_")) {
							paraIdentifierList.add(tmpIdentifier);
						}
					}

					paraIdentifierList.sort((o1, o2) -> {
						int operationId1 = Integer.parseInt(o1.split("_")[0]);
						int operationId2 = Integer.parseInt(o2.split("_")[0]);
						int paraIndex1 = Integer.parseInt(o1.split("_")[2]);
						int paraIndex2 = Integer.parseInt(o2.split("_")[2]);

						if (operationId1 < operationId2) {
							return -1;
						} else if (operationId1 > operationId2) {
							return 1;
						} else {
							return Integer.compare(paraIndex1, paraIndex2);
						}
					}); // 这个排序在参数生成时会利用到~
					ParameterNode parameterNode = new ParameterNode(paraIdentifierList);

					// 对于集合中的所有参数标识符都构建相应的映射关系，以便后续可根据任一参数标识符寻找相应的ParameterNode
					for (String s : paraIdentifierList) {
						parameterNodeMap.put(s, parameterNode);
					}

					flag = true;
					break;
				}
			} // for identicalSets

			if (!flag) { // 该参数不属于某个完全值相等的集合
				List<String> identifiers = new ArrayList<String>();
				identifiers.add(paraEqualInfo.getKey());
				ParameterNode parameterNode = new ParameterNode(identifiers);
				parameterNodeMap.put(paraEqualInfo.getKey(), parameterNode);
			}
			// ------ 至此，已为当前参数构建好ParameterNode，并存放在parameterNodeMap中

			// ------ 后面将为当前参数构建其等于依赖关系，即设置parameterNode中的dependencies
			List<ParameterDependency> dependencies = new ArrayList<>();
			// paraDependencyInfo中的关联概率大小是倒序存放的
			List<Entry<String, Double>> paraDependencyInfo = paraEqualInfo.getValue();

			// 注意：对于一个值完全相等的集合中的任一元素构建依赖关系后，对于该集合中的其他元素则不需要再构建依赖关系
			// 所以我们需要对paraDependencyInfo的依赖对象进行过滤。若多个依赖对象属于某个值完全相等的集合，则仅需保留其中一个
			List<Entry<String, Double>> filteredParaDependencyInfo = new ArrayList<>();
			Set<String> identicalItems = new HashSet<>();
			for (Entry<String, Double> entry : paraDependencyInfo) {
				if (identicalItems.contains(entry.getKey())) {
					continue;
				}
				filteredParaDependencyInfo.add(entry);
				for (Set<String> tmpSet : identicalSets) {
					if (tmpSet.contains(entry.getKey())) {
						identicalItems.addAll(tmpSet);
						break;
					}
				}
			}

			// 寻找一个概率和最大的依赖关系组合，总依赖概率之和需小于等于1。目前这里采用的是贪心解法，并不是最优解~ TODO
			// maxGroup中的依赖概率也是逆序的
			List<Entry<String, Double>> maxGroup = findOptimalGroup(filteredParaDependencyInfo);
			for (Entry<String, Double> stringDoubleEntry : maxGroup) {
				ParameterDependency dependency = new ParameterDependency(stringDoubleEntry.getKey(),
						stringDoubleEntry.getValue(), ParameterDependency.DependencyType.EQUAL);
				dependencies.add(dependency);
			}

			parameterNodeMap.get(paraEqualInfo.getKey()).setDependencies(dependencies);

		} //  for all paraEqualInfo in formattedCounter

//		System.out.println("EqualRelationAnalyzer.constructDependency -> parameterNodeMap: \n\t" + parameterNodeMap);
	}

	/**
	 * @param filteredParaDependencyInfo：经过过滤的某个输入参数的所有等于依赖关系列表（此时依赖的对象不会是完全值相等的）
	 * @return maxGroup：一个概率和最大（概率和需小于等于1）的依赖关系组合
	 * @function：目前该函数采用的是贪心解法，并非最优解。大致策略：选择一个起始点，不断从概率大的依赖项向概率小的依赖项遍历，只要当前依赖项
	 *     的添加未使组合的概率和大于1，就将其添加到组合中，直至遍历完所有依赖项。同时起始点的位置不断从index=0向后移动~
	 */
	private List<Entry<String, Double>> findOptimalGroup(List<Entry<String, Double>> filteredParaDependencyInfo) {
		List<Entry<String, Double>> group = new ArrayList<>();
		List<Entry<String, Double>> maxGroup = new ArrayList<>();
		double sum, maxSum = 0;

		for (int i = 0; i < filteredParaDependencyInfo.size(); i++) {
			group.clear();
			sum = 0;
			for (int j = i; j < filteredParaDependencyInfo.size(); j++) {
				Entry<String, Double> entry = filteredParaDependencyInfo.get(j);
				if (sum + entry.getValue() <= 1) {
					sum += entry.getValue();
					group.add(entry);
				}
			}
			if (sum > maxSum) {
				maxSum = sum;
				maxGroup.clear();
				maxGroup.addAll(group);
			}
		}
		return maxGroup;
	}

}
