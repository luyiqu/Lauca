package transactionlogic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

public class IncludeRelationAnalyzer {

	/**
	 * @param txDataList: 一个事务（同一个事务模板）的所有运行数据（多个事务实例数据）
	 * @return para2Result2IncludeCounter：包含依赖关系的计数器
	 * @function: 统计事务中参数与返回结果集之间的包含依赖关系。大致策略是依次统计事务中所有操作的输入参数与前面操作的返回结果
	 *     集（返回结果集必须是多个tuple）之间的包含关联关系。同样，针对Multiple内部操作的多次执行数据，这里仅取其第一次执行的
	 *     数据供统计之用。输入参数的包含依赖关系对资源竞争强度，死锁发生的可能性和分布式事务的比例有一定的影响。
	 * @see EqualRelationAnalyzer，两者代码具有一定相似性
	 */
	@SuppressWarnings("unchecked")
	public Map<String, Map<String, Integer>> countIncludeInfo(List<TransactionData> txDataList) {

		// 包含依赖关系（或者叫包含关联关系）的计数器
		// 数据结构解释：当前参数的标识符 -> Map(前面操作返回结果集的标识符 -> 符合包含关系的次数，即计数器)
		// operationId_"para"_paraIndex -> Map(operationId_"result"_resultIndex -> counter)
		Map<String, Map<String, Integer>> para2Result2IncludeCounter = new HashMap<>();

		for (TransactionData txData : txDataList) {
			int[] operationTypes = txData.getOperationTypes();
			Object[] operationDatas = txData.getOperationDatas();

			for (int i = 0; i < operationDatas.length; i++) {

				OperationData operationData = null;
				if (operationTypes[i] == -1) {
					continue;
				} else if (operationTypes[i] == 1) {
					// 对于multiple中操作的多次运行数据，这里只取其第一次运行的数据作统计分析，同时假设multiple中操作至少运行一次
					operationData = ((ArrayList<OperationData>)operationDatas[i]).get(0);
				} else if (operationTypes[i] == 0) {
					operationData = (OperationData)operationDatas[i];
				}

				int operationId = operationData.getOperationId();
				int[] paraDataTypes = operationData.getParaDataTypes();
				Object[] parameters = operationData.getParameters();

				// 针对每个参数进行分析，统计其与前面操作中返回结果集（返回结果集必须是多条记录）之间的包含关联关系
				for (int j = 0; j < parameters.length; j++) {

					// 当前操作的输入参数
					Object parameter = parameters[j];
					int dataType = paraDataTypes[j];
					String paraIdentifier = operationId + "_para_" + j; // 当前参数的标识符
					if (!para2Result2IncludeCounter.containsKey(paraIdentifier)) {
						para2Result2IncludeCounter.put(paraIdentifier, new HashMap<>());
					}

					// 针对当前参数 前面操作的数据 依次进行遍历
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
						int[] frontReturnDataTypes = frontOperationData.getReturnDataTypes();

						// 当返回结果集包含多条记录时才需统计包含关系
						if (!frontOperationData.isFilterPrimaryKey()) {
							Object[][] returnItemsOfTuples = frontOperationData.getReturnItemsOfTuples();
							if (returnItemsOfTuples != null && returnItemsOfTuples.length != 0) {
								// 针对返回结果集中每列属性依次进行判断（是否存在包含关系）
								for (int m = 0; m < frontReturnDataTypes.length; m++) {
									if (Util.isContain(returnItemsOfTuples, m, frontReturnDataTypes[m], parameter, dataType)) {
										String resultIdentifier = frontOperationId + "_result_" + m;
										if (!para2Result2IncludeCounter.get(paraIdentifier).containsKey(resultIdentifier)) {
											para2Result2IncludeCounter.get(paraIdentifier).put(resultIdentifier, 1);
										} else {
											int tmp = para2Result2IncludeCounter.get(paraIdentifier).get(resultIdentifier);
											para2Result2IncludeCounter.get(paraIdentifier).put(resultIdentifier, tmp + 1);
										}
									}
								} // 遍历返回结果集中的所有列
							}
						} // if 非主键过滤

					} // 针对当前参数前面操作的数据的遍历
				} // 针对当前操作中所有参数的遍历
			} // 针对当前事务中所有操作的遍历
		} // 针对一类事务中所有事务实例数据的遍历

//		System.out.println("IncludeRelationAnalyzer.countIncludeInfo -> para2Result2IncludeCounter: \n\t" 
//				+ para2Result2IncludeCounter);
		return para2Result2IncludeCounter;
	}


	// 输入参数identicalSetList是指值完全相等的参数或返回结果集元素集合，与函数constructEqualDependency相同
	// 已知paraResultNode中的dependencies是按概率降序保持的
	
	/**
	 * @param parameterNodeMap：此时等于依赖关系必须已维护完成（等于、包含和线性依赖关系都会维护在ParameterNode中）
	 * @param formattedCounter：经格式化后的包含依赖关系的计数器（新计数器的特征：事务实例个数转化成相应比例 & 按照一定规则进行了排序）
	 * @param identicalSets：数值大小完全相等的输入参数和返回结果集元素的集合，该输入与
	 *     EqualRelationAnalyzer.constructDependency输入中的identicalSets完全相同。
	 * @function：依据formatedCounter中的统计信息 以及 identicalSets，构建参数与 返回结果集 之间的包含依赖关系。
	 */
	public void constructDependency(Map<String, ParameterNode> parameterNodeMap, 
			List<Entry<String, List<Entry<String, Double>>>> formattedCounter, List<Set<String>> identicalSets) {
		// 若两个参数完全相同，当前一个参数构建完包含依赖关系后，后一个参数无需再构建包含依赖关系（其值已完全确定）
		Set<String> identicalItems = new HashSet<>();

		for (Entry<String, List<Entry<String, Double>>> paraIncludeInfo : formattedCounter) {
			if (identicalItems.contains(paraIncludeInfo.getKey())) {
				// 前面有个参数与当前参数的值完全相同，当前参数的值已确定~ 无需维护包含依赖关系
				continue;
			}

			// 每个参数的ParameterNode都必然已存在，在函数EqualRelationAnalyzer.constructDependency中构建的
			// 下面获取之前已维护的等于依赖关系
			// List<ParameterDependency> dependencies = parameterNodeMap.get(paraIncludeInfo.getKey()).getDependencies();
			// bug fix: 添加了事务逻辑统计项控制参数之后，一个参数的ParameterNode可能不存在（没有统计等于关联关系）
			List<ParameterDependency> dependencies = null;
			if (parameterNodeMap.containsKey(paraIncludeInfo.getKey())) {
				dependencies = parameterNodeMap.get(paraIncludeInfo.getKey()).getDependencies();
			} else {
				List<String> identifiers = new ArrayList<String>();
				identifiers.add(paraIncludeInfo.getKey());
				ParameterNode parameterNode = new ParameterNode(identifiers);
				parameterNodeMap.put(paraIncludeInfo.getKey(), parameterNode);
				dependencies = new ArrayList<>();
				parameterNode.setDependencies(dependencies);
			}

			// paraDependencyInfo为当前参数上统计得到的包含依赖关系
			// 因为paraDependencyInfo上有更新操作，这样处理是不想破坏原有统计数据
			List<Entry<String, Double>> paraDependencyInfo = new ArrayList<>(paraIncludeInfo.getValue());

			double probabilitySum = 0; // 已维护依赖关系的概率之和
			for (ParameterDependency dependency : dependencies) {
				probabilitySum += dependency.getProbability();
			}

			List<ParameterDependency> appendedIncludeDependencies = new ArrayList<>();
			// 先遍历一遍，将可以直接添加的包含依赖关系找出来，对于这些包含依赖关系的添加是不需要替换掉原来的等于依赖关系的
			for (int j = 0; j < paraDependencyInfo.size(); j++) {
				Entry<String, Double> includeDependency = paraDependencyInfo.get(j);
				if (probabilitySum + includeDependency.getValue() <= 1) {
					appendedIncludeDependencies.add(
							new ParameterDependency(includeDependency.getKey(), includeDependency.getValue(), ParameterDependency.DependencyType.INCLUDE));
					probabilitySum += includeDependency.getValue();
					paraDependencyInfo.remove(j--);
				}
			}

			// 对于剩下的包含依赖关系，根据其关联关系强弱选择性地替换掉原来的等于依赖关系
			// 替换规则：关联概率probability大于原等于依赖关系的两倍，才可替换（前提是总依赖关系的概率和不超过1）
			// 选择这样的替换规则的原因是我们认为等于依赖关系相比包含依赖关系更重要~
			for (Entry<String, Double> includeDependency : paraDependencyInfo) {
				// 我们更看重等于依赖关系，替换时也从关联概率最小的等于关系替换
				probabilitySum = getProbabilitySum(dependencies, probabilitySum, includeDependency);
			}

			dependencies.addAll(appendedIncludeDependencies); // 等于关联概率逆序，包含关联概率先升序后逆序（大致情况）

			// 在上面的两个for循环中不作过滤的原因是我们不认为有两个返回结果集一直是完全相同的！
			// 下面将所有和该参数完全相等的参数集合一并存储在identicalItems中，以便后续过滤之用
			for (Set<String> tmpSet : identicalSets) {
				if (tmpSet.contains(paraIncludeInfo.getKey())) {
					identicalItems.addAll(tmpSet);
					break;
				}
			}
		}  //  for all paraIncludeInfo in formattedCounter

//		System.out.println("IncludeRelationAnalyzer.constructDependency -> parameterNodeMap: \n\t" + parameterNodeMap);
	}

	static double getProbabilitySum(List<ParameterDependency> dependencies, double probabilitySum, Entry<String, Double> includeDependency) {
		for (int k = dependencies.size() - 1; k >= 0; k--) {
			if (dependencies.get(k).getDependencyType() == ParameterDependency.DependencyType.EQUAL
					&& includeDependency.getValue() >= dependencies.get(k).getProbability() * 2
					&& probabilitySum - dependencies.get(k).getProbability() + includeDependency.getValue() <= 1.00000001) {
				dependencies.set(k, new ParameterDependency(includeDependency.getKey(), includeDependency.getValue(), ParameterDependency.DependencyType.INCLUDE));
				probabilitySum = probabilitySum - dependencies.get(k).getProbability() + includeDependency.getValue();
				break;
			}
		}
		return probabilitySum;
	}

}
