package transactionlogic;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

/**
 * 当前的代码实现与论文中有一点差异：目前仅考虑概率为100%的线性依赖关系，暂不考虑在一定概率下满足的线性依赖关系
 */
public class LinearRelationAnalyzer {

	// 支持线性依赖关系分析的最小事务实例数据量，不能特别小，建议100以上
	private int minTxDataSize = 100;
	// 随机事务实例对数，因为一组线性系数的计算至少需要一对事务实例。建议设置得稍微大点，比如10000
	private int randomPairs = 100000;

	public LinearRelationAnalyzer(int minTxDataSize, int randomPairs) {
		super();
		this.minTxDataSize = minTxDataSize;
		this.randomPairs = randomPairs;
	}

	/**
	 * @param txDataList：一个事务（同一个事务模板）的所有运行数据（多个事务实例数据）
	 * @return coefficientMap：保存了线性依赖关系的Map，key为标识符pair，value为线性系数
	 * @function 寻找参数与参数（或返回结果集元素）之间的线性依赖关系，这里的线性依赖关系是指：y=ax+b。 如果两个数据项（后一个数据项
	 *           必须是SQL参数，前一个数据项可以是SQL参数或SQL返回结果集元素）在所有事务实例中都保持相同的线性关系，
	 *           即系数a、b的值相同，那么我们认为这两个数据项存在线性关系。大致处理方案：随机选择一组（两个）事务实例数据
	 *           ->针对每个输入参数判断与前面操作的输入参数或返回结果集元素以及当前操作前面的参数之间是否存在线性关系
	 *           ->即计算系数a、b的值->判断与之前计算的系数a、b值是否相同
	 *           如果不同则该对数据项不存在线性关系，如果相同则pass。参数的线性依赖关系体现了一定的事务逻辑。
	 *           注意：当前的线性依赖关系必须是百分之百满足的，暂不考虑一定概率下满足的线性依赖关系（对事务性能影响不是那么大~）。
	 * 
	 */
	@SuppressWarnings("unchecked")
	public Map<String, Coefficient> countLinearInfo(List<TransactionData> txDataList) {
		if (txDataList == null || txDataList.size() < minTxDataSize) {
			System.out.println(
					"\tThe amount of transaction data is too small to support the analysis of linear relationships!"
							+ "\n\tThe number of transaction instances is " + txDataList.size()
							+ ", and the minTxDataSize is " + minTxDataSize + "!");
			return null;
		}

		// 记录不存在线性依赖关系的数据项对，Key的形式：currentIdentifier + "&" + priorIdentifier
		Set<String> noLinearRelation = new HashSet<>();
		// 记录计算过程中 数据项Pair 对应的线性关系系数，Key：currentIdentifier + "&" +
		// priorIdentifier；Value：线性关系的系数
		Map<String, Coefficient> coefficientMap = new HashMap<>();

		int randomPairsCount = 0;
		while (randomPairsCount++ < randomPairs) {
			// 随机选择两个事务实例数据，后面会根据这两个事务实例来计算线性关系系数
			TransactionData txData1 = txDataList.get((int) (txDataList.size() * Math.random()));
			TransactionData txData2 = txDataList.get((int) (txDataList.size() * Math.random()));
			int[] operationTypes1 = txData1.getOperationTypes();
			Object[] operationDatas1 = txData1.getOperationDatas();
			int[] operationTypes2 = txData2.getOperationTypes();
			Object[] operationDatas2 = txData2.getOperationDatas();

			for (int i = 0; i < operationTypes1.length; i++) {
				// 两个事务实例在该操作上的数据只要有一个为Null，就无法计算线性关系系数，故直接跳过去
				if (operationTypes1[i] == -1 || operationTypes2[i] == -1) {
					continue;
				}

				OperationData operationData1 = null, operationData2 = null;
				if (operationTypes1[i] == 1) {
					operationData1 = ((ArrayList<OperationData>) operationDatas1[i]).get(0);
				} else if (operationTypes1[i] == 0) {
					operationData1 = (OperationData) operationDatas1[i];
				}
				if (operationTypes2[i] == 1) {
					operationData2 = ((ArrayList<OperationData>) operationDatas2[i]).get(0);
				} else if (operationTypes2[i] == 0) {
					operationData2 = (OperationData) operationDatas2[i];
				}

				// 两操作的operationId和paraDataTypes必然相同
				int operationId = operationData1.getOperationId();
				int[] paraDataTypes = operationData1.getParaDataTypes();
				Object[] parameters1 = operationData1.getParameters();
				Object[] parameters2 = operationData2.getParameters();

				for (int j = 0; j < paraDataTypes.length; j++) {
					int dataType = paraDataTypes[j];
					Object parameter1 = parameters1[j];
					Object parameter2 = parameters2[j];
					String paraIdentifier = operationId + "_para_" + j;

					// 现在开始遍历前面操作的输入参数和返回结果集元素，以判断是否存在线性依赖关系
					for (int k = 0; k < i; k++) {
						// 两个事务实例在前面操作上的数据也都不能为null，不然无法计算线性关系的系数
						if (operationTypes1[k] == -1 || operationTypes2[k] == -1) {
							continue;
						}

						OperationData frontOperationData1 = null, frontOperationData2 = null;
						if (operationTypes1[k] == 1) {
							frontOperationData1 = ((ArrayList<OperationData>) operationDatas1[k]).get(0);
						} else if (operationTypes1[k] == 0) {
							frontOperationData1 = (OperationData) operationDatas1[k];
						}
						if (operationTypes2[k] == 1) {
							frontOperationData2 = ((ArrayList<OperationData>) operationDatas2[k]).get(0);
						} else if (operationTypes2[k] == 0) {
							frontOperationData2 = (OperationData) operationDatas2[k];
						}

						// ------ 与前面操作返回结果集元素之间的关系 ------
						// 同样，前面两个操作的 operationId & returnDataTypes 也必然相同
						int frontOperationId = frontOperationData1.getOperationId();
						int[] frontReturnDataTypes = frontOperationData1.getReturnDataTypes();
						if (frontOperationData1.isFilterPrimaryKey()) { // 若返回结果集是一个集合，则无需判断线性关系 qly： 为啥？存疑 ~
							Object[] returnItems1 = frontOperationData1.getReturnItems();
							Object[] returnItems2 = frontOperationData2.getReturnItems();
							if (returnItems1 == null || returnItems2 == null) {
								for (int m = 0; m < frontReturnDataTypes.length; m++) {
									String resultIdentifier = frontOperationId + "_result_" + m;
									String identifierPair = paraIdentifier + "&" + resultIdentifier;
									noLinearRelation.add(identifierPair);
									coefficientMap.remove(identifierPair);
								}
							} else {
								for (int m = 0; m < frontReturnDataTypes.length; m++) {  //qly: 是对应位置的线性关系吗
									String resultIdentifier = frontOperationId + "_result_" + m;
									String identifierPair = paraIdentifier + "&" + resultIdentifier;
									if (noLinearRelation.contains(identifierPair)) {
										continue;
									} else {
										maintainLinearRelation(noLinearRelation, coefficientMap, identifierPair,
												parameter1, parameter2, dataType, returnItems1[m], returnItems2[m],  //para1,2和return1,2位置都是对应的
												frontReturnDataTypes[m]);
									}
								}
							}
						} // ------

						// ------ 与前面操作的输入参数之间的关系 ------
						int[] frontParaDataTypes = frontOperationData1.getParaDataTypes();
						// 输入参数必然不会为Null（这里暂不考虑输入参数为空的特殊场景）
						Object[] frontParameters1 = frontOperationData1.getParameters();
						Object[] frontParameters2 = frontOperationData2.getParameters();
						for (int m = 0; m < frontParaDataTypes.length; m++) {
							String frontParaIdentifier = frontOperationId + "_para_" + m;
							String identifierPair = paraIdentifier + "&" + frontParaIdentifier;
							if (noLinearRelation.contains(identifierPair)) {
								continue;
							} else {
								maintainLinearRelation(noLinearRelation, coefficientMap, identifierPair, parameter1,
										parameter2, dataType, frontParameters1[m], frontParameters2[m],
										frontParaDataTypes[m]);
							}
						} // ------

					} // ------ 已判断完 与前面操作的数据项 之间的线性关系

					// ------ 与 当前操作 前面输入参数（同一个SQL中的参数） 之间的关系 ------
					for (int k = 0; k < j; k++) {
						String frontParaIdentifier = operationId + "_para_" + k;
						String identifierPair = paraIdentifier + "&" + frontParaIdentifier;
						if (noLinearRelation.contains(identifierPair)) {
							continue;
						} else {
							maintainLinearRelation(noLinearRelation, coefficientMap, identifierPair, parameter1,
									parameter2, dataType, parameters1[k], parameters2[k], paraDataTypes[k]);
						}
					} // ------

				} // 针对当前操作的所有输入参数进行遍历
			} // 针对所有操作进行遍历
		} // 循环随机选择一组事务示例

//		System.out.println("LinearRelationAnalyzer.countLinearInfo -> coefficientMap: \n\t" + coefficientMap);
		return coefficientMap;
	}

	/**
	 * @param parameterNodeMap：此时等于和包含依赖关系必须已维护完成（等于、包含和线性依赖关系都会维护在ParameterNode中）
	 * @param coefficientMap：保存了线性依赖关系的Map，key为标识符pair，value为线性系数。需要注意的是有些无效线性关系需要被过滤掉~
	 * @function：根据coefficientMap中统计的线性关联关系，构建输入参数与前面输入参数以及返回结果集元素之间的线性依赖关系
	 */
	public void constructDependency(Map<String, ParameterNode> parameterNodeMap,
			Map<String, Coefficient> coefficientMap) {

		Iterator<Entry<String, Coefficient>> iter = coefficientMap.entrySet().iterator();
		List<Entry<String, Coefficient>> validLinearRelations = new ArrayList<>();
		while (iter.hasNext()) {
			Entry<String, Coefficient> entry = iter.next();
			if (entry.getValue().a == 1 && entry.getValue().b == 0) { // 其实就是等于关系
				continue;
			}
			if (entry.getValue().a == 0) { // 后一个数据项是一个确定的数值，与前一个数据项并无关联关系 qly : 存疑 ~ 为啥要排除啊！
				continue;
			}
			validLinearRelations.add(entry);
		}

		// 将参数的线性依赖关系添加到 ParameterNode中
		for (int i = 0; i < validLinearRelations.size(); i++) {
			String identifierPair = validLinearRelations.get(i).getKey();
			String paraIdentifier = identifierPair.split("&")[0];

			ParameterNode node = parameterNodeMap.get(paraIdentifier);
			// List<ParameterDependency> dependencies = node.getDependencies();
			// bug fix：添加事务逻辑统计项控制参数
			List<ParameterDependency> dependencies = null;
			if (node == null) {
				List<String> identifiers = new ArrayList<String>();
				identifiers.add(paraIdentifier);
				node = new ParameterNode(identifiers);
				parameterNodeMap.put(paraIdentifier, node);
				dependencies = new ArrayList<>();
				node.setDependencies(dependencies);
			} else {
				dependencies = node.getDependencies();
			}

			boolean flag = true; // 当前线性依赖关系是否需要维护
			for (int j = 0; j < dependencies.size(); j++) {
				// 不需要维护的情况：原ParameterNode中存在依赖概率大于等于0.7的等于依赖关系（我们更重视等于依赖关系）
				if (dependencies.get(j).getDependencyType() == 0 && dependencies.get(j).getProbability() >= 0.7) {
					flag = false;
					break;
				}
			}
			if (flag) {
				String priorIdentifier = identifierPair.split("&")[1];
				Coefficient coefficient = validLinearRelations.get(i).getValue();
				if (node.getLinearDependencies() == null) {
					node.setLinearDependencies(new ArrayList<>());
				}
				// 可能添加的多个线性依赖关系本质上是一样的（依赖项是相等的 && 线性系数也一致），但是不影响程序的正确性，后面生成参数时随机选一个便好
				//qly: 也就是存在相同的 ~重复的多个线性依赖关系~ 但是在生成参数的时候线性依赖关系如何选择出来进去生成呢？ 存疑~
				node.getLinearDependencies()
						.add(new ParameterDependency(priorIdentifier, 1, 2, coefficient.a, coefficient.b));
			}
		}

//		System.out.println("LinearRelationAnalyzer.constructDependency -> parameterNodeMap: \n\t" + parameterNodeMap);
	}

	private void maintainLinearRelation(Set<String> noLinearRelation, Map<String, Coefficient> coefficientMap,
			String identifierPair, Object object1, Object object2, int dataType, Object frontObject1,
			Object frontObject2, int frontDataType) {
		Coefficient coefficient = calculateCoefficient(object1, object2, dataType, frontObject1, frontObject2,
				frontDataType);

		if (coefficient == null) {
			// 在数据类型上，两数据项不可能存在线性关系
			noLinearRelation.add(identifierPair);
		} else {
			// 两个事务实例在当前两个数据项上的数值相同，无法计算线性系数
			if (coefficient.a == Double.MAX_VALUE && coefficient.b == Double.MIN_VALUE) {
				return;
			}

			if (!coefficientMap.containsKey(identifierPair)) { // 第一次计算系数
				coefficientMap.put(identifierPair, coefficient);
			} else {
				if (!coefficientMap.get(identifierPair).equals(coefficient)) {
					noLinearRelation.add(identifierPair);
					coefficientMap.remove(identifierPair);
				}
			}
		}
	}

	private Coefficient calculateCoefficient(Object object1, Object object2, int dataType, Object frontObject1,
			Object frontObject2, int frontDataType) {
		// 数据类型只能为 0: integer(long); 1: real(double); 2: decimal(BigDecimal); 3:
		// datetime(millisecond -- long)
		if (dataType != 0 && dataType != 1 && dataType != 2 && dataType != 3) {
			return null;
		}
		if (frontDataType != 0 && frontDataType != 1 && frontDataType != 2 && frontDataType != 3) {
			return null;
		}

		double y1 = 0, y2 = 0, x1 = 0, x2 = 0;
		switch (dataType) {
		case 0:
		case 3:
			y1 = ((Long) object1).longValue();
			y2 = ((Long) object2).longValue();
			break;
		case 1:
			y1 = ((Double) object1).doubleValue();
			y2 = ((Double) object2).doubleValue();
			break;
		case 2:
			y1 = (new BigDecimal(object1.toString())).doubleValue();
			y2 = (new BigDecimal(object2.toString())).doubleValue();
			break;
		}
		switch (frontDataType) {
		case 0:
		case 3:
			x1 = ((Long) frontObject1).longValue();
			x2 = ((Long) frontObject2).longValue();
			break;
		case 1:
			x1 = ((Double) frontObject1).doubleValue();
			x2 = ((Double) frontObject2).doubleValue();
			break;
		case 2:
			x1 = (new BigDecimal(frontObject1.toString())).doubleValue();
			x2 = (new BigDecimal(frontObject2.toString())).doubleValue();
			break;
		}

		// 两个事务实例的数据可能相同，此时系数无法计算
		if (x1 == x2) {
			return new Coefficient(Double.MAX_VALUE, Double.MIN_VALUE);
		}

		double a = (y1 - y2) / (x1 - x2);
		double b = y1 - a * x1;
		return new Coefficient(a, b);
	}

}

//线性关系的系数
class Coefficient {

	double a, b;

	public Coefficient(double a, double b) {
		super();
		this.a = a;
		this.b = b;
	}

	public boolean equals(Coefficient other) {
		if (this.a == other.a && this.b == other.b) {
			return true;
		} else {
			return false;
		}
	}

	@Override
	public String toString() {
		return "Coefficient [a=" + a + ", b=" + b + "]";
	}
}
