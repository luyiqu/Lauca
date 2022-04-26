package transactionlogic;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.PropertyConfigurator;

import abstraction.Table;
import abstraction.Transaction;
import input.TableInfoSerializer;
import input.WorkloadReader;

/**
 * multiple关键字内的操作是多次执行的，其第一次执行时所有输入参数可根据事务逻辑和数据访问分布生成，后续的多次执行其输入参数的生成
 * 需要考虑multiple内操作多次执行的自身逻辑。目前multiple逻辑考虑两点：输入参数多次执行都保持不变；输入参数多次执行保持单调递
 * 增（或递减，每次增幅不一定为1）。当前multiple逻辑独立分析、独立维护。
 * multiple逻辑目前不考虑概率问题，主要是因为我们认为等于关系可以在一定程度上维持相应的事务逻辑。
 * 目前暂不考虑multiple结构（整个multiple操作块）处于分支语句中~ TODO
 */
public class MultipleLogicAnalyzer {

	/**
	 * @param txDataList：一个事务（同一个事务模板）的所有运行数据（多个事务实例数据）
	 * @return multipleLogicMapMerge：该事务上所有multiple块内操作的逻辑（只考虑输入参数的逻辑）
	 */
	@SuppressWarnings("unchecked")
	public Map<String, Double> obtainMultipleLogic(List<TransactionData> txDataList) {

		// Key："multiple"_operationId_"para"_paraIndex
		// Value：0表示该参数在multiple的多次执行中保持不变，n表示该参数在multiple的每次执行后单调增n
		// 统计时首先针对一个事务实例数据维护一个相对应的统计得到的multiple逻辑，即list中的map数据结构
		List<Map<String, Double>> multipleLogicMapList = new ArrayList<>();

		// 遍历每个事务实例数据，逐个分析其中的multiple逻辑
		for (TransactionData txData : txDataList) {
			// multipleLogicMap存储针对当前事务实例数据统计得到的multiple逻辑信息
			Map<String, Double> multipleLogicMap = new HashMap<>();

			int[] operationTypes = txData.getOperationTypes();
			Object[] operationDatas = txData.getOperationDatas();

			for (int i = 0; i < operationTypes.length; i++) {
				// multiple内的操作一般会有多次运行数据
				ArrayList<OperationData> multiOperationData = null;
				if (operationTypes[i] == -1 || operationTypes[i] == 0) {
					continue;
				} else if (operationTypes[i] == 1) {
					multiOperationData = (ArrayList<OperationData>)operationDatas[i];
					if (multiOperationData.size() == 0) {
						continue;
					}
				}
				int operationId = multiOperationData.get(0).getOperationId();
				int[] paraDataTypes = multiOperationData.get(0).getParaDataTypes();

				for (int j = 0; j < paraDataTypes.length; j++) {
					// 当multiple中操作仅执行一次，此时无法获取multiple逻辑，故需进行额外标记
					if (multiOperationData.size() == 1) {
						multipleLogicMap.put("multiple_" + operationId + "_para_" + j, Double.MAX_VALUE);
						continue;
					}

					Object lastParameter = multiOperationData.get(0).getParameters()[j];
					boolean equalFlag = true;
					boolean monotonousFlag = true;
					double[] increment = new double[1];
					increment[0] = Double.MIN_VALUE;
					for (int k = 1; k < multiOperationData.size(); k++) {
						if (!Util.isEqual(lastParameter, multiOperationData.get(k).getParameters()[j], paraDataTypes[j])) {
							equalFlag = false;
						}
						if (!Util.isMonotonous(lastParameter, multiOperationData.get(k).getParameters()[j], 
								paraDataTypes[j], increment)) {
							monotonousFlag = false;
						}
						lastParameter = multiOperationData.get(k).getParameters()[j];
					}

					if (equalFlag) {
						multipleLogicMap.put("multiple_" + operationId + "_para_" + j, 0d);
					}
					if (monotonousFlag && increment[0] != 0) {  //qly: 这里的increment[0]不应该不为Double.MIN_VALUE嘛？
						multipleLogicMap.put("multiple_" + operationId + "_para_" + j, increment[0]);
					}
				} // 遍历当前操作的所有参数
			} // 遍历当前事务的所有操作

			multipleLogicMapList.add(multipleLogicMap);
		} // 遍历所有事务实例数据

		// 前面我们已经根据每个事务实例统计得到一个multipleLogicMap，所有事务实例的multiple逻辑都存放在multipleLogicMapList中
		// 现在我们需要比对根据每个事务实例维护得到的multiple逻辑，若每个事务实例都符合这样的逻辑才输出
		// qly：这里跟线性依赖差不多 ~
		Map<String, Double> multipleLogicMapMerge = new HashMap<>();
		Set<String> noMultipleLogicSet = new HashSet<>();

		for (int i = 1; i < multipleLogicMapList.size(); i++) {
			Map<String, Double> tmpMap = multipleLogicMapList.get(i);
			Iterator<Entry<String, Double>> iter = tmpMap.entrySet().iterator();
			while (iter.hasNext()) {
				Entry<String, Double> entry = iter.next();
				if (noMultipleLogicSet.contains(entry.getKey())) {
					continue;
				}
				if (entry.getValue() != Double.MAX_VALUE) {
					if (multipleLogicMapMerge.containsKey(entry.getKey())) {
						if (multipleLogicMapMerge.get(entry.getKey()).doubleValue() != 
								entry.getValue().doubleValue()) {
							noMultipleLogicSet.add(entry.getKey());
							multipleLogicMapMerge.remove(entry.getKey());
						}
					} else {
						multipleLogicMapMerge.put(entry.getKey(), entry.getValue());
					}
				}
			}

			// 对于multipleLogicMapMerge中存在，但是当前tmpMap中不存在的multiple逻辑也需要删除~（删除了该部分代码）
		}
		// 特例分析：本来我们对于有些事务实例中未出现的multiple逻辑是需要去除的（有时存在，有时不存在），但是最终选择不去除。
		// 目前我们对于在多个事务实例中不一致的multiple逻辑是去除的。

//		System.out.println("MultipleLogicAnalyzer.obtainMultipleLogic -> multipleLogicMapMerge: \n\t" + multipleLogicMapMerge);
		return multipleLogicMapMerge;
	}

	public static void main(String[] args) {
		PropertyConfigurator.configure(".//lib//log4j.properties");
		TableInfoSerializer serializer = new TableInfoSerializer();
		List<Table> tables = serializer.read(new File(".//testdata//tables2.obj"));
		WorkloadReader workloadReader = new WorkloadReader(tables);
		List<Transaction> transactions = workloadReader.read(new File(".//testdata//tpcc-transactions.txt"));

		Preprocessor preprocessor = new Preprocessor();
		preprocessor.constructOperationTemplateAndDistInfo(transactions);
		
		RunningLogReader runningLogReader = new RunningLogReader(preprocessor.getTxName2OperationId2Template());
		runningLogReader.read(new File(".//testdata//output.txt"));

		MultipleLogicAnalyzer multipleLogicAnalyzer = new MultipleLogicAnalyzer();
		Iterator<Entry<String, List<TransactionData>>> iter = 
				runningLogReader.getTxName2TxDataList().entrySet().iterator();
		while (iter.hasNext()) {
			Entry<String, List<TransactionData>> entry = iter.next();
			System.out.println(entry.getKey() + ": " + entry.getValue().size());
			multipleLogicAnalyzer.obtainMultipleLogic(entry.getValue());
			System.out.println();
		}
	}
}
