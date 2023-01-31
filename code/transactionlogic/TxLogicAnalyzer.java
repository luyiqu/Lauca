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
import java.util.TreeMap;

import org.apache.log4j.PropertyConfigurator;

import abstraction.Table;
import abstraction.Transaction;
import config.Configurations;
import input.TableInfoSerializer;
import input.WorkloadReader;

public class TxLogicAnalyzer {

	// 事务逻辑的最终表示：parameterNodeMap中维护的是等于、包含和线性依赖关系；multipleLogicMap中维护的是multiple逻辑
	private Map<String, ParameterNode> parameterNodeMap = null;
	private Map<String, Double> multipleLogicMap = null;

	// 操作ID -> 平均执行次数，用于表示：if/else分支执行比例，multiple内操作平均执行次数（也算事务逻辑的一部分）
	private Map<Integer, Double> operationId2AvgRunTimes = null;

	// 统计每个列在该事务的参数中的基数，如果这个列有分区键，统计其分区的基数
	private Map<String, Integer> cardinality4paraInSchema = null;

	// 线性依赖关系分析时的配置参数，见LinearRelationAnalyzer
	private int minTxDataSize = 1;
	private int randomPairs = 10000;

	public void setMinTxDataSize(int minTxDataSize) {
		this.minTxDataSize = minTxDataSize;
	}

	public void setRandomPairs(int randomPairs) {
		this.randomPairs = randomPairs;
	}

	public void obtainTxLogic(List<TransactionData> txDataList, Map<Integer, List<String>> opId2paraSchema, List<Table> tables) {
		Map<Integer, Integer> operationId2ExecutionNum = countOperationExecutionNum(txDataList);
		parameterNodeMap = new HashMap<>();
		operationId2AvgRunTimes = new HashMap<>();

		// if/else分支执行比例，multiple内操作平均执行次数
		countIfElseMultipleExecutionInfo(txDataList);

		// 等于依赖关系
		EqualRelationAnalyzer equalRelationAnalyzer = new EqualRelationAnalyzer();
		Map<String, Map<String, Integer>> para2ParaOrResult2EqualCounter = equalRelationAnalyzer
				.countEqualInfo(txDataList);
		List<Entry<String, List<Entry<String, Double>>>> formatedEqualCounter = Util
				.convertCounter(para2ParaOrResult2EqualCounter, operationId2ExecutionNum);
//		System.out.println("ER\n"+formatedEqualCounter);

		List<Set<String>> identicalSets = equalRelationAnalyzer.obtainIdenticalSets(formatedEqualCounter);

		if (Configurations.isEqualRelationFlag()) {
			equalRelationAnalyzer.constructDependency(parameterNodeMap, formatedEqualCounter, identicalSets);
		}

		// 包含依赖关系
		IncludeRelationAnalyzer includeRelationAnalyzer = new IncludeRelationAnalyzer();
		Map<String, Map<String, Integer>> para2Result2IncludeCounter = includeRelationAnalyzer
				.countIncludeInfo(txDataList);
		List<Entry<String, List<Entry<String, Double>>>> formatedIncludeCounter = Util
				.convertCounter(para2Result2IncludeCounter, operationId2ExecutionNum);
		if (Configurations.isIncludeRelationFlag()) {
			includeRelationAnalyzer.constructDependency(parameterNodeMap, formatedIncludeCounter, identicalSets);
		}

		// 线性依赖关系
		LinearRelationAnalyzer linearRelationAnalyzer = new LinearRelationAnalyzer(minTxDataSize, randomPairs);
		Map<String, Coefficient> coefficientMap = linearRelationAnalyzer.countLinearInfo(txDataList);
		if (Configurations.isLinearRelationFlag()) {
			linearRelationAnalyzer.constructDependency(parameterNodeMap, coefficientMap);
		}

		// 初始化ParameterNode中的累计概率和数组~
		for (Entry<String, ParameterNode> stringParameterNodeEntry : parameterNodeMap.entrySet()) {
			stringParameterNodeEntry.getValue().initCumulativeProbabilities();
		}

		// multiple逻辑
		MultipleLogicAnalyzer multipleLogicAnalyzer = new MultipleLogicAnalyzer();
		if (Configurations.isMultipleLogicFlag()) {
			multipleLogicMap = multipleLogicAnalyzer.obtainMultipleLogic(txDataList);
		} else {
			multipleLogicMap = new HashMap<>();
		}

		// 统计基数约束
//		cardinality4paraInSchema = obtainCardinality(tables, txDataList, opId2paraSchema);

		// between and 逻辑
		// TODO


		//TODO 把存在依赖关系的参数值去掉




	}

	// 如果有分区键，基于分区键进行基数统计；否则直接统计
	private Map<String, Integer> obtainCardinality(List<Table> tables, List<TransactionData> txDataList, Map<Integer, List<String>> opId2paraSchema) {
		Map<String, List<Integer>> cardinality4paraInSchema = new HashMap<>();

		// 所有可能被用到的列
		Set<String> paraSchemaInfo = new HashSet<>();
		opId2paraSchema.values().forEach(paraSchemaInfo::addAll);
		paraSchemaInfo.forEach(para -> cardinality4paraInSchema.put(para,new ArrayList<>()));


		for (TransactionData txData : txDataList){
			int[] operationTypes = txData.getOperationTypes();
			Object[] operationDatas = txData.getOperationDatas();


			Map<String, Set<Object>> para4paraInSchema = new HashMap<>();

			paraSchemaInfo.forEach(para -> para4paraInSchema.put(para, new HashSet<>()));


			for (int i = 0; i < operationDatas.length; i++) {
				if (operationTypes[i] == 1) {// 是循环中的操作并执行了多次
					for (OperationData operationData : ((ArrayList<OperationData>) operationDatas[i])){
						int operationId = operationData.getOperationId();
						if (opId2paraSchema.containsKey(operationId)){
							addParaCardinality(tables, operationData, opId2paraSchema.get(operationId), para4paraInSchema);
						}
					}
				} else if (operationTypes[i] == 0) {// 不是循环中的操作或者只执行了一次
					OperationData operationData = (OperationData) operationDatas[i];
					int operationId = operationData.getOperationId();
					if (opId2paraSchema.containsKey(operationId)){
						addParaCardinality(tables, operationData, opId2paraSchema.get(operationId), para4paraInSchema);
					}
				}
			}

			for (String para : paraSchemaInfo){
				cardinality4paraInSchema.get(para).add(para4paraInSchema.get(para).size());
			}
		}

		// 获得平均的基数
		Map<String, Integer> ret = new HashMap<>();
		for (String para : paraSchemaInfo){
//			System.out.println(para+": "+ (cardinality4paraInSchema.get(para) ));
			double sum = cardinality4paraInSchema.get(para).stream().mapToInt(e->e).sum();
			ret.put(para, (int) (sum / txDataList.size()));

		}
//		System.out.println();
		return ret;
	}

	private void addParaCardinality(List<Table> tables, OperationData operationData, List<String> strings,
									Map<String, Set<Object>> para4paraInSchema) {
		int[] paraDataTypes = operationData.getParaDataTypes();

		assert (paraDataTypes.length != strings.size());

		for (int i = 0; i < paraDataTypes.length; i++){
			String para = strings.get(i);
			int idx = para.indexOf("@");
			String tableName = para.substring(0,idx);
			String columnName = para.substring(idx + 1);

			boolean isAdd = false;
			for (Table table : tables){
				if (table.getName().equals(tableName)){
					if (table.getPartition() != null && table.getPartition().getPartitionKey().equals(columnName)){
						String partitionName = table.getPartition().getPartition((Number) operationData.getParameters()[i]);
						para4paraInSchema.get(para).add(partitionName);
						isAdd = true;
					}
					break;
				}
			}

			if (!isAdd){
				para4paraInSchema.get(para).add(operationData.getParameters()[i]);
			}
		}
	}

	// 统计每个操作的总执行次数，算比例用的，所以循环中的只算一次
	@SuppressWarnings("unchecked")
	private Map<Integer, Integer> countOperationExecutionNum(List<TransactionData> txDataList) {
		Map<Integer, Integer> operationId2ExecutionNum = new HashMap<>();
		for (TransactionData txData : txDataList) {
			int[] operationTypes = txData.getOperationTypes();
			Object[] operationDatas = txData.getOperationDatas();
			for (int i = 0; i < operationDatas.length; i++) {
				OperationData operationData = null;
				if (operationTypes[i] == -1) {// 可能是未执行的分支
					continue;
				} else if (operationTypes[i] == 1) {// 是循环中的操作并执行了多次
					operationData = ((ArrayList<OperationData>) operationDatas[i]).get(0);
				} else if (operationTypes[i] == 0) {// 不是循环中的操作或者只执行了一次
					operationData = (OperationData) operationDatas[i];
				}
				int operationId = operationData.getOperationId();
				if (!operationId2ExecutionNum.containsKey(operationId)) {
					operationId2ExecutionNum.put(operationId, 1);
				} else {
					operationId2ExecutionNum.put(operationId, operationId2ExecutionNum.get(operationId) + 1);
				}
			}
		}
		return operationId2ExecutionNum;
	}


	/**
	 * 把存在依赖关系的参数值去掉,对于没有在事务逻辑中计算的参数才放入事务访问分布中进行计算
	 * @param txDataList
	 * @return 去掉特定参数值的txDataList\
	 * @调用 由
	 */
	private List<TransactionData> removeParaAlreadyInTransactionLogic(List<TransactionData> txDataList){


		return txDataList;
	}

	/**
	 * 计算每个操作在一个事务中平均会执行多少次，结果存放于operationId2AvgRunTimes
	 * @param txDataList 一个事务模板所有事务实例的数据
	 */
	private void countIfElseMultipleExecutionInfo(List<TransactionData> txDataList) {
		for (TransactionData txData : txDataList) {
			int[] operationTypes = txData.getOperationTypes();
			Object[] operationDatas = txData.getOperationDatas();
			for (int i = 0; i < operationDatas.length; i++) {
				if (!operationId2AvgRunTimes.containsKey(i + 1)) {
					operationId2AvgRunTimes.put(i + 1, 0d);
				} // 就这样吧~ 懒得提出去了

				if (operationTypes[i] == -1) {
					continue;
				} else if (operationTypes[i] == 1) {
					@SuppressWarnings("unchecked")
					double tmp = operationId2AvgRunTimes.get(i + 1)
							+ ((ArrayList<OperationData>) operationDatas[i]).size();
					operationId2AvgRunTimes.put(i + 1, tmp);
				} else if (operationTypes[i] == 0) {
					double tmp = operationId2AvgRunTimes.get(i + 1) + 1;
					operationId2AvgRunTimes.put(i + 1, tmp);
				}
			}
		}

		for (int i = 1; i <= operationId2AvgRunTimes.size(); i++) {
			double tmp = operationId2AvgRunTimes.get(i) / txDataList.size();
			operationId2AvgRunTimes.put(i, tmp);
		}

//		System.out.println("IfElseMultipleExecutionInfo: \n\t" + operationId2AvgRunTimes);
	}

	/**
	 * @param operationDataTemplateIter：事务信息模板，用于遍历所有操作的输入参数
	 * @return statParameters：需要统计数据访问分布的参数，其中依数据类型对参数进行了分类存储
	 */
	public List<List<String>> getStatParameters(Iterator<Entry<Integer, OperationData>> operationDataTemplateIter) {
		List<String> integerTypeParameters = new ArrayList<>();
		List<String> realTypeParameters = new ArrayList<>();
		List<String> decimalTypeParameters = new ArrayList<>();
		List<String> dateTypeParameters = new ArrayList<>();
		List<String> varcharTypeParameters = new ArrayList<>();
		List<String> booleanTypeParameters = new ArrayList<>();

		Set<String> identicalItems = new HashSet<>();
		// 针对每一条操作的处理
		while (operationDataTemplateIter.hasNext()) {
			OperationData template = operationDataTemplateIter.next().getValue();
			int operationId = template.getOperationId();
			int[] paraDataTypes = template.getParaDataTypes();
			for (int i = 0; i < paraDataTypes.length; i++) {
				String paraIdentifier = operationId + "_para_" + i;
				if (identicalItems.contains(paraIdentifier)) {
					// continue;
				}

				// 统计这个有什么意义？ （存疑）
				ParameterNode parameterNode = parameterNodeMap.get(paraIdentifier);
				if (parameterNode != null) { // bug fix：加了事务逻辑统计项配置参数后，parameterNode可能为空
					List<ParameterDependency> dependencies = parameterNode.getDependencies();
					double probabilitySum = 0;
					for (ParameterDependency tmp : dependencies) {
						probabilitySum += tmp.getProbability();
					}
					//TODO: 1-probabilitySum 去做access data distribution [qly]
					//qly: 这里是因为事务逻辑都把它统计的超好了，不需要再去统计访问分布了
					if (probabilitySum > 0.99999999) { // 这里考虑到double类型是非精确数值，在计算时可能有精度丢失
						// continue;
					}
				}

				// 关于 “// continue” 的说明 ：
				// 因为二级索引读无返回集（实际应用下的主键读写也可能这样），导致事务后面操作无法依据事务逻辑确定参数，所以所有参数的数据访问分布都进行统计 （存疑）

				if (paraDataTypes[i] == 0) {
					integerTypeParameters.add(paraIdentifier);
				} else if (paraDataTypes[i] == 1) {
					realTypeParameters.add(paraIdentifier);
				} else if (paraDataTypes[i] == 2) {
					decimalTypeParameters.add(paraIdentifier);
				} else if (paraDataTypes[i] == 3) {
					dateTypeParameters.add(paraIdentifier);
				} else if (paraDataTypes[i] == 4) {
					varcharTypeParameters.add(paraIdentifier);
				} else if (paraDataTypes[i] == 5) {
					booleanTypeParameters.add(paraIdentifier);
				}

				// bug fix：加了事务逻辑统计项配置参数后，parameterNode可能为空
				if (parameterNode != null) {
					identicalItems.addAll(parameterNode.getIdentifiers());  //qly: identicalItems只需要统计一遍即可
				} else {
					// identicalItems.add(paraIdentifier); // 其实无需加进去，仅遍历一次
				}
			}
		}

		List<List<String>> statParameters = new ArrayList<>();
		statParameters.add(integerTypeParameters);
		statParameters.add(realTypeParameters);
		statParameters.add(decimalTypeParameters);
		statParameters.add(dateTypeParameters);
		statParameters.add(varcharTypeParameters);
		statParameters.add(booleanTypeParameters);

//		System.out.println("TxLogicAnalyzer.getStatParameters -> statParameters: \n\t" + statParameters);
		return statParameters;
	}

	public Map<String, ParameterNode> getParameterNodeMap() {
		return parameterNodeMap;
	}

	public Map<String, Double> getMultipleLogicMap() {
		return multipleLogicMap;
	}

	public Map<Integer, Double> getOperationId2AvgRunTimes() {
		return operationId2AvgRunTimes;
	}

	public Map<String, Integer> getCardinality4paraInSchema() {
		return cardinality4paraInSchema;
	}

	public static void main(String[] args) {

		// 实验数据需要 - 20190615
		long startTime = System.currentTimeMillis();

		PropertyConfigurator.configure(".//lib//log4j.properties");
		TableInfoSerializer serializer = new TableInfoSerializer();
		List<Table> tables = serializer.read(new File(".//testdata//tables2.obj"));
		WorkloadReader workloadReader = new WorkloadReader(tables);
		List<Transaction> transactions = workloadReader.read(new File(".//testdata//tpcc-transactions.txt"));
		// System.out.println(transactions);

		Preprocessor preprocessor = new Preprocessor();
		preprocessor.constructOperationTemplateAndDistInfo(transactions);
		Map<String, Map<Integer, OperationData>> txName2OperationId2Template = preprocessor
				.getTxName2OperationId2Template();
		Map<String, Map<Integer, List<String>>> txName2OpId2paraSchema = preprocessor.getTxName2OperationId2paraSchema();


		RunningLogReader runningLogReader = new RunningLogReader(txName2OperationId2Template);
		runningLogReader.read(new File(".//testdata//log4txlogic"));

		TxLogicAnalyzer txLogicAnalyzer = new TxLogicAnalyzer();

		for (Entry<String, List<TransactionData>> entry : runningLogReader.getTxName2TxDataList().entrySet()) {
			System.out.println("###########################");
			System.out.println(entry.getKey() + ": " + entry.getValue().size());
			System.out.println("###########################");
			txLogicAnalyzer.obtainTxLogic(entry.getValue(), txName2OpId2paraSchema.get(entry.getKey()), tables);
			System.out.println("---------------------------");

			Map<Integer, OperationData> operationDataTemplates = txName2OperationId2Template.get(entry.getKey());
			Map<Integer, OperationData> sortedOperationDataTemplates = new TreeMap<>(operationDataTemplates);
			txLogicAnalyzer.getStatParameters(sortedOperationDataTemplates.entrySet().iterator());
			System.out.println("###########################");
			System.out.println();
		}

		// 实验数据需要 - 20190615
		System.out.println(System.currentTimeMillis() - startTime);
	}

}
