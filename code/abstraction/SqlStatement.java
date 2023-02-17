package abstraction;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;

import accessdistribution.DataAccessDistribution;
import accessdistribution.DistributionTypeInfo;
import accessdistribution.IntegerParaDistribution;
import config.Configurations;
import transactionlogic.ParameterDependency;
import transactionlogic.ParameterNode;
import workloadgenerator.Stats;

public abstract class SqlStatement extends TransactionBlock {

	public int operationId;

	public String sql = null;
	protected PreparedStatement pstmt = null;

	protected int[] paraDataTypes = null;

	// 输入参数的数据分布类型，在sql解析时获取
	protected DistributionTypeInfo[] paraDistTypeInfos = null;

	// 当一个参数的值不能依据事务逻辑确定时需根据数据访问分布随机生成，优先使用当前时间窗口的数据访问分布
	// 当前时间窗口SQL参数的数据分布
	protected DataAccessDistribution[] windowParaGenerators = null;
	// 全负载周期SQL参数的数据分布
	protected DataAccessDistribution[] fullLifeCycleParaGenerators = null;

	// 非预编译执行时，需要将日期类型属性转化为固定形式
	protected static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	@Override
	public void prepare(Connection conn) {
		try {
			pstmt = conn.prepareStatement(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	// 注意这里的obj都是包装类型，index的起始位置是1
	protected void setParameter(int index, int dataType, Object obj) throws SQLException {
		try{
			switch (dataType) {
				case 0:
					pstmt.setLong(index, (Long) obj);
					break;
				case 1:
					pstmt.setDouble(index, (Double) obj);
					break;
				case 2:
					pstmt.setBigDecimal(index, new BigDecimal(obj.toString()));
					break;
				case 3:
					// pstmt.setDate(index, new Date((Long)obj));
					pstmt.setTimestamp(index, new Timestamp((Long) obj));
					break;
				case 4:
					pstmt.setString(index, obj.toString());
					break;
				case 5:
					pstmt.setBoolean(index, (Boolean) obj);
					break;
				default:
					System.err.println("Unrecognized data type!");
			}
		}catch (Exception e){
			e.printStackTrace();
		}

	}
//
//	protected Object geneParameter(int paraIndex) {
//		return 1l;
//	}

	private Object getParameterByIdx(int idx){
		if (windowParaGenerators[idx] != null && !Configurations.isExpFullLifeCycleDist()) {
			return windowParaGenerators[idx].geneValue();
		} else {
			return fullLifeCycleParaGenerators[idx].geneValue();
		}
	}

	private Object getParameterInSamePartitionByIdx(int idx, Object parameter){
		if (windowParaGenerators[idx] != null && !Configurations.isExpFullLifeCycleDist()) {
			return windowParaGenerators[idx].geneValueInSamePartition(parameter);
		} else {
			return fullLifeCycleParaGenerators[idx].geneValueInSamePartition(parameter);
		}
	}

	private Object getParameterInDiffPartitionByIdx(int idx, Object parameter){
		if (windowParaGenerators[idx] != null && !Configurations.isExpFullLifeCycleDist()) {
			return windowParaGenerators[idx].geneValueInDiffPartition(parameter);
		} else {
			return fullLifeCycleParaGenerators[idx].geneValueInDiffPartition(parameter);
		}
	}



	// 返回值一定需和当前参数的数据类型一致（且为包装类型），paraIndex的起始位置为0
	protected Object geneParameter(int paraIndex) {
		//
		//TODO: 发现一些要传进来的参数都没传进来！！！！ 对于Delivery事务，只传进来第一次循环的参数以及7_para_0和7_para_3，且
		//TODO 7_para_3 ER 3_result_0 依赖的也是第一次的，因为3_para_0啥的都没传进来，先解决参数传进来的问题
		//
		//		// 为了做实验后续添加的。生成一个完全随机参数，即采用均匀分布生成参数~
		//		// 按理说应该根据相关属性信息来生成随机参数的，这里为了简便，就直接用全局数据访问分布了；同时事务逻辑被直接pass掉了
		if (Configurations.isExpUniformPara()) {
//			System.out.println(paraIndex+": 是采用均匀分布生成参数的，做实验才会进这里，看到就说明出现问题了");
			return fullLifeCycleParaGenerators[paraIndex].geneUniformValue();
		}
		// 上面这段if代码是为了做实验后续补充的~

		Object parameter = null;
		// 当前参数的标识符
		String paraIdentifier = operationId + "_para_" + paraIndex;

//		System.out.println("看一下进来的是哪个参数： "+paraIdentifier);
		// 获得当前参数的事务逻辑信息（等于、包含和线性依赖关系）
		ParameterNode parameterNode = parameterNodeMap.get(paraIdentifier);


		// bug fix：添加事务逻辑统计项控制参数后，parameterNode可能为空 --------
		// Configurations.isExpFullLifeCycleDist()为真时，仅根据全局数据访问分布生成参数
		if (parameterNode == null) {
//			System.out.println("parameterNode居然是空的，小概率出现这个现象");
//			System.out.println("**** parameterNode == null ");
			parameter = getParameterByIdx(paraIndex);
			intermediateState.put(paraIdentifier,
					new TxRunningValue(paraIdentifier, parameter, paraDataTypes[paraIndex]));
			//todo: 20210102这样的设计，照理说multiple不会有问题的！

//			if (parameter != null && parameter instanceof Long && (Long) parameter == Long.MIN_VALUE) {
//				System.out.println("111111111111111111111111111111111111111111111111111");
//			}
//			if (sql.startsWith("UPDATE STOCK SET")&&paraIdentifier.equals("9_para_3")) {
//				System.out.println("111111111111111111111111111111111111111111111111111"+" "+paraIdentifier+" "+sql);
//			}


//			System.out.println(parameter);
			return parameter;
		}
		// bug fix ----------------

		// identicalIds中的参数都是完全相等的，并且按照operationId和paraIndex进行了排序（升序）
		List<String> identicalIds = parameterNode.getIdentifiers();
		// 只要identicalIds中参数多于1个并且当前参数不是其中第一个参数，则可直接令当前参数等于identicalIds中第一个参数
		if (identicalIds.size() > 1 && !paraIdentifier.equals(identicalIds.get(0))) {

			if (intermediateState.containsKey(identicalIds.get(0))) {
				parameter = intermediateState.get(identicalIds.get(0)).value; // 维护的参数必然不为null
//				System.out.println("通过identicalIds来生成的 ");

//				if (sql.startsWith("UPDATE STOCK SET")&&paraIdentifier.equals("9_para_3")) {
//					System.out.println("22222222222222222222222222222222222222222222222222"+" "+paraIdentifier+" "+sql);
//				}
//				if (parameter != null && parameter instanceof Long && (Long) parameter == Long.MIN_VALUE) {
//					System.out.println("22222222222222222222222222222222222222222222222222");
//				}
			}
		}

		if (parameter == null) {
			// 首先查看下是否有线性依赖关系，若有线性依赖关系则尝试据此生成参数（前提是依赖项不为空）
			if (parameterNode.getLinearDependencies() != null) {
//				System.out.println("**** parameterNode.getLinearDependencies() ");

				List<ParameterDependency> linearDependencies = parameterNode.getLinearDependencies();
				// 可能其中多个线性依赖关系本质上是一样的，依赖项是相等的 && 线性系数也一致，但这不影响程序的正确性，随便选择其中一个生成参数即可
				for (ParameterDependency linearDependency : linearDependencies) {
					if (intermediateState.containsKey(linearDependency.getIdentifier())) {
//						System.out.println("通过线性依赖关系来生成的 ");
						TxRunningValue txRunningValue = intermediateState.get(linearDependency.getIdentifier());
						double a = linearDependency.getCoefficientA();
						double b = linearDependency.getCoefficientB();
						Double value = txRunningValue.getLinearRelationValue(a, b);
						if (value == null) { // 未读到数据 或者 读到的数据为null
							continue;
						}
						// 将value转化成当前参数的数据类型
						switch (paraDataTypes[paraIndex]) {
							case 0:
							case 3:
								parameter = value.longValue();
								break;
							case 1:
								parameter = value;
								break;
							case 2:
								parameter = new BigDecimal(value);
								break;
						}
						break;
					}
				} // 遍历所有线性依赖关系

//				if (sql.startsWith("UPDATE STOCK SET")&&paraIdentifier.equals("9_para_3")) {
//					System.out.println("3333333333333333333333333333333333333333333"+" "+paraIdentifier+" "+sql);
//				}
//				if (parameter != null && parameter instanceof Long && (Long) parameter == Long.MIN_VALUE) {
//					System.out.println("3333333333333333333333333333333333333333333");
//				}

			}
		} // 线性依赖关系
		//根据数据访问分布生成参数
		if (parameter == null) {
			double randomValue = Math.random();
			if (randomValue > 0.99999999) {
				randomValue = randomValue - 0.000000001;
			}

			if (randomValue >= parameterNode.getProbabilitySum()) {
				// 根据数据访问分布生成SQL参数

				parameter = getParameterByIdx(paraIndex);
			} else {
				// 根据等于、包含事务依赖关系生成SQL参数
				// dependencies中仅含有所有 等于、包含 事务依赖关系
				List<ParameterDependency> dependencies = parameterNode.getDependencies();
				// 标示每个依赖项是否存在（所在SQL操作被执行）
				boolean[] flags = new boolean[dependencies.size()];
				Arrays.fill(flags, true);
				boolean flag = true; // 标示所有依赖项是否都存在（所有相关SQL操作都被执行）
				for (int i = 0; i < dependencies.size(); i++) {
					if (!intermediateState.containsKey(dependencies.get(i).getIdentifier())) {
						flags[i] = false; // 相应SQL操作未被执行（如所在分支未被执行）
						flag = false;
					}
				}

				ParameterDependency parameterDependency = null;
				double[] cumulativeProbabilities = null;
				if (flag) {
					// 所有依赖项都存在
					cumulativeProbabilities = parameterNode.getCumulativeProbabilities();
				} else {
//					System.out.println("出现这个了，说明程序会出现问题");
					// 某些依赖项不存在（所属分支未执行），注意目前这里的概率转换是有问题的！具有相当大的误差！TODO
					//todo: 看一下这里20210102
					cumulativeProbabilities = new double[dependencies.size()];
					Arrays.fill(cumulativeProbabilities, 0);
					for (int i = 0; i < dependencies.size(); i++) {
						if (flags[i]) {
							cumulativeProbabilities[i] = dependencies.get(i).getProbability();
						}
					}
					for (int i = 1; i < cumulativeProbabilities.length; i++) {
						cumulativeProbabilities[i] += cumulativeProbabilities[i - 1];
					}
					randomValue = randomValue * (cumulativeProbabilities[cumulativeProbabilities.length - 1]
							/ parameterNode.getProbabilitySum());
				}

				for (int i = 0; i < cumulativeProbabilities.length; i++) {
					if (randomValue < cumulativeProbabilities[i]) {
						parameterDependency = parameterNode.getDependencies().get(i);
						break;
					}
				}

				// bug fix: parameterDependency报空指针错误，原因是依赖的所有项都为空（相应的分支都未执行）
				if (parameterDependency != null) {
					// 根据 "等于" 依赖关系 或者 "包含" 依赖关系 生成参数
					if (parameterDependency.getDependencyType() == ParameterDependency.DependencyType.EQUAL) { // "等于" 依赖关系
//						//modified by lyqu
//						if(paraIdentifier.equals("2_para_0")){
//							System.out.println("这里判别一下2_para_0的问题，明明就是包含依赖，怎么跑等于依赖来了");
//							System.out.println(parameterDependency);
//							System.out.println("判别结束 ************8");
//						}
//
//						//----
						parameter = intermediateState.get(parameterDependency.getIdentifier()).value;
//						System.out.println("等于依赖生成参数");
//
//						if (sql.startsWith("UPDATE STOCK SET")&&paraIdentifier.equals("9_para_3")) {
//							System.out.println("66666666666666666666666666666666666666666666"+" "+paraIdentifier+" "+sql);
//						}
//						if (parameter != null && parameter instanceof Long && (Long) parameter == Long.MIN_VALUE) {
//							System.out.println("66666666666666666666666666666666666666666666");
//						}

					} else if (parameterDependency.getDependencyType() == ParameterDependency.DependencyType.PARTITION_EQUAL){
						parameter = intermediateState.get(parameterDependency.getIdentifier()).value;

						Object para = getParameterInSamePartitionByIdx(paraIndex,parameter);
//						System.out.println(parameter+" : "+para);
						parameter = para;
					}else if (parameterDependency.getDependencyType() == ParameterDependency.DependencyType.PARTITION_NOT_EQUAL){
						parameter = intermediateState.get(parameterDependency.getIdentifier()).value;

						Object para = getParameterInDiffPartitionByIdx(paraIndex,parameter);
//						System.out.println(parameter+" : "+para);
						parameter = para;
					}
					else if (parameterDependency.getDependencyType() == ParameterDependency.DependencyType.INCLUDE) { // "包含" 依赖关系
//						System.out.println("进入包含依赖");
						TxRunningValue txRunningValue = intermediateState.get(parameterDependency.getIdentifier());
						parameter = txRunningValue.getIncludeRelationValue();

//						System.out.println("包含依赖生成参数 ");
//						if (sql.startsWith("UPDATE STOCK SET")&&paraIdentifier.equals("9_para_3")) {
//							System.out.println("77777777777777777777777777777777777777777"+" "+paraIdentifier+" "+sql);
//						}
//						if (parameter != null && parameter instanceof Long && (Long) parameter == Long.MIN_VALUE) {
//							System.out.println("77777777777777777777777777777777777777777");
//						}

					}
				}

				// 搞了半天，依赖的数据项竟然都为空... 只能再一次进行补救了~
				if (parameter == null) {
					parameter = getParameterByIdx(paraIndex);

					// bug fix: parameterDependency可能为空
					if (parameterDependency != null) {
						intermediateState.put(parameterDependency.getIdentifier(), new TxRunningValue(
								parameterDependency.getIdentifier(), parameter, paraDataTypes[paraIndex]));
					}
				}

				// Bug fix: 利用事务逻辑得来的参数可能超过了相应属性的阈值，这是多层依赖（或者叫级联依赖）带来的问题
				// 其实 Configurations.getMinProbability() 也是在解决这个问题
				// 处理的思路，针对那种非确定性的参数依赖（暂定小于0.96），此时将根据事务依赖得到的生成值与属性的阈值比较
				// 一下，不在阈值内则重新利用数据访问分布生成。
				else {
					if (parameterDependency.getProbability() < 0.96 && windowParaGenerators[paraIndex] != null
							&& !windowParaGenerators[paraIndex].inDomain(parameter)) {
						parameter = windowParaGenerators[paraIndex].geneValue();
//						if (sql.startsWith("UPDATE STOCK SET")&&paraIdentifier.equals("9_para_3")) {
//							System.out.println("000000000000000000000000000000000000000000000"+" "+paraIdentifier+" "+sql);
//						}
//						if (parameter != null && parameter instanceof Long && (Long) parameter == Long.MIN_VALUE) {
//							System.out.println("000000000000000000000000000000000000000000000"+" "+paraIdentifier+" "+sql);
//						}
//						if (sql.startsWith("UPDATE STOCK SET")&&paraIdentifier.equals("9_para_3")) {
//							System.out.println("000000000000000000000000000000000000000000000"+" "+paraIdentifier+" "+parameter+" "+sql);
//						}
					}
				}
				// bug fix -----------------

			} // 根据等于、包含事务依赖关系生成SQL参数
		} // 数据访问分布 & 等于、包含事务依赖关系

		intermediateState.put(paraIdentifier, new TxRunningValue(paraIdentifier, parameter, paraDataTypes[paraIndex]));
//		System.out.println(parameter);
		return parameter;
	}

	private Object getParameterPartition(Object parameter, int idx){
		Object paraPartition = parameter;
		String paraIdentifier = operationId + "_para_" + idx;
		ParameterNode parameterNode = parameterNodeMap.get(paraIdentifier);
//		if (parameterNode == null){
			if (windowParaGenerators[idx] != null){
				paraPartition = windowParaGenerators[idx].getParaPartition(parameter);
			}
			else if(fullLifeCycleParaGenerators[idx] != null){
				paraPartition = fullLifeCycleParaGenerators[idx].getParaPartition(paraPartition);
			}
//		}

		return paraPartition;
	}

	protected Object checkParaOutOfCardinality(int idx , String paraSchemaInfo,
											   Map<String, Integer> cardinality4paraInSchema, Map<String, Map<Object, List<Object>>> partitionUsed){
//		return geneParameter(idx);

		Object parameter = geneParameter(idx);
////		if (paraSchemaInfo.contains("@w_id") ){
////			System.out.println();
////		}
		Object paraPartition = getParameterPartition(parameter, idx);

		boolean hasPartition =  paraPartition.toString().contains("p") && !parameter.equals(paraPartition);


		Map<Object, List<Object>> partitionUsedPara = partitionUsed.get(paraSchemaInfo);

		if (!cardinality4paraInSchema.containsKey(paraSchemaInfo)){
			return parameter;
		}
//
//		if (Configurations.isUsePartitionRule() && hasPartition && cardinality4paraInSchema.get(paraSchemaInfo) > 0){
//			Random random = new Random();
//			if (cardinality4paraInSchema.get(paraSchemaInfo) <= partitionUsedPara.size()){
////				if (paraSchemaInfo.contains("s_w_id") ){
////					System.out.println(partitionUsedPara.size()+" "+partitionUsedPara.get(paraPartition) + " " + paraPartition + " " + parameter);
////				}
//				if (!partitionUsedPara.containsKey(paraPartition)){// 如果已经填满基数，不再重新构造，直接从已知的参数里找一个
//
//					int partitionIdx = random.nextInt(partitionUsedPara.size());
//					paraPartition = new ArrayList<>(partitionUsedPara.keySet()).get(partitionIdx);
//
//					partitionIdx = new Random().nextInt(partitionUsedPara.get(paraPartition).size());
//						parameter = partitionUsedPara.get(paraPartition).get(partitionIdx);
//				}
//			}
//			else{
//				// 如果还没填满就重复了，重新生成
//				while (partitionUsedPara.containsKey(paraPartition) && random.nextDouble() < 0.5){
//					parameter = geneParameter(idx);
//					paraPartition = getParameterPartition(parameter, idx);
//				}
//			}
//		}
//
//
		if (!partitionUsedPara.containsKey(paraPartition)) {
			partitionUsedPara.put(paraPartition, new ArrayList<>());
		}
		if (hasPartition){
			partitionUsedPara.get(paraPartition).add(parameter);
		}
//
//
		return parameter;
	}

	protected Object checkParaOutOfCardinality(int idx, Object para , String paraSchemaInfo,
											   Map<String, Integer> cardinality4paraInSchema, Map<String, Map<Object, List<Object>>> partitionUsed){
		Object parameter = para;
		Object paraPartition = getParameterPartition(parameter, idx);

		boolean hasPartition = paraPartition.toString().contains("p") && !parameter.equals(paraPartition);


		Map<Object, List<Object>> partitionUsedPara = partitionUsed.get(paraSchemaInfo);

		if (!cardinality4paraInSchema.containsKey(paraSchemaInfo)){
			return parameter;
		}
//
//		if (Configurations.isUsePartitionRule() && hasPartition && cardinality4paraInSchema.get(paraSchemaInfo) > 0){
//			Random random = new Random();
//			if (cardinality4paraInSchema.get(paraSchemaInfo) <= partitionUsedPara.size()){
////				if (paraSchemaInfo.contains("s_w_id") ){
////					System.out.println(partitionUsedPara.size()+" "+partitionUsedPara.get(paraPartition) + " " + paraPartition + " " + parameter);
////				}
//				if (!partitionUsedPara.containsKey(paraPartition)){// 如果已经填满基数，不再重新构造，直接从已知的参数里找一个
//
//
//					int partitionIdx = random.nextInt(partitionUsedPara.size());
//					paraPartition = new ArrayList<>(partitionUsedPara.keySet()).get(partitionIdx);
//
//					// 分区键的参数是value
//					partitionIdx = new Random().nextInt(partitionUsedPara.get(paraPartition).size());
//
//					parameter = partitionUsedPara.get(paraPartition).get(partitionIdx);
//				}
//			}
//			else{
//				// 如果还没填满就重复了，重新生成
//				if (Configurations.isUsePartitionRule() && partitionUsedPara.containsKey(paraPartition) && random.nextDouble() < 0.7){
//					return null;
//				}
//			}
//		}
//
//
		if (!partitionUsedPara.containsKey(paraPartition)) {
			partitionUsedPara.put(paraPartition, new ArrayList<>());
		}
		if (hasPartition){
			partitionUsedPara.get(paraPartition).add(parameter);
		}


		return parameter;
	}



	protected Object geneParameterByMultipleLogic(int paraIndex, Map<String, Double> multipleLogicMap, int round) {
		String paraMultiIdentifier = "multiple_" + operationId + "_para_" + paraIndex;
//		System.out.println("multiple生成加进来的参数： "+paraMultiIdentifier);
		if (multipleLogicMap.containsKey(paraMultiIdentifier)) {
//			System.out.println("根据multiple逻辑生成");
			double increment = multipleLogicMap.get(paraMultiIdentifier);
			String paraIdentifier = operationId + "_para_" + paraIndex;
			TxRunningValue txRunningValue = intermediateState.get(paraIdentifier);
			// 这里返回的参数不可能为null
			return txRunningValue.getMultipleLogicValue(increment * round);
		} else {
//			System.out.println("根据一般的参数生成");
			return geneParameter(paraIndex);
		}
	}

	//added by qly
	public void setParaDataTypes(int []paraDataTypes){
		this.paraDataTypes = paraDataTypes;
	}
	public void setParaDistTypeInfos(DistributionTypeInfo[] paraDistTypeInfos){
		this.paraDistTypeInfos = paraDistTypeInfos;
	}
	public void setWindowParaGenerators(Column[] fakeColumn){
		DataAccessDistribution[] windowParaGeneratorsModified = new DataAccessDistribution[this.windowParaGenerators.length+fakeColumn.length];
		int i = 0;
//		long time = this.windowParaGenerators[0].getTime(); //dsc的时候才settime
		for(;i < this.windowParaGenerators.length;i++){
			windowParaGeneratorsModified[i] = windowParaGenerators[i];
		}
		for (Column column : fakeColumn) {
			windowParaGeneratorsModified[i] = generateFakeColumnParaDistribution(column);
			i++;
		}
		this.windowParaGenerators = windowParaGeneratorsModified;
	}
	public void setFullLifeCycleParaGenerators(Column[] fakeColumn){
		DataAccessDistribution[] fullLifeCycleParaGeneratorsModified = new DataAccessDistribution[this.fullLifeCycleParaGenerators.length+fakeColumn.length];
		int i = 0;
		for(;i < this.fullLifeCycleParaGenerators.length;i++){
			fullLifeCycleParaGeneratorsModified[i] = fullLifeCycleParaGenerators[i];
		}
		for (Column column : fakeColumn) {
			fullLifeCycleParaGeneratorsModified[i] = generateFakeColumnParaDistribution(column);
			i++;
		}
		this.fullLifeCycleParaGenerators = fullLifeCycleParaGeneratorsModified;

	}
//	public DataAccessDistribution[] getWindowParaGenerators(){
//		return windowParaGenerators;
//	}
	public DataAccessDistribution[] getFullLifeCycleParaGenerators(){
		return fullLifeCycleParaGenerators;
	}

	//added by qly
	public IntegerParaDistribution generateFakeColumnParaDistribution(Column column){
		long windowMinValue =(long)column.getPara1(), windowMaxValue = (long)column.getPara2();
		// 0.7214
		int hFItemNumber = Configurations.getHighFrequencyItemNum();
		int intervalNumber = Configurations.getIntervalNum();
		double[] hFItemFrequencies = new double[hFItemNumber];
		double[] intervalFrequencies = new double[intervalNumber];
		long[] intervalCardinalities = new long[intervalNumber];
		double uniformFrequencies = 1.0/(hFItemNumber+intervalNumber);
		for(int i=0;i < hFItemNumber;i++){
			hFItemFrequencies[i] = uniformFrequencies;
		}
		for(int i = 0;i < intervalNumber;i++){
			intervalFrequencies[i] = uniformFrequencies;
			intervalCardinalities[i] = (long)(Math.random()*column.getCardinality());
		}

		IntegerParaDistribution distribution = new IntegerParaDistribution(windowMinValue, windowMaxValue,
				hFItemFrequencies, intervalCardinalities, intervalFrequencies);
		distribution.setColumnInfo((long)column.getPara1(),(long)column.getPara2(),column.getCardinality(), column.getCoefficient());
		distribution.init4IntegerParaGene();
		return distribution;  //注意：调用这个函数的话需要后面跟一个init来生成累计概率分布&index 啥的~  错，感觉不需要 super已经init了
	}

	// 服务于Multiple块内操作的执行（非第一次执行）
//	public abstract int execute(Map<String, Double> multipleLogicMap, int round);

	public abstract int execute(Map<String, Integer> cardinality4paraInSchema, Map<String, Map<Object, List<Object>>> partitionUsed,
								Map<String, Double> multipleLogicMap, int round);

	public abstract int execute(Map<String, Integer> cardinality4paraInSchema, Map<String, Map<Object, List<Object>>> partitionUsed,
								Statement stmt, Map<String, Double> multipleLogicMap, int round);

//	public abstract int execute(Statement stmt, Map<String, Double> multipleLogicMap, int round);

	public int getOperationId() {
		return operationId;
	}

	public int[] getParaDataTypes() {
		return paraDataTypes;
	}

	public DistributionTypeInfo[] getParaDistTypeInfos() {
		if (paraDistTypeInfos == null){
			return new DistributionTypeInfo[0];
		}
		return paraDistTypeInfos;
	}

	/**
	 * 获取记录表中每个分区共有多少个参数
	 * @param partitionUsed
	 * @return
	 */
	protected Map<String, Map<Object, Integer>> getUsedPartitionSize(Map<String, Map<Object, List<Object>>> partitionUsed) {
		Map<String, Map<Object, Integer>> usedPartitionSize = new HashMap<>();
		for (String columnName :partitionUsed.keySet()) {
			Map<Object, Integer> usedSize = new HashMap<>();
			for (Object key : partitionUsed.get(columnName).keySet()) {
				if (partitionUsed.get(columnName).get(key) != null)
					usedSize.put(key, partitionUsed.get(columnName).get(key).size());
			}
			usedPartitionSize.put(columnName, usedSize);
		}
		return usedPartitionSize;
	}



	protected void getDiffUsedPartitionSize(Map<String, Map<Object, Integer>> usedPartitionSize, Map<String, Map<Object, List<Object>>> partitionUsed) {
		int partitionCnt = 1;
		for (String columnName :partitionUsed.keySet()) {
//			int cnt = 0;
			for (Object key : partitionUsed.get(columnName).keySet()) {
				if (partitionUsed.get(columnName).get(key) != null && (
						!usedPartitionSize.get(columnName).containsKey(key) ||
								partitionUsed.get(columnName).get(key).size() > usedPartitionSize.get(columnName).get(key))) {
					partitionCnt ++;
				}
			}
//			partitionCnt = Math.max(partitionCnt, cnt);
		}
		Stats.addSQLPartitionCnt(partitionCnt);
	}

	public void setParaDistribution(Map<String, DataAccessDistribution> paraId2Distribution, int type) {
		if (type == 0) { // 全负载周期数据访问分布
			for (int i = 0; i < (paraDataTypes == null ? 0 : paraDataTypes.length); i++) {
				String paraIdentifier = operationId + "_" + i;
				if (paraId2Distribution != null) { // 等于null应该是不可能的
					fullLifeCycleParaGenerators[i] = paraId2Distribution.get(paraIdentifier);
				}
			}
		} else if (type == 1) { // 当前时间窗口的数据访问分布
			for (int i = 0; i < (paraDataTypes == null ? 0 : paraDataTypes.length); i++) {
				String paraIdentifier = operationId + "_" + i;
				// 更倾向于利用全负载周期的数据访问分布，而不是保留一个最近时间窗口的数据访问分布 TODO 是否合理呢？
				if (paraId2Distribution == null) { // 有可能等于null，在当前时间窗口该事务没有执行过
					windowParaGenerators[i] = null;
				} else {
					windowParaGenerators[i] = paraId2Distribution.get(paraIdentifier);
					// if (paraId2Distribution.containsKey(paraIdentifier) &&
					// paraId2Distribution.get(paraIdentifier) != null) {
					// windowParaGenerators[i] = paraId2Distribution.get(paraIdentifier);
					// }
				}
			}
		}
	}

	// added by zsy 用作比较事务模板是否相等
	@Override
	public int hashCode() {
		return this.sql.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof SqlStatement)) {
			return false;
		}
		return this.sql.equals(((SqlStatement) obj).sql);
	}
}
