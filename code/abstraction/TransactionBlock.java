package abstraction;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import transactionlogic.ParameterNode;

public abstract class TransactionBlock {
	
	public abstract void prepare(Connection conn);
	
	// 原来这里的返回值是boolean（执行成功或者失败），后面为了统计Deadlock的吞吐，需要返回三种状态
	// 1：成功；0：非Deadlock失败；-1：Deadlock失败

//	public abstract int execute();
//	public abstract int execute(Statement stmt);
	public abstract int execute(Map<String, Integer> cardinality4paraInSchema, Map<String, Set<Object>> paraUsed);
	public abstract int execute(Map<String, Integer> cardinality4paraInSchema, Map<String, Set<Object>> paraUsed,
								Statement stmt);
	// 返回paraid和对应的tableName_columnName
	public abstract Map<String, String> getParaId2Name();

	// String: paraIdentifier = operationId + "_para_" + paraIndex;
	// TxRunningValue: 事务运行过程中一些中间状态的值，包含SQL操作的输入参数和返回结果集
	// (三个实例变量：identifier(形式为：operationId + "para"/"result" + index),value,type)
	protected Map<String, TxRunningValue> intermediateState = null;
	protected Map<String, ParameterNode> parameterNodeMap = null;
	
	public void setIntermediateState(Map<String, TxRunningValue> intermediateState) {
		this.intermediateState = intermediateState;
	}
	public void setParameterNodeMap(Map<String, ParameterNode> parameterNodeMap) {
		this.parameterNodeMap = parameterNodeMap;
	}

	protected Object checkParaOutOfCardinality(Object para, String paraSchemaInfo,
											   Map<String, Integer> cardinality4paraInSchema, Map<String, Set<Object>> paraUsed){
		Object parameter = para;
//		if (cardinality4paraInSchema.containsKey(paraSchemaInfo)){
//			// 如果已经填满基数，不再重新构造，直接从已知的参数里找一个
//			if (cardinality4paraInSchema.get(paraSchemaInfo) == paraUsed.get(paraSchemaInfo).size()){
//				parameter = new ArrayList<>(paraUsed.get(paraSchemaInfo)).get(
//						new Random().nextInt(cardinality4paraInSchema.get(paraSchemaInfo)));
//			}
//			else{
//				paraUsed.get(paraSchemaInfo).add(parameter);
//			}
//		}
		return parameter;
	}
}
