package abstraction;

import java.sql.Connection;
import java.sql.Statement;
import java.util.*;

import transactionlogic.ParameterNode;

public abstract class TransactionBlock {
	
	public abstract void prepare(Connection conn);
	
	// 原来这里的返回值是boolean（执行成功或者失败），后面为了统计Deadlock的吞吐，需要返回三种状态
	// 1：成功；0：非Deadlock失败；-1：Deadlock失败

//	public abstract int execute();
//	public abstract int execute(Statement stmt);

	/**
	 * @param cardinality4paraInSchema 单个事务里每个参数的分区基数
	 * @param partitionUsed paraId -> partitionName -> 每个分区里已经用了的参数，如果是没有分区键的属性，partitionName就是参数本身
	 * @return
	 */
	public abstract int execute(Map<String, Integer> cardinality4paraInSchema, Map<String, Map<Object, List<Object>>> partitionUsed);
	public abstract int execute(Map<String, Integer> cardinality4paraInSchema, Map<String, Map<Object, List<Object>>> partitionUsed,
								Statement stmt);
	// 返回paraid和对应的tableName@columnName
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


}
