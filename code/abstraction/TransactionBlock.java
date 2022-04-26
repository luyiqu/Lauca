package abstraction;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import transactionlogic.ParameterNode;

public abstract class TransactionBlock {
	
	public abstract void prepare(Connection conn);
	
	// 原来这里的返回值是boolean（执行成功或者失败），后面为了统计Deadlock的吞吐，需要返回三种状态
	// 1：成功；0：非Deadlock失败；-1：Deadlock失败
	public abstract int execute();
	public abstract int execute(Statement stmt);

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
