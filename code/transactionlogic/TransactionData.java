package transactionlogic;

import java.util.Arrays;

/**
 * 一个事务实例的数据
 */
public class TransactionData {

	// 所有操作的数据
	private Object[] operationDatas = null; //qly: operationData or operationDataList
	// 0:一个OperationData对象; 1:OperationData对象的列表(Multiple内的操作); -1:Null(分支内的操作可能未被执行)
	private int[] operationTypes = null;

	public TransactionData(Object[] operationDatas, int[] operationTypes) {
		super();
		this.operationDatas = operationDatas;
		this.operationTypes = operationTypes;
	}

	public Object[] getOperationDatas() {
		return operationDatas;
	}

	public int[] getOperationTypes() {
		return operationTypes;
	}

	@Override
	public String toString() {
		return "TransactionData [operationDatas=" + Arrays.toString(operationDatas) + ", operationTypes="
				+ Arrays.toString(operationTypes) + "]";
	}
}
