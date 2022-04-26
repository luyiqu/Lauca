package transactionlogic;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

import input.TraceInfo;
import org.omg.CORBA.OBJECT_NOT_EXIST;

/**
 * 一个操作的数据
 */
public class OperationData implements Comparable<OperationData> {

	// 操作的标识ID,从1开始计数
	private int operationId;

	// 返回元素的数据类型
	private int[] returnDataTypes = null;
	// 两种情况下该属性值为TRUE. 1:filter条件中的属性包含所有主键属性; 2:SQL语句中包含聚合函数或者'limit 1'。
	private boolean filterPrimaryKey;
	private Object[] returnItems = null; // filterPrimaryKey=True
	private Object[][] returnItemsOfTuples = null; // filterPrimaryKey=False

	// 数据参数的数据类型
	private int[] paraDataTypes = null;
	private Object[] parameters = null;

	// 从输入的事务模板中构造的对象
	public OperationData(int operationId, int[] returnDataTypes, boolean filterPrimaryKey, int[] paraDataTypes) {
		super();
		this.operationId = operationId;
		this.returnDataTypes = returnDataTypes;
		this.filterPrimaryKey = filterPrimaryKey;
		this.paraDataTypes = paraDataTypes;
	}

	// 从运行日志中构造的对象
	public OperationData(int operationId, int[] returnDataTypes, boolean filterPrimaryKey, Object[] returnItems,
			Object[][] returnItemsOfTuples, int[] paraDataTypes, Object[] parameters) {
		super();
		this.operationId = operationId;
		this.returnDataTypes = returnDataTypes;
		this.filterPrimaryKey = filterPrimaryKey;
		this.returnItems = returnItems;
		this.returnItemsOfTuples = returnItemsOfTuples;
		this.paraDataTypes = paraDataTypes;
		this.parameters = parameters;
	}

	/**
	 * 
	 * @param oneTrace 一条操作的负载轨迹
	 * @param index    处理轨迹中的第几组参数
	 * @return 当前操作的一个具体数据对象
	 * @author Shuyan Zhang
	 */
	public OperationData newInstance(TraceInfo oneTrace) {

//		String[] arr = runningLog.trim().split(";");

		String[] paraArr = oneTrace.parameters.toArray(new String[0]);
		parameters = new Object[paraDataTypes.length];
		try{
			for (int i = 0; i < paraDataTypes.length; i++) {

				parameters[i] = str2Object(paraArr[i].trim(), paraDataTypes[i]);
			}
		}catch (Exception e){
			System.out.println(oneTrace.operationID);
			System.out.println(this.operationId);
		}


		returnItems = null;
		returnItemsOfTuples = null;
		if (oneTrace.results == null || oneTrace.results.size() == 0) {
			if (returnDataTypes != null) { // 当前操作虽然是select操作,但是没有满足谓词的记录
				return new OperationData(operationId, Arrays.copyOf(returnDataTypes, returnDataTypes.length),
						filterPrimaryKey, null, null, Arrays.copyOf(paraDataTypes, paraDataTypes.length), parameters);
			} else {
				return new OperationData(operationId, null, filterPrimaryKey, null, null,
						Arrays.copyOf(paraDataTypes, paraDataTypes.length), parameters);
			}
		}

		if (filterPrimaryKey) {
			String[] returnItemsArr = oneTrace.results.get(0).toArray(new String[0]);
			returnItems = new Object[returnItemsArr.length];
			for (int i = 0; i < returnItemsArr.length; i++) {
				returnItems[i] = str2Object(returnItemsArr[i].trim(), returnDataTypes[i]);
			}
		} else {
//			String[] returnItemsOfTuplesArr = arr[1].split("#");
//			returnItemsOfTuples = new Object[returnItemsOfTuplesArr.length][];
//			for (int i = 0; i < returnItemsOfTuplesArr.length; i++) {
//				String[] returnItemsArr = returnItemsOfTuplesArr[i].split(",");
//				Object[] tmp = new Object[returnItemsArr.length];
//				for (int j = 0; j < returnItemsArr.length; j++) {
//					tmp[j] = str2Object(returnItemsArr[j].trim(), returnDataTypes[j]);
//				}
//				returnItemsOfTuples[i] = tmp;
//			}

			returnItemsOfTuples = new Object[oneTrace.results.size()][];
			for (int i = 0; i < oneTrace.results.size(); ++i) {
				Object[] tmp = new Object[oneTrace.results.get(i).size()];
				for (int j = 0; j < tmp.length; j++) {
					tmp[j] = str2Object(oneTrace.results.get(i).get(j), returnDataTypes[j]);
				}
				returnItemsOfTuples[i] = tmp;
			}
		}

		return new OperationData(operationId, Arrays.copyOf(returnDataTypes, returnDataTypes.length), filterPrimaryKey,
				returnItems, returnItemsOfTuples, Arrays.copyOf(paraDataTypes, paraDataTypes.length), parameters);

	}

	/**
	 * @param runningLog: 运行日志,格式为:'para1, para2, ...; res1, res2, ...# ...'
	 *                    ('#'分隔的是返回的多个tuple)
	 * @return 当前操作的一个具体数据对象
	 */
	public OperationData newInstance(String runningLog) {
		String[] arr = runningLog.trim().split(";");

		String[] paraArr = arr[0].split(",");
		parameters = new Object[paraArr.length];
		for (int i = 0; i < paraArr.length; i++) {
			parameters[i] = str2Object(paraArr[i].trim(), paraDataTypes[i]);
		}

		returnItems = null;
		returnItemsOfTuples = null;
		if (arr.length == 1) {
			if (returnDataTypes != null) { // 当前操作虽然是select操作,但是没有满足谓词的记录
				return new OperationData(operationId, Arrays.copyOf(returnDataTypes, returnDataTypes.length),
						filterPrimaryKey, null, null, Arrays.copyOf(paraDataTypes, paraDataTypes.length), parameters);
			} else {
				return new OperationData(operationId, null, filterPrimaryKey, null, null,
						Arrays.copyOf(paraDataTypes, paraDataTypes.length), parameters);
			}
		}

		if (filterPrimaryKey) {
			String[] returnItemsArr = arr[1].split(",");
			returnItems = new Object[returnItemsArr.length];
			for (int i = 0; i < returnItemsArr.length; i++) {
				returnItems[i] = str2Object(returnItemsArr[i].trim(), returnDataTypes[i]);
			}
		} else {
			String[] returnItemsOfTuplesArr = arr[1].split("#");
			returnItemsOfTuples = new Object[returnItemsOfTuplesArr.length][];
			for (int i = 0; i < returnItemsOfTuplesArr.length; i++) {
				String[] returnItemsArr = returnItemsOfTuplesArr[i].split(",");
				Object[] tmp = new Object[returnItemsArr.length];
				for (int j = 0; j < returnItemsArr.length; j++) {
					tmp[j] = str2Object(returnItemsArr[j].trim(), returnDataTypes[j]);
				}
				returnItemsOfTuples[i] = tmp;
			}
		}

		return new OperationData(operationId, Arrays.copyOf(returnDataTypes, returnDataTypes.length), filterPrimaryKey,
				returnItems, returnItemsOfTuples, Arrays.copyOf(paraDataTypes, paraDataTypes.length), parameters);
	}

	private Object str2Object(String str, int dataType) {
		switch (dataType) {
		case 0:
			return Long.parseLong(str);
		case 1:
			return Double.parseDouble(str);
		case 2:
			return new BigDecimal(str);
		case 3:
			return Long.parseLong(str);
		case 4:
			return str;
		case 5:
			return Boolean.parseBoolean(str);
		default:
			return null;
		}
	}

	public int getOperationId() {
		return operationId;
	}

	public int[] getReturnDataTypes() {
		return returnDataTypes;
	}

	public boolean isFilterPrimaryKey() {
		return filterPrimaryKey;
	}

	public Object[] getReturnItems() {
		return returnItems;
	}

	public Object[][] getReturnItemsOfTuples() {
		return returnItemsOfTuples;
	}

	public int[] getParaDataTypes() {
		return paraDataTypes;
	}

	public Object[] getParameters() {
		return parameters;
	}

	@Override
	public int compareTo(OperationData o) {
		if (this.operationId < o.operationId) {
			return -1;
		} else if (this.operationId > o.operationId) {
			return 1;
		} else {
			return 0;
		}
	}

	@Override
	public String toString() {
		return "OperationData [operationId=" + operationId + ", returnDataTypes=" + Arrays.toString(returnDataTypes)
				+ ", filterPrimaryKey=" + filterPrimaryKey + ", returnItems=" + Arrays.toString(returnItems)
				+ ", returnItemsOfTuples=" + Arrays.deepToString(returnItemsOfTuples) + ", paraDataTypes="
				+ Arrays.toString(paraDataTypes) + ", parameters=" + Arrays.toString(parameters) + "]\n\t";
	}
}
