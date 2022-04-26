package transactionlogic;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import config.Configurations;

/**
 * 工具类，这里的方法主要供事务逻辑分析之用
 */
public class Util {

	/**
	 * @param obj1：比较对象1
	 * @param dataType1：比较对象1的数据类型
	 * @param obj2：比较对象2
	 * @param dataType2：比较对象2的数据类型
	 * @return 若两个数据项数值相等则返回True，否则返回False
	 * @function：目前两个数据项的数据类型必然相同才会进行数值大小的相对，不然直接返回False。
	 *     但是不同数据类型的数据项有时可能也存在相等关系，比如都是数值型属性~ TODO 
	 */
	public static boolean isEqual(Object obj1, int dataType1, Object obj2, int dataType2) {
		if (dataType1 != dataType2) {
			return false;
		}
		return isEqual(obj1, obj2, dataType1);
	}

	/**
	 * @see function isEqual(Object obj1, int dataType1, Object obj2, int dataType2)
	 */
	public static boolean isEqual(Object obj1, Object obj2, int dataType) {
		switch (dataType) {
		case 0:
		case 3:
			if (((Long)obj1).longValue() == ((Long)obj2).longValue()) {
				return true;
			}
			break;
		case 1:
			if (((Double)obj1).doubleValue() == ((Double)obj2).doubleValue()) {
				return true;
			}
			break;
		case 2:
			if ((new BigDecimal(obj1.toString())).compareTo(new BigDecimal(obj2.toString())) == 0) {
				return true;
			}
			break;
		case 4:
			if (obj1.toString().equals(obj2.toString())) {
				return true;
			}
			break;
		case 5:
			if (((Boolean)obj1).booleanValue() == ((Boolean)obj2).booleanValue()) {
				return true;
			}
			break;
		}
		return false;
	}

	/**
	 * @param obj1：obj1是一个二维数组，但是我们比较的对象是其第columnIndex列上的所有数据
	 * @param columnIndex：指定obj1中待比较的列位置
	 * @param dataType1：obj1中columnIndex列的数据类型
	 * @param obj2：比较对象2
	 * @param dataType2：比较对象2的数据类型
	 * @return 若obj1中columnIndex列上的数据包含obj2则返回True，否则返回False
	 */
	public static boolean isContain(Object[][] obj1, int columnIndex, int dataType1, Object obj2, int dataType2) {
		for (int i = 0; i < obj1.length; i++) {
			Object tmp = obj1[i][columnIndex];
			if (isEqual(tmp, dataType1, obj2, dataType2)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * @param obj1：数据项1
	 * @param obj2：数据项2
	 * @param dataType：数据项1和数据项2的数据类型
	 * @param increment：一个double的数组，长度为1，increment[0]中存储的是数据项1至数据项2的期望增量。当increment[0]中为
	 *     Double.MIN_VALUE时，此时将obj2-obj1的值存在其中。采用数组是为了实现引用传参~
	 * @return 当obj2-obj1的值等于increment[0] 或 increment[0]中值为Double.MIN_VALUE，返回True；否则返回False
	 */
	public static boolean isMonotonous(Object obj1, Object obj2, int dataType, double[] increment) {
		switch (dataType) {
		case 0:
		case 3:
			long ltmp = ((Long)obj2).longValue() - ((Long)obj1).longValue();
			if (increment[0] == Double.MIN_VALUE) {
				increment[0] = ltmp;
				return true;
			} else {
				if (ltmp == increment[0]) {
					return true;
				}
			}
			break;
		case 1:
			double dtmp = ((Double)obj2).doubleValue() - ((Double)obj1).doubleValue();
			if (increment[0] == Double.MIN_VALUE) {
				increment[0] = dtmp;
				return true;
			} else {
				if (dtmp == increment[0]) {
					return true;
				}
			}
			break;
		case 2:
			double btmp = (new BigDecimal(obj2.toString())).subtract(new BigDecimal(obj1.toString())).doubleValue();
			if (increment[0] == Double.MIN_VALUE) {
				increment[0] = btmp;
				return true;
			} else {
				if (btmp == increment[0]) {
					return true;
				}
			}
			break;
		case 4:
		case 5:
			return false;
		}
		return false;
	}

	/**
	 * @param counter：统计得到的等于或包含关联关系的计数器（满足关系的事务实例的个数）
	 * @param operationId2ExecutionNum：统计得到的每个操作的执行次数
	 * @return formatedCounter：格式化后的计数器，以方便后续的关联关系分析
	 * @function：对计数器的数据结构进行转化（相当于预处理操作），转化前的计算器数据结构是map，其中记录的是满足关系的事务实例个数；
	 *     转化后的计数器数据结构是list，其中记录的是满足关系的事务实例比例（已排序，具体排序规则见方法中注释）
	 */
	public static List<Entry<String, List<Entry<String, Double>>>> convertCounter(
			Map<String, Map<String, Integer>> counter, Map<Integer, Integer> operationId2ExecutionNum) {

		// return的数据结构
		List<Entry<String, List<Entry<String, Double>>>> formatedCounter = new ArrayList<>();
		// 用List方便排序
		// 因为Entry<>对象无法new出来（即无法自己构造出来），所以只能先构建相应的Map数据结构，再通过迭代器取出来~
		Map<String, List<Entry<String, Double>>> map4FormatedCounter = new HashMap<>();

		Iterator<Entry<String, Map<String, Integer>>> counterIterator = counter.entrySet().iterator();
		while (counterIterator.hasNext()) {
			// entry为当前参数的等于关联关系统计信息，下面需将其中的事务实例个数转化成比例
			Entry<String, Map<String, Integer>> entry = counterIterator.next();
			Iterator<Entry<String, Integer>> entryValueIterator = entry.getValue().entrySet().iterator();
			Map<String, Double> entryFormatedValueMap = new HashMap<>();

			int operationId = Integer.parseInt(entry.getKey().split("_")[0]);
			int executionNum = operationId2ExecutionNum.get(operationId); // 当前操作的总执行次数
			while (entryValueIterator.hasNext()) {
				Entry<String, Integer> tmp = entryValueIterator.next();
				
				// entryFormatedValueMap.put(tmp.getKey(), tmp.getValue().doubleValue() / executionNum);
				
				// bug fix: 针对decimal和varchar数据类型，lauca的输入中没有指定约束参数（如p,s,n），故可能出现
				//   一个阈值小的decimal在一个极小的概率上与一个前面阈值较大的decimal满足等于关系或其他关系；而对于字符型参
				//   数来说可能就是长度不匹配的问题。这里我们通过对小概率关系的去除来规避这个问题~
				// 相关ERROR："Data truncation: Out of range value for column ..."
				double probability = tmp.getValue().doubleValue() / executionNum;
				if (probability >= Configurations.getMinProbability()) {
					entryFormatedValueMap.put(tmp.getKey(), probability);
				}
				
			}

			// Entry<>对象无法自己new出来，所以只能这么迂回处理。下面的entryFormatedValueList是针对一个输入参数的格式化后的统计信息
			List<Entry<String, Double>> entryFormatedValueList = new ArrayList<>();
			Iterator<Entry<String, Double>> tmpIterator = entryFormatedValueMap.entrySet().iterator();
			while (tmpIterator.hasNext()) {
				entryFormatedValueList.add(tmpIterator.next());
			}

			// 排序规则：关联关系强弱，逆序 -> 操作的前后顺序，升序 -> 对象类型（para or result），先参数后返回结果集元素 -> 对象的index
			Collections.sort(entryFormatedValueList, new EntryFormatedValueComparator());
			map4FormatedCounter.put(entry.getKey(), entryFormatedValueList);
		}

		Iterator<Entry<String, List<Entry<String, Double>>>> tmpIterator = map4FormatedCounter.entrySet().iterator();
		while (tmpIterator.hasNext()) {
			formatedCounter.add(tmpIterator.next());
		}
		// 排序规则：操作的前后顺序，升序 -> 参数的index，升序
		Collections.sort(formatedCounter, new ParaIdentifierComparator());

//		System.out.println("Util.convertCounter -> formatedCounter: \n\t" + formatedCounter);
		return formatedCounter;
	}
}

class EntryFormatedValueComparator implements Comparator<Entry<String, Double>> {

	@Override
	public int compare(Entry<String, Double> o1, Entry<String, Double> o2) {
		// 首先根据关联关系的强弱进行排序，逆序（强 -> 弱）
		if (o1.getValue() < o2.getValue()) {
			return 1;
		} else if (o1.getValue() > o2.getValue()) {
			return -1;
		} else {
			// 当关联关系强弱相同时，根据操作的前后顺序进行排序，升序
			int operationId1 = Integer.parseInt(o1.getKey().split("_")[0]);
			int operationId2 = Integer.parseInt(o2.getKey().split("_")[0]);
			if (operationId1 < operationId2) {
				return -1;
			} else if (operationId1 > operationId2) {
				return 1;
			} else {
				// 当两个对象位于用一个操作中时，输入参数排在前面，返回结果集元素排在后面
				// type：para or result
				String type1 = o1.getKey().split("_")[1];
				String type2 = o2.getKey().split("_")[1];
				if (type1.equals("para") && type2.equals("result")) {
					return -1;
				} else if (type2.equals("para") && type1.equals("result")) {
					return 1;
				} else {
					// 当两个对象同为输入参数或者返回结果集元素时，根据其index进行排序，升序
					int index1 = Integer.parseInt(o1.getKey().split("_")[2]);
					int index2 = Integer.parseInt(o2.getKey().split("_")[2]);
					if (index1 < index2) {
						return -1;
					} else if (index1 > index2) {
						return 1;
					} else {
						return 0;
					}
				}
			}
		}
	}
}

class ParaIdentifierComparator implements Comparator<Entry<String, List<Entry<String, Double>>>> {

	@Override
	public int compare(Entry<String, List<Entry<String, Double>>> o1, Entry<String, List<Entry<String, Double>>> o2) {
		int operationId1 = Integer.parseInt(o1.getKey().split("_")[0]);
		int operationId2 = Integer.parseInt(o2.getKey().split("_")[0]);
		int paraIndex1 = Integer.parseInt(o1.getKey().split("_")[2]);
		int paraIndex2 = Integer.parseInt(o2.getKey().split("_")[2]);
		// 首先根据操作的前后顺序进行排序，升序
		if (operationId1 < operationId2) {
			return -1;
		} else if (operationId1 > operationId2) {
			return 1;
		} else {
			// 然后根据参数的index进行排序，升序
			if (paraIndex1 < paraIndex2) {
				return -1;
			} else if (paraIndex1 > paraIndex2) {
				return 1;
			} else {
				return 0;
			}
		}
	}
}
