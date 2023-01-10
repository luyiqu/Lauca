package abstraction;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.*;

import accessdistribution.DataAccessDistribution;
import accessdistribution.DistributionTypeInfo;

public class ReadOperation extends SqlStatement {

	private String[] returnItems = null;
	private int[] returnDataTypes = null;
	private boolean filterPrimaryKey;

	// tableName@columnName
	private List<String> paraSchemaInfos = new ArrayList<>();

	public ReadOperation(int operationId, String sql, int[] paraDataTypes, DistributionTypeInfo[] paraDistTypeInfos,List<String> paraSchemaInfos,
			String[] returnItems, int[] returnDataTypes, boolean filterPrimaryKey) {
		super();
		this.operationId = operationId;
		this.sql = sql;
		this.paraDataTypes = paraDataTypes;
		this.paraDistTypeInfos = paraDistTypeInfos;
		this.paraSchemaInfos = paraSchemaInfos;
		
		this.returnItems = returnItems;
		this.returnDataTypes = returnDataTypes;
		this.filterPrimaryKey = filterPrimaryKey;
		
		windowParaGenerators = new DataAccessDistribution[paraDataTypes == null ? 0 : paraDataTypes.length];
		fullLifeCycleParaGenerators = new DataAccessDistribution[paraDataTypes == null ? 0 : paraDataTypes.length];
//		sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	}

	//added by lyqu，针对没有谓词，即where后面 的情况
	public ReadOperation(int operationId, String sql, String[] returnItems, int[] returnDataTypes) {
		super();
		this.operationId = operationId;
		this.sql = sql;
		this.paraDataTypes = null;
		this.paraDistTypeInfos = null;

		this.returnItems = returnItems;
		this.returnDataTypes = returnDataTypes;
		this.filterPrimaryKey = false;

		windowParaGenerators = null;
		fullLifeCycleParaGenerators = null;
//		sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	}
	//---



	// 深拷贝主要是为了保证 数据库执行器 不被覆盖
	public ReadOperation(ReadOperation readOperation) {
		super();
		// 这些成员都是只读的
		this.operationId = readOperation.operationId;
		this.sql = readOperation.sql;
		this.paraDataTypes = readOperation.paraDataTypes;
		this.paraDistTypeInfos = readOperation.paraDistTypeInfos;
		this.returnItems = readOperation.returnItems;
		this.returnDataTypes = readOperation.returnDataTypes;
		this.filterPrimaryKey = readOperation.filterPrimaryKey;

		if (readOperation.paraSchemaInfos != null)
			this.paraSchemaInfos = new ArrayList<>(readOperation.paraSchemaInfos);

		windowParaGenerators = new DataAccessDistribution[paraDataTypes == null ? 0 : paraDataTypes.length];
		fullLifeCycleParaGenerators = new DataAccessDistribution[paraDataTypes == null ? 0 : paraDataTypes.length];
//		sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	}

	@Override
	public int execute(Map<String, Integer> cardinality4paraInSchema, Map<String, Map<Object, List<Object>>> partitionUsed) {
//		long startTime = System.currentTimeMillis();
		try {
			for (int i = 0; i < paraDataTypes.length; i++) {
//				Object para = geneParameter(i);
//				System.out.println("**************");
//				System.out.println(para+" "+para.getClass());
//				System.out.println(paraDataTypes[i]);
//				System.out.println("**************");
				Object parameter = checkParaOutOfCardinality(
						i,
						this.paraSchemaInfos.get(i),
						cardinality4paraInSchema,
						partitionUsed
				);


				setParameter(i + 1, paraDataTypes[i], parameter);
			}
//			long endTime = System.currentTimeMillis();
//			LaucaTestingEnv.geneTime += endTime - startTime;
//			long startTime1 = System.currentTimeMillis();
			ResultSet rs = pstmt.executeQuery();
//			long endTime1 = System.currentTimeMillis();
//			LaucaTestingEnv.updateTime += endTime1 - startTime1;
			saveResultSet(rs);
			return 1;
		} catch (Exception e) {
			if (e instanceof  SQLException) {
				return -1;
			}
			System.out.println("bbbbbbb"+pstmt.toString());
			e.printStackTrace();
			System.out.println(sql);
			return 0;
		}
	}

	@Override
	public int execute(Map<String, Integer> cardinality4paraInSchema, Map<String, Map<Object, List<Object>>> partitionUsed, Statement stmt) {
		try {
			String tmp = sql;
			for (int i = 0; i < paraDataTypes.length; i++) {
				Object parameter = geneParameter(i);
				if (paraDataTypes[i] == 3) {
					tmp = tmp.replaceFirst("\\?", " '" + sdf.format(new Date((Long)parameter)) + "' ");
				} else if (paraDataTypes[i] == 4) {
					tmp = tmp.replaceFirst("\\?", " '" + parameter.toString() + "' ");
				} else {
					tmp = tmp.replaceFirst("\\?", " " + parameter.toString() + " ");
				}
			}
			ResultSet rs = stmt.executeQuery(tmp);
			saveResultSet(rs);
			return 1;
		} catch (SQLException e) {
//			e.printStackTrace();
			if (e.getMessage().contains("Deadlock")) {
				return -1;
			}
//			System.err.println("ERROR!!!");
			return 0;
		}
	}

	@Override
	public int execute(Map<String, Integer> cardinality4paraInSchema, Map<String, Map<Object, List<Object>>> partitionUsed,
					   Map<String, Double> multipleLogicMap, int round) {
//		long startTime = System.currentTimeMillis();
//		System.out.println("照例说Read操作肯定会走这里，但这里是指multiple的事务逻辑");
		try {
			for (int i = 0; i < paraDataTypes.length; i++) {
				Object parameter = checkParaOutOfCardinality(i,
						geneParameterByMultipleLogic(i, multipleLogicMap, round),
						this.paraSchemaInfos.get(i),
						cardinality4paraInSchema,
						partitionUsed
				);
				while (parameter == null){
					parameter = checkParaOutOfCardinality(i,
							geneParameterByMultipleLogic(i, multipleLogicMap, round),
							this.paraSchemaInfos.get(i),
							cardinality4paraInSchema,
							partitionUsed
					);
				}

				setParameter(i + 1, paraDataTypes[i], parameter);
			}

//			long endTime = System.currentTimeMillis();
//			LaucaTestingEnv.geneTime += endTime - startTime;
//			long startTime1 = System.currentTimeMillis();
			ResultSet rs = pstmt.executeQuery();
//			System.out.println(sql+" : save了");
			saveResultSet(rs);

//			long endTime1 = System.currentTimeMillis();
//			LaucaTestingEnv.updateTime += endTime1 - startTime1;
			return 1;
		} catch (Exception e) {
			if ( e instanceof SQLException) {
				return -1;
			}
			e.printStackTrace();
			return 0;
		}
	}

	@Override
	public int execute(Map<String, Integer> cardinality4paraInSchema, Map<String, Map<Object, List<Object>>> partitionUsed,
					   Statement stmt, Map<String, Double> multipleLogicMap, int round) {
		try {
			String tmp = sql;
			for (int i = 0; i < paraDataTypes.length; i++) {
				Object parameter = geneParameterByMultipleLogic(i, multipleLogicMap, round);
				if (paraDataTypes[i] == 3) {
					tmp = tmp.replaceFirst("\\?", " '" + sdf.format(new Date((Long)parameter)) + "' ");
				} else if (paraDataTypes[i] == 4) {
					tmp = tmp.replaceFirst("\\?", " '" + parameter.toString() + "' ");
				} else {
					tmp = tmp.replaceFirst("\\?", " " + parameter.toString() + " ");
				}
			}
			ResultSet rs = stmt.executeQuery(tmp);
			saveResultSet(rs);
			return 1;
		} catch (SQLException e) {
//			e.printStackTrace();
			if (e.getMessage().contains("Deadlock")) {
				return -1;
			}
//			System.err.println("ERROR!!!");
			return 0;
		}
	}

	@Override
	public Map<String, String> getParaId2Name() {
		Map<String, String> paraId2Name = new HashMap<>();
		for (int i = 0; i < paraSchemaInfos.size(); i++) {
			String paraIdentifier = operationId + "_" + i;
			paraId2Name.put(paraIdentifier, paraSchemaInfos.get(i));
		}

		return paraId2Name;
	}

	private void saveResultSet(ResultSet rs) throws SQLException {
		// 先把整个ResultSet中的数据取出来
		List<Object[]> resultList = new ArrayList<>();
		while (rs.next()) {
			// result是ResultSet中的一条tuple
			Object[] result = new Object[returnItems.length];
			for (int i = 0; i < returnItems.length; i++) {
				result[i] = getReturnValue(i + 1, returnDataTypes[i], rs);
			}
			resultList.add(result);
		}

		for (int i = 0; i < returnItems.length; i++) { // 针对每个返回项依次进行处理
			// 当前返回项的标识符
			String identifier = operationId + "_result_" + i;
			// 当前返回项的所有数据 -- 利用数组是考虑到返回结果集中可能含有多个tuple
//			System.out.println("看一下1_result_0是否被保存起来了");
//			System.out.println(identifier);
			Object[] values = new Object[resultList.size()];
			for (int j = 0; j < resultList.size(); j++) {
				values[j] = resultList.get(j)[i];
			}

			if (values.length == 0) {
				intermediateState.put(identifier, new TxRunningValue(identifier, null, returnDataTypes[i]));
			} else if (values.length == 1) {
				intermediateState.put(identifier, new TxRunningValue(identifier, values[0], returnDataTypes[i]));
			} else {
				intermediateState.put(identifier, new TxRunningValue(identifier, values, returnDataTypes[i] + 6));
			}
		}
	}

	// 返回值需为包装类型
	private Object getReturnValue(int index, int dataType, ResultSet rs) throws SQLException {
		switch (dataType) {
		case 0:
			return rs.getLong(index);
		case 1:
			return rs.getDouble(index);
		case 2:
			return rs.getBigDecimal(index);
		case 3:
			// bug fix：对于TPC-C，这里返回的日期可能为null
			Timestamp time = rs.getTimestamp(index);
			if (time != null) {
				return time.getTime();
			} else {
				return null;
			}
		case 4:
			return rs.getString(index);
		case 5:
			return rs.getBoolean(index);
		default:
			System.err.println("Unrecognized data type!");
			return null;
		}
	}

	public int[] getReturnDataTypes() {
		return returnDataTypes;
	}

	public boolean isFilterPrimaryKey() {
		return filterPrimaryKey;
	}

	@Override
	public String toString() {
		return "\n\t\tReadOperation [operationId=" + operationId + ", returnItems=" + Arrays.toString(returnItems) + 
				", returnDataTypes=" + Arrays.toString(returnDataTypes) + ", filterPrimaryKey=" + filterPrimaryKey + 
				", sql=" + sql + ", paraDataTypes=" + Arrays.toString(paraDataTypes) + ", paraDistTypeInfos=" + 
				Arrays.toString(paraDistTypeInfos) + "]";
	}
	
}
