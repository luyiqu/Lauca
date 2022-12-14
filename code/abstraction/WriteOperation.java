package abstraction;

import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;

import accessdistribution.DataAccessDistribution;
import accessdistribution.DistributionTypeInfo;

public class WriteOperation extends SqlStatement {

	private boolean batchExecute;

	// tableName_columnName
	private List<String> paraSchemaInfos = new ArrayList<>();

	public WriteOperation(int operationId, String sql, int[] paraDataTypes, 
			DistributionTypeInfo[] paraDistTypeInfos,List<String> paraSchemaInfos , boolean batchExecute) {
		super();
		this.operationId = operationId;
		this.sql = sql;
		this.paraDataTypes = paraDataTypes;
		this.paraDistTypeInfos = paraDistTypeInfos;

		this.paraSchemaInfos = paraSchemaInfos;
		this.batchExecute = batchExecute;
		
		windowParaGenerators = new DataAccessDistribution[paraDataTypes.length];
		fullLifeCycleParaGenerators = new DataAccessDistribution[paraDataTypes.length];
		sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	}

	// 深拷贝主要是为了保证 数据库执行器 不被覆盖
	public WriteOperation(WriteOperation writeOperation) {
		super();
		this.operationId = writeOperation.operationId;
		this.sql = writeOperation.sql;
		this.paraDataTypes = writeOperation.paraDataTypes;
		this.paraDistTypeInfos = writeOperation.paraDistTypeInfos;

		if (writeOperation.paraSchemaInfos != null)
			this.paraSchemaInfos = new ArrayList<>(writeOperation.paraSchemaInfos);
		this.batchExecute = writeOperation.batchExecute;
		windowParaGenerators = new DataAccessDistribution[paraDataTypes.length];
		fullLifeCycleParaGenerators = new DataAccessDistribution[paraDataTypes.length];
		sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	}

	public boolean isBatchExecute() {
		return batchExecute;
	}

	@Override
	public int execute(Map<String, Integer> cardinality4paraInSchema, Map<String, Set<Object>> paraUsed) {
//		long startTime = System.currentTimeMillis();
//		long para = -1; //lyqu: 照理说不应该为long类型，而是int主键类型
//		LaucaTestingEnv.writeOperationTimes.getAndIncrement();
		try {

			for (int i = 0; i < paraDataTypes.length; i++) {

//				if(i == paraDataTypes.length-1){
//					para = (long)parameter;
//				}

				Object parameter = checkParaOutOfCardinality(
						geneParameter(i),
						this.paraSchemaInfos.get(i),
						cardinality4paraInSchema,
						paraUsed
				);

				setParameter(i + 1, paraDataTypes[i], parameter);

			}
//			long endTime = System.currentTimeMillis();
//			LaucaTestingEnv.geneTime += endTime-startTime;

			if (batchExecute) {
//				System.out.println("I am in WriteOperation batchExecute");
				pstmt.addBatch();
			} else {
//				long startTime1 = System.currentTimeMillis();
				pstmt.executeUpdate();
//				long endTime1 = System.currentTimeMillis();
//				LaucaTestingEnv.updateTime += endTime1 - startTime1;


//				if(rowCount == 0){
//					LaucaTestingEnv.noUpdateRowCount.getAndIncrement();
////					System.out.println(para);
//				}
//				else if(rowCount == 1){
//					LaucaTestingEnv.updateRowCount.getAndIncrement();
//
//				}
//				else{
//					LaucaTestingEnv.moreUpdateRowCount.getAndIncrement();
//				}
			}
			return 1;
		} catch (Exception e) {
//			return -1;
			if (e instanceof  SQLException) {
				return -1;
			}
//			e.printStackTrace();
//			System.err.println("ERROR!!!");
			System.out.println("aaaaaaaa"+pstmt.toString());
			System.out.println(this.getClass().getName());
			System.out.println(sql);
//			System.out.println(e.getMessage());
////			System.exit(1);
			return 0;
		}
	}

	// add by wsy
	public List<String> getParaSchemaInfos(){
		return paraSchemaInfos;
	}

	@Override
	public int execute(Map<String, Integer> cardinality4paraInSchema, Map<String, Set<Object>> paraUsed, Statement stmt) {
		
		String tmp = sql; // 方便程序调试
		
		try {
			// String tmp = sql;
			for (int i = 0; i < paraDataTypes.length; i++) {

				Object parameter = checkParaOutOfCardinality(
						geneParameter(i),
						this.paraSchemaInfos.get(i),
						cardinality4paraInSchema,
						paraUsed
				);

				if (paraDataTypes[i] == 3) {
					tmp = tmp.replaceFirst("\\?", " '" + sdf.format(new Date((Long)parameter)) + "' ");
				} else if (paraDataTypes[i] == 4) {
					tmp = tmp.replaceFirst("\\?", " '" + parameter.toString() + "' ");
				} else {
					tmp = tmp.replaceFirst("\\?", " " + parameter.toString() + " ");
				}
			}
			// 非预编译执行不存在批处理的策略
			stmt.executeUpdate(tmp);
			return 1;
		} catch (SQLException e) {
//			e.printStackTrace();
//			System.out.println(tmp);
//			System.exit(0);
			return -1;
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

	@Override
	public int execute(Map<String, Integer> cardinality4paraInSchema, Map<String, Set<Object>> paraUsed,
					   Map<String, Double> multipleLogicMap, int round) {
		try {
			for (int i = 0; i < paraDataTypes.length; i++) {
				Object parameter = checkParaOutOfCardinality(
						geneParameterByMultipleLogic(i, multipleLogicMap, round),
						this.paraSchemaInfos.get(i),
						cardinality4paraInSchema,
						paraUsed
				);

				setParameter(i + 1, paraDataTypes[i], parameter);
			}
			if (batchExecute) {
				pstmt.addBatch();
			} else {

				pstmt.executeUpdate();

			}
			return 1;
		} catch (SQLException e) {
//			e.printStackTrace();
			if (e.getMessage().contains("Deadlock")) {
				return -1;
			}
			System.out.println(this.getClass().getName());
			System.out.println(sql);
			System.out.println(e.getMessage());
//			System.exit(1);
//			System.err.println("ERROR!!!");
			return 0;
		}
	}

	@Override
	public int execute(Map<String, Integer> cardinality4paraInSchema, Map<String, Set<Object>> paraUsed,
					   Statement stmt, Map<String, Double> multipleLogicMap, int round) {
		try {
			String tmp = sql;
			for (int i = 0; i < paraDataTypes.length; i++) {
				Object parameter = checkParaOutOfCardinality(
						geneParameterByMultipleLogic(i, multipleLogicMap, round),
						this.paraSchemaInfos.get(i),
						cardinality4paraInSchema,
						paraUsed
				);
				if (paraDataTypes[i] == 3) {
					tmp = tmp.replaceFirst("\\?", " '" + sdf.format(new Date((Long)parameter)) + "' ");
				} else if (paraDataTypes[i] == 4) {
					tmp = tmp.replaceFirst("\\?", " '" + parameter.toString() + "' ");
				} else {
					tmp = tmp.replaceFirst("\\?", " " + parameter.toString() + " ");
				}
			}
			stmt.executeUpdate(tmp);
			return 1;
		} catch (SQLException e) {
//			e.printStackTrace();
			return -1;

		}
	}

	public int executeBatch() {
		try {
			if (batchExecute) {
				pstmt.executeBatch();

//				pstmt.clearBatch();
//				int totalCount = 0; //一般是10
//				for(int i = 0; i < rowCount.length;i++){
//					totalCount += rowCount[i];
//				}

//				LaucaTestingEnv.multipleUpdateRowCount.addAndGet(totalCount);
//				if(totalCount == 10){
//					LaucaTestingEnv.multipleUpdateRowCount.addAndGet(10);
//				}else{
////					System.out.println("totalCount: "+totalCount);
////					System.out.println("rowCount Length： "+rowCount.length);
////					for(int k=0;k<rowCount.length;++k){
////						if(rowCount[k]!=1){
////							System.out.println("一批中第几个sql:"+k);
////						}
////					}
//
//
//					LaucaTestingEnv.multipleNoUpdateRowCount.addAndGet(totalCount);
//				}
				//multiple中update&insert影响的条数,一般是10.

			}
			return 1;
		} catch (SQLException e) {
			return -1;
		}
	}

	@Override
	public String toString() {
		return "\n\t\tWriteOperation [operationId=" + operationId + ", batchExecute=" + batchExecute + 
				", sql=" + sql + ", paraDataTypes=" + Arrays.toString(paraDataTypes) + ", paraDistTypeInfos=" + 
				Arrays.toString(paraDistTypeInfos) + "]";
	}
}
