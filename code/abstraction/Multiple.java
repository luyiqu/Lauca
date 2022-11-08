package abstraction;

import com.sun.javaws.IconUtil;
import workloadgenerator.LaucaTestingEnv;

import javax.swing.plaf.IconUIResource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Multiple extends TransactionBlock {

	private List<SqlStatement> sqls = null;
	private boolean batchExecute;

	//20210127... 在测试环境中才初始化
	private double avgRunTimes;

	// multiple块内数据库操作的逻辑（只考虑SQL输入参数的逻辑）
	// Key："multiple"_operationId_"para"_paraIndex
	// Value：0表示该参数在multiple的多次执行中保持不变，n表示该参数在multiple中每次执行后单调增n
	private Map<String, Double> multipleLogicMap = null;

	public Multiple(List<SqlStatement> sqls, boolean batchExecute) {
		super();
		this.sqls = sqls;
		this.batchExecute = batchExecute;
	}

	// Multiple对象深拷贝
	public Multiple(Multiple multiple) {
		this.sqls = new ArrayList<>();
		for (SqlStatement sql : multiple.sqls) {
			if (sql.getClass().getSimpleName().equals("ReadOperation")) {
				this.sqls.add(new ReadOperation((ReadOperation) sql));
			} else if (sql.getClass().getSimpleName().equals("WriteOperation")) {
				this.sqls.add(new WriteOperation((WriteOperation) sql));
			}
		}
		this.batchExecute = multiple.batchExecute;
	}

	public boolean isBatchExecute() {
		return batchExecute;
	}

	// avgRunTimes是分析事务逻辑时顺便统计出来的（operationId2AvgRunTimes）
	public void setAvgRunTimes(double avgRunTimes) {
		this.avgRunTimes = avgRunTimes;
	}

	public double getAvgRunTimes() {
		return avgRunTimes;
	}

	public void setMultipleLogicMap(Map<String, Double> multipleLogicMap) {
		this.multipleLogicMap = multipleLogicMap;
	}

	@Override
	public void prepare(Connection conn) {
		for (int i = 0; i < sqls.size(); i++) {
			sqls.get(i).prepare(conn);
		}
	}

	@Override
	public int execute(Map<String, Integer> cardinality4paraInSchema, Map<String, Set<Object>> paraUsed){
		//modified by lyqu for debug
		double decimalPart = avgRunTimes % 1;
		int runTimes = (int) avgRunTimes;

		if (Math.random() < decimalPart) {
			runTimes += 1;
		}
//		int runTimes = 10;

//		System.out.println("runTimes: "+runTimes);
		for (int i = 0; i < runTimes; i++) {
			if (i == 0) { // multiple块内操作的第一次执行，无需考虑multiple逻辑
				for (int j = 0; j < sqls.size(); j++) {
					int flag = sqls.get(j).execute(cardinality4paraInSchema, paraUsed);
					if (flag != 1) {
						return flag;
					}
				}
			} else { // 非第一次执行，此时块内操作的执行需考虑multiple逻辑
				for (int j = 0; j < sqls.size(); j++) {
					int flag = sqls.get(j).execute(cardinality4paraInSchema, paraUsed, multipleLogicMap, i);
					if (flag != 1) {
						return flag;
					}
				}
			}
		}

//		System.out.println("*************** I am out batchExecute *********************");
		// 只有在预编译执行时才可能为batch execute
		if (batchExecute) {
//			System.out.println("*************** I am in batchExecute *********************");
			for (int i = 0; i < sqls.size(); i++) {
				if (sqls.get(i).getClass().getSimpleName().equals("WriteOperation")) {
//					System.out.println("*************** I am in IF WriteOperation *********************");
					int flag = ((WriteOperation) sqls.get(i)).executeBatch();
					try {
						sqls.get(i).pstmt.clearBatch();
					} catch (SQLException e) {
						e.printStackTrace();
					}
					if (flag != 1) {

						// bug fix: clearBatch
						for (int j = 0; j < sqls.size(); j++) {
							if (sqls.get(j).getClass().getSimpleName().equals("WriteOperation")) {
								try {
									sqls.get(j).pstmt.clearBatch();
								} catch (SQLException e) {
									e.printStackTrace();
								}
							}
						} // --------

						return flag;
					}
				}
			}
		}

		return 1;
	}

	@Override
	public int execute(Map<String, Integer> cardinality4paraInSchema, Map<String, Set<Object>> paraUsed, Statement stmt) {
		double decimalPart = avgRunTimes % 1;
		//modified by lyqu
//		System.out.println("2222");
//		int runTimes = (int)(Math.random()*11)+5;
		int runTimes = (int) avgRunTimes;
		//-----------

		if (Math.random() < decimalPart) {
			runTimes += 1;
		}

		for (int i = 0; i < runTimes; i++) {
			if (i == 0) {
				for (int j = 0; j < sqls.size(); j++) {
					int flag = sqls.get(j).execute(cardinality4paraInSchema, paraUsed, stmt);
					if (flag != 1) {
						return flag;
					}
				}
			} else {
				for (int j = 0; j < sqls.size(); j++) {
					int flag = sqls.get(j).execute(cardinality4paraInSchema, paraUsed, stmt, multipleLogicMap, i);
					if (flag != 1) {
						return flag;
					}
				}
			}
		}

		return 1;
	}

	public List<SqlStatement> getSqls() {
		return sqls;
	}

	@Override
	public String toString() {
		return "\n\t\tMultiple [avgRunTimes=" + avgRunTimes + ", batchExecute=" + batchExecute + ", sqls=" + sqls
				+ "\n\t\t---------------------------------]";
	}

	// added by zsy 用作比较事务模板是否相等
	@Override
	public int hashCode() {
		int result = 0;
		for(SqlStatement sql:this.sqls) {
			result = result + sql.hashCode();
		}
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if(!(obj instanceof Multiple)) {
			return false;
		}
		Multiple otherLoop = (Multiple)obj;
		if(this.sqls.size()!=otherLoop.sqls.size()) {
			return false;
		}
		for(int i=0;i<this.sqls.size();++i) {
			if(!this.sqls.get(i).equals(otherLoop.sqls.get(i))) {
				return false;
			}
		}
		return true;
	}
}
