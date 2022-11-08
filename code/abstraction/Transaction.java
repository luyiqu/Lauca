package abstraction;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import accessdistribution.DataAccessDistribution;
import transactionlogic.ParameterNode;
import workloadgenerator.LaucaTestingEnv;

public class Transaction{

	private String name = null;
	private double ratio;
	private boolean prepared;
	private List<TransactionBlock> transactionBlocks = null;

	// -------------------------
	// 在有些应用负载中（如smallbank），可能会在某些操作后主动回滚事务，这个主动回滚一般是根据某些条件判断进行的。
	// 但是我们不关注条件判断，我们仅关注操作后事务回滚的概率有多大，因此我们利用rollbackProbabilities
	// 来记录每个操作之后发生事务回滚的概率。
	// 目前仅支持最外一层TransactionBlock的主动事务回滚...
	// 目前我们仅针对smallbank负载启用该属性

	private double[] rollbackProbabilities = null;

	// -------------------------

	// 下面三个属性是分析统计得到的事务逻辑信息
	// 参数的依赖关系（等于、包含、线性），尽可能保证模拟负载的事务逻辑与真实负载一致
	private Map<String, ParameterNode> parameterNodeMap = null;
	// Multiple块内SQL操作的逻辑（目前仅考虑SQL输入参数是否保持不变或者单调改变）
	private Map<String, Double> multipleLogicMap = null;
	// 以访问的列为单位统计的基数
	private Map<String, Integer> cardinality4paraInSchema = null;
	// 操作ID -> 平均执行次数，用来确定：if/else分支执行比例，multiple内操作平均执行次数
	private Map<Integer, Double> operationId2AvgRunTimes = null;

	private Connection conn = null;
	private Statement stmt = null;

	// 记录事务执行过程中的中间状态，如返回结果集以及根据数据分布生成的SQL参数值
	private Map<String, TxRunningValue> intermediateState = null;

//	private Logger logger = Logger.getLogger(Transaction.class);


	public Transaction(String name, double ratio, boolean prepared, List<TransactionBlock> transactionBlocks) {
		super();
		this.name = name;
		this.ratio = ratio;
		this.prepared = prepared;
		this.transactionBlocks = transactionBlocks;
	}

	// 设置事务逻辑信息
	public void setTransactionLogicInfo(Map<String, ParameterNode> parameterNodeMap,
			Map<String, Double> multipleLogicMap, Map<Integer, Double> operationId2AvgRunTimes,
										Map<String, Integer> cardinality4paraInSchema) {
		this.parameterNodeMap = parameterNodeMap;
		this.multipleLogicMap = multipleLogicMap;
		this.operationId2AvgRunTimes = operationId2AvgRunTimes;
		this.cardinality4paraInSchema = cardinality4paraInSchema;
	}

	// 深拷贝Transaction对象，主要保证数据库操作执行器（stmt、pstmt）是独立的
	public Transaction(Transaction transaction) {
		super();
		this.name = transaction.name;
		this.ratio = transaction.ratio;
		this.prepared = transaction.prepared;
		this.transactionBlocks = new ArrayList<>();

		for (TransactionBlock txBlock : transaction.transactionBlocks) {
			if (txBlock.getClass().getSimpleName().equals("Multiple")) {
				this.transactionBlocks.add(new Multiple((Multiple) txBlock));
			} else if (txBlock.getClass().getSimpleName().equals("Branch")) {
				this.transactionBlocks.add(new Branch((Branch) txBlock));
			} else if (txBlock.getClass().getSimpleName().equals("ReadOperation")) {
				this.transactionBlocks.add(new ReadOperation((ReadOperation) txBlock));
			} else if (txBlock.getClass().getSimpleName().equals("WriteOperation")) {
				this.transactionBlocks.add(new WriteOperation((WriteOperation) txBlock));
			}
		}

		// 这些都是只读数据
		this.rollbackProbabilities = transaction.rollbackProbabilities;
		this.parameterNodeMap = transaction.parameterNodeMap;
		this.multipleLogicMap = transaction.multipleLogicMap;
		this.operationId2AvgRunTimes = transaction.operationId2AvgRunTimes;
	}

	public void init(Connection conn) {
		this.conn = conn;
		if (prepared) {
			for (int i = 0; i < transactionBlocks.size(); i++) {
				transactionBlocks.get(i).prepare(conn);
			}
		} else {
			try {
				stmt = conn.createStatement();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		init();
	}

	// 将intermediateState、parameterNodeMap传给每一个SqlStatement对象，将multipleLogicMap传给所有Multiple对象
	// 通过operationId2AvgRunTimes计算：if/else分支执行比例，multiple块内操作平均执行次数
	private void init() {
//		System.out.println("Transaction Init");
		intermediateState = new HashMap<>();
		for (TransactionBlock txBlock : transactionBlocks) {
			if (txBlock.getClass().getSimpleName().equals("Multiple")) {
//				System.out.println("Multiple DEBUG");
				Multiple multiple = (Multiple) txBlock;
				List<SqlStatement> sqls = multiple.getSqls();
				for (SqlStatement sql : sqls) {
					sql.setIntermediateState(intermediateState);
					sql.setParameterNodeMap(parameterNodeMap);
				}
				multiple.setMultipleLogicMap(multipleLogicMap);
				int operationId = multiple.getSqls().get(0).getOperationId();

				// bug fix: 事务模板中的某些事务可能没有实例数据
				if (operationId2AvgRunTimes == null) {
					continue;
				}

				multiple.setAvgRunTimes(operationId2AvgRunTimes.get(operationId));
			} else if (txBlock.getClass().getSimpleName().equals("Branch")) {
				Branch branch = (Branch) txBlock;
				List<List<SqlStatement>> branches = branch.getBranches();
				for (int i = 0; i < branches.size(); i++) {
					for (int j = 0; j < branches.get(i).size(); j++) {
						branches.get(i).get(j).setIntermediateState(intermediateState);
						branches.get(i).get(j).setParameterNodeMap(parameterNodeMap);
					}
				}
				double[] branchRatios = new double[branches.size()];
				for (int i = 0; i < branches.size(); i++) {
					int operationId = branches.get(i).get(0).getOperationId();

					// bug fix: 事务模板中的某些事务可能没有实例数据
					if (operationId2AvgRunTimes == null) {
						continue;
					}

					branchRatios[i] = operationId2AvgRunTimes.get(operationId);
				}
				branch.setBranchRatios(branchRatios);
			} else {
				txBlock.setIntermediateState(intermediateState);
				txBlock.setParameterNodeMap(parameterNodeMap);
			}
		}
	}

	public float exectue(){
		intermediateState.clear();

		long startTime = System.nanoTime();
		int flag = 1;

		Map<String, Set<Object>> paraUsed = new HashMap<>();
		for (String para : cardinality4paraInSchema.keySet()){
			paraUsed.put(para, new HashSet<>());
		}


//		if(transactionBlocks.size()!=1){
//			System.out.println(transactionBlocks.size());
//			for(int i = 0;i<transactionBlocks.size();i++){
//				System.out.println(transactionBlocks.get(i));
//			}
//
//		}
		for (int i = 0; i < transactionBlocks.size(); i++) {
			if (prepared) {
				flag = transactionBlocks.get(i).execute(cardinality4paraInSchema, paraUsed);
				if (flag != 1) {
//					System.out.println("prepared"+this.name+" "+i);
					break;
				}
			} else {

				flag = transactionBlocks.get(i).execute(cardinality4paraInSchema, paraUsed, stmt);
//				System.out.println("NoPrepared: "+transactionBlocks.get(i));
				if (flag != 1) {
					break;
				}
			}

			// mainly for smallbank workload. 针对其他负载，rollbackProbabilities[i]都为0
			if (Math.random() < rollbackProbabilities[i]) {
				flag = 0;
				break;
			}
			// -------------------
		}

		try {
			if (flag == 1) {
				this.cleanBatch(transactionBlocks);
				conn.commit();
			} else {
				this.cleanBatch(transactionBlocks);
				conn.rollback();
			}
		} catch (Exception e) {  //lyqu : 将SQLException换为Exception
			e.printStackTrace();
			try {
				flag = 0;
				conn.rollback();
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		}

		// 像oltp-bench的事务机制看齐?
//		try {
//			conn.commit();
//		} catch (SQLException e) {
//			e.printStackTrace();
//		}

		// 事务响应时间，单位为ms
		float responceTime = 0;
		if (flag == 1) {

			responceTime = (System.nanoTime() - startTime) / 1000000f;
		} else if (flag == -1) {
			responceTime = -1;
		}

		// logger.info("###" + name + "," + flag + "," + responceTime);
		// 若responceTime为-1，表示当前事务因为死锁执行失败；responceTime为0，表示当前事务因为一个非死锁的错误而执行失败
		return responceTime;
	}

	public void setRatio(double ratio) {
		this.ratio = ratio;
	}

	public void setRollbackProbabilities(double[] rollbackProbabilities) {
		this.rollbackProbabilities = rollbackProbabilities;
	}

	public double[] getRollbackProbabilities() {
		return rollbackProbabilities;
	}

	// 设置SQL参数的数据分布。当type为0时，设置的是全负载周期的数据访问分布；当type为1时，设置的是当前时间窗口的数据访问分布
	public void setSqlParaDistribution(Map<String, DataAccessDistribution> paraId2Distribution, int type) {
		for (TransactionBlock txBlock : transactionBlocks) {
			if (txBlock.getClass().getSimpleName().equals("Multiple")) {
				Multiple multiple = (Multiple) txBlock;
				List<SqlStatement> sqls = multiple.getSqls();
				for (int i = 0; i < sqls.size(); i++) {
					sqls.get(i).setParaDistribution(paraId2Distribution, type);
				}
			} else if (txBlock.getClass().getSimpleName().equals("Branch")) {
				Branch branch = (Branch) txBlock;
				List<List<SqlStatement>> branches = branch.getBranches();
				for (int i = 0; i < branches.size(); i++) {
					for (int j = 0; j < branches.get(i).size(); j++) {
						branches.get(i).get(j).setParaDistribution(paraId2Distribution, type);
					}
				}
			} else {
				SqlStatement sqlStatement = (SqlStatement) txBlock;
				sqlStatement.setParaDistribution(paraId2Distribution, type);
			}
		}
	}

	//added b lyqu
	public void cleanBatch(List<TransactionBlock> transactionBlocks){
		for(TransactionBlock transactionBlock:transactionBlocks){
			if(transactionBlock.getClass().getSimpleName().contains("Multiple")){
				Multiple multiple = (Multiple) transactionBlock;
				List<SqlStatement> sqls = multiple.getSqls();
				if (multiple.isBatchExecute()) {
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
				}

			}
		}
	}

	public String getName() {
		return name;
	}

	public double getRatio() {
		return ratio;
	}

	public boolean isPrepared() {
		return prepared;
	}

	public List<TransactionBlock> getTransactionBlocks() {
		return transactionBlocks;
	}

	public Map<Integer, Double> getOperationId2AvgRunTimes() {
		return operationId2AvgRunTimes;
	}

	public Map<String, Double> getMultipleLogicMap() {
		return multipleLogicMap;
	}

	public Map<String, ParameterNode> getParameterNodeMap() {
		return parameterNodeMap;
	}

//	@Override
//	public String toString() {
//		return "\n\tTransaction [name=" + name + ", ratio=" + ratio + ", prepared=" + prepared + ", transactionBlocks="
//				+ transactionBlocks + "]";
//	}

	@Override
	public String toString() {
		return "Transaction [name=" + name + ", ratio=" + ratio + ", prepared=" + prepared + ", transactionBlocks="
				+ transactionBlocks + ", rollbackProbabilities=" + Arrays.toString(rollbackProbabilities)
				+ ", parameterNodeMap=" + parameterNodeMap + ", multipleLogicMap=" + multipleLogicMap
				+ ", operationId2AvgRunTimes=" + operationId2AvgRunTimes + ", conn=" + conn + ", stmt=" + stmt
				+ ", intermediateState=" + intermediateState + "]";
	}

	// added by zsy 用作比较事务模板是否相等
	@Override
	public int hashCode() {
		int result = 0;
		for (TransactionBlock txnB : this.transactionBlocks) {
			result += txnB.hashCode();
		}
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof Transaction)) {
			return false;
		}
		Transaction otherTxn = (Transaction) obj;
		if (this.transactionBlocks.size() != otherTxn.transactionBlocks.size()) {
			return false;
		}
		for (int i = 0; i < this.transactionBlocks.size(); ++i) {
			if (!this.transactionBlocks.get(i).equals(otherTxn.transactionBlocks.get(i))) {
				return false;
			}
		}
		return true;
	}
}

// 事务运行过程中一些中间状态的值，包含SQL操作的输入参数和返回结果集
class TxRunningValue {

	// 标识符，形式为：operationId + "para"/"result" + index
	String identifier = null;

	// 具体数值，因为可能存在不同的数据类型以及数组形式（返回结果集为一组tuple），故采用Object类型
	// 注意这里的value是包装类型
	Object value = null;

	// 0: integer(long); 1: real(double); 2: decimal(BigDecimal);
	// 3: datetime(Date millisecond -- long); 4: varchar; 5: boolean
	// 注意：存进value中的全都是包装类型，对于返回结果集为多个tuple时（value为数组类型）相应的type值加6
	int type;

	public TxRunningValue(String identifier, Object value, int type) {
		super();
		this.identifier = identifier;
		this.value = value;
		this.type = type;
	}

	// 针对包含依赖关系，随机返回结果集中任意一个元素
	public Object getIncludeRelationValue() {
		if (type <= 5) {
			return value;
		}

		Object[] values = (Object[]) value;
		return values[(int) (Math.random() * values.length)];
	}

	// 线性依赖关系，依赖的数据项可能是返回结果集元素，故可能为null
	public Double getLinearRelationValue(double a, double b) {
		if (value == null) {
			return null;
		}

		switch (type) {
		case 0:
		case 3:
			return (Long) value * a + b;
		case 1:
			return (Double) value * a + b;
		case 2:
			return new BigDecimal(value.toString()).doubleValue() * a + b;
		default:
			System.err.println("当前数据类型不支持线性依赖关系！标识符为：" + identifier + "，数据类型为：" + type);
			return null;
		}
	}

	// 注意：具有multiple逻辑的两个参数之间数据类型必然一致（同一个SQL参数的不同次执行）
	public Object getMultipleLogicValue(double increment) {
		// 这里的value是SQL参数，故不可能为null
		double paraValue;
		switch (type) {
		case 0:
		case 3:
			paraValue = (Long) value + increment;
			return new Long((long) paraValue);
		case 1:
			paraValue = (Double) value + increment;
			return new Double(paraValue);
		case 2:
			paraValue = new BigDecimal(value.toString()).doubleValue() + increment;
			return new BigDecimal(paraValue);
		default:
			System.err.println("当前数据类型不支持multiple逻辑！标识符为：" + identifier + "，数据类型为：" + type);
			return null;
		}
	}
}
