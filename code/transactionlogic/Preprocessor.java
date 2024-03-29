package transactionlogic;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import abstraction.Branch;
import abstraction.Multiple;
import abstraction.ReadOperation;
import abstraction.SqlStatement;
import abstraction.Transaction;
import abstraction.WriteOperation;
import accessdistribution.DistributionTypeInfo;

public class Preprocessor {

	private Map<String, Map<Integer, OperationData>> txName2OperationId2Template = null;
	private Map<String, Map<Integer, DistributionTypeInfo[]>> txName2OperationId2paraDistTypeInfos = null;

	private Map<String, Map<Integer, List<String>>> txName2OperationId2paraSchema = null;

	public void constructOperationTemplateAndDistInfo(List<Transaction> transactions) {
		txName2OperationId2Template = new HashMap<>();
		txName2OperationId2paraDistTypeInfos = new HashMap<>();
		txName2OperationId2paraSchema = new HashMap<>();

		for (Transaction tx : transactions) {
			txName2OperationId2Template.put(tx.getName(), new HashMap<>());
			txName2OperationId2paraDistTypeInfos.put(tx.getName(), new HashMap<>());
			txName2OperationId2paraSchema.put(tx.getName(), new HashMap<>());

			int operationId = 1; // 后期我们在SqlStatement中维护了operationId~
			for (int j = 0; j < tx.getTransactionBlocks().size(); j++) {
				String className = tx.getTransactionBlocks().get(j).getClass().getName();

				switch (className) {
					case "abstraction.ReadOperation": {
						ReadOperation op = (ReadOperation) tx.getTransactionBlocks().get(j);
						txName2OperationId2Template.get(tx.getName()).put(operationId, new OperationData(operationId,
								op.getReturnDataTypes(), op.isFilterPrimaryKey(), op.getParaDataTypes()));
						txName2OperationId2paraDistTypeInfos.get(tx.getName()).put(operationId, op.getParaDistTypeInfos());
						txName2OperationId2paraSchema.get(tx.getName()).put(operationId, op.getParaSchemaInfos());
						operationId++;

						break;
					}
					case "abstraction.WriteOperation": {
						WriteOperation op = (WriteOperation) tx.getTransactionBlocks().get(j);
						txName2OperationId2Template.get(tx.getName()).put(operationId, new OperationData(operationId,
								null, false, op.getParaDataTypes()));
						txName2OperationId2paraDistTypeInfos.get(tx.getName()).put(operationId, op.getParaDistTypeInfos());
						txName2OperationId2paraSchema.get(tx.getName()).put(operationId, op.getParaSchemaInfos());
						operationId++;

						break;
					}
					case "abstraction.Multiple": {
						Multiple multiple = (Multiple) tx.getTransactionBlocks().get(j);
						for (SqlStatement sql : multiple.getSqls()) {
							operationId = constructSingleOp(tx, operationId, sql);
						}
						break;
					}
					case "abstraction.Branch": {
						Branch branch = (Branch) tx.getTransactionBlocks().get(j);
						List<List<SqlStatement>> branches = branch.getBranches();

						for (List<SqlStatement> sqls : branches) {
							for (SqlStatement sql : sqls) {
								operationId = constructSingleOp(tx, operationId, sql);
							}
						}
						break;
					}
				}
			}
		}
	}

	/**
	 * 构建单个sql的模板，可处理write op 或 read op
	 * @param tx sql从属的事务
	 * @param operationId 当前操作的id，这是一个递增序列
	 * @param sql 需要构建模板的sql
	 * @return 更新后的operationId
	 */
	private int constructSingleOp(Transaction tx, int operationId, SqlStatement sql) {
		String className = sql.getClass().getName();
		if (className.equals("abstraction.ReadOperation")) {
			ReadOperation op = (ReadOperation) sql;
			txName2OperationId2Template.get(tx.getName()).put(operationId, new OperationData(operationId,
					op.getReturnDataTypes(), op.isFilterPrimaryKey(), op.getParaDataTypes()));
			txName2OperationId2paraDistTypeInfos.get(tx.getName()).put(operationId, op.getParaDistTypeInfos());
			txName2OperationId2paraSchema.get(tx.getName()).put(operationId, op.getParaSchemaInfos());
			operationId++;
		} else if (className.equals("abstraction.WriteOperation")) {
			WriteOperation op = (WriteOperation) sql;
			txName2OperationId2Template.get(tx.getName()).put(operationId, new OperationData(operationId,
					null, false, op.getParaDataTypes()));
			txName2OperationId2paraDistTypeInfos.get(tx.getName()).put(operationId, op.getParaDistTypeInfos());
			txName2OperationId2paraSchema.get(tx.getName()).put(operationId, op.getParaSchemaInfos());
			operationId++;
		}
		return operationId;
	}

	public Map<String, Map<Integer, OperationData>> getTxName2OperationId2Template() {
		return txName2OperationId2Template;
	}

	public Map<String, Map<Integer, DistributionTypeInfo[]>> getTxName2OperationId2paraDistTypeInfos() {
		return txName2OperationId2paraDistTypeInfos;
	}

	public Map<String, Map<Integer, List<String>>> getTxName2OperationId2paraSchema() {
		return txName2OperationId2paraSchema;
	}
}
