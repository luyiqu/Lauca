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

	public void constructOperationTemplateAndDistInfo(List<Transaction> transactions) {
		txName2OperationId2Template = new HashMap<>();
		txName2OperationId2paraDistTypeInfos = new HashMap<>();

		for (int i = 0; i < transactions.size(); i++) {
			Transaction tx = transactions.get(i);
			txName2OperationId2Template.put(tx.getName(), new HashMap<>());
			txName2OperationId2paraDistTypeInfos.put(tx.getName(), new HashMap<>());

			int operationId = 1; // 后期我们在SqlStatement中维护了operationId~
			for (int j = 0; j < tx.getTransactionBlocks().size(); j++) {
				String className = tx.getTransactionBlocks().get(j).getClass().getName();

				if (className.equals("abstraction.ReadOperation")) {
					ReadOperation op = (ReadOperation)tx.getTransactionBlocks().get(j);
					txName2OperationId2Template.get(tx.getName()).put(operationId, new OperationData(operationId, 
							op.getReturnDataTypes(), op.isFilterPrimaryKey(), op.getParaDataTypes()));
					txName2OperationId2paraDistTypeInfos.get(tx.getName()).put(operationId, op.getParaDistTypeInfos());
					operationId++;

				} else if (className.equals("abstraction.WriteOperation")) {
					WriteOperation op = (WriteOperation)tx.getTransactionBlocks().get(j);
					txName2OperationId2Template.get(tx.getName()).put(operationId, new OperationData(operationId, 
							null, false, op.getParaDataTypes()));
					txName2OperationId2paraDistTypeInfos.get(tx.getName()).put(operationId, op.getParaDistTypeInfos());
					operationId++;

				} else if (className.equals("abstraction.Multiple")) {
					Multiple multiple = (Multiple)tx.getTransactionBlocks().get(j);
					List<SqlStatement> sqls = multiple.getSqls();

					for (int k = 0; k < sqls.size(); k++) {
						String className2 = sqls.get(k).getClass().getName();
						if (className2.equals("abstraction.ReadOperation")) {
							ReadOperation op = (ReadOperation)sqls.get(k);
							txName2OperationId2Template.get(tx.getName()).put(operationId, new OperationData(operationId, 
									op.getReturnDataTypes(), op.isFilterPrimaryKey(), op.getParaDataTypes()));
							txName2OperationId2paraDistTypeInfos.get(tx.getName()).put(operationId, op.getParaDistTypeInfos());
							operationId++;
						} else if (className2.equals("abstraction.WriteOperation")) {
							WriteOperation op = (WriteOperation)sqls.get(k);
							txName2OperationId2Template.get(tx.getName()).put(operationId, new OperationData(operationId, 
									null, false, op.getParaDataTypes()));
							txName2OperationId2paraDistTypeInfos.get(tx.getName()).put(operationId, op.getParaDistTypeInfos());
							operationId++;
						}
					}
				} else if (className.equals("abstraction.Branch")) {
					Branch branch = (Branch)tx.getTransactionBlocks().get(j);
					List<List<SqlStatement>> branches = branch.getBranches();

					for (int k = 0; k < branches.size(); k++) {
						List<SqlStatement> sqls = branches.get(k);
						for (int m = 0; m < sqls.size(); m++) {
							String className2 = sqls.get(m).getClass().getName();
							if (className2.equals("abstraction.ReadOperation")) {
								ReadOperation op = (ReadOperation)sqls.get(m);
								txName2OperationId2Template.get(tx.getName()).put(operationId, new OperationData(operationId, 
										op.getReturnDataTypes(), op.isFilterPrimaryKey(), op.getParaDataTypes()));
								txName2OperationId2paraDistTypeInfos.get(tx.getName()).put(operationId, op.getParaDistTypeInfos());
								operationId++;
							} else if (className2.equals("abstraction.WriteOperation")) {
								WriteOperation op = (WriteOperation)sqls.get(m);
								txName2OperationId2Template.get(tx.getName()).put(operationId, new OperationData(operationId, 
										null, false, op.getParaDataTypes()));
								txName2OperationId2paraDistTypeInfos.get(tx.getName()).put(operationId, op.getParaDistTypeInfos());
								operationId++;
							}
						}
					}
				}
			}
		}
	}

	public Map<String, Map<Integer, OperationData>> getTxName2OperationId2Template() {
		return txName2OperationId2Template;
	}

	public Map<String, Map<Integer, DistributionTypeInfo[]>> getTxName2OperationId2paraDistTypeInfos() {
		return txName2OperationId2paraDistTypeInfos;
	}
}
