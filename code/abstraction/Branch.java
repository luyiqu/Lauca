package abstraction;

import java.sql.Connection;
import java.sql.Statement;
import java.util.*;

public class Branch extends TransactionBlock {

	private List<List<SqlStatement>> branches = null;
	private double[] branchRatios = null;
	private double[] cumulativeRatios = null;

	public Branch(List<List<SqlStatement>> branches) {
		super();
		this.branches = branches;
	}

	// Branch对象的深拷贝
	public Branch(Branch branch) {
		this.branches = new ArrayList<>();
		for (int i = 0; i < branch.branches.size(); i++) {
			this.branches.add(new ArrayList<>());
			for (int j = 0; j < branch.branches.get(i).size(); j++) {
				SqlStatement sql = branch.branches.get(i).get(j);
				if (sql.getClass().getSimpleName().equals("ReadOperation")) {
					this.branches.get(i).add(new ReadOperation((ReadOperation) sql));
				} else if (sql.getClass().getSimpleName().equals("WriteOperation")) {
					this.branches.get(i).add(new WriteOperation((WriteOperation) sql));
				}
			}
		}
	}

	// branchRatios是分析事务逻辑时顺便统计出来的（operationId2AvgRunTimes）
	public void setBranchRatios(double[] branchRatios) {
		this.branchRatios = branchRatios;
		cumulativeRatios = new double[branchRatios.length];
		cumulativeRatios[0] = branchRatios[0];
		for (int i = 1; i < branchRatios.length; i++) {
			cumulativeRatios[i] = cumulativeRatios[i - 1] + branchRatios[i];
		}
	}

	@Override
	public void prepare(Connection conn) {
		for (List<SqlStatement> branch : branches) {
			for (SqlStatement sqlStatement : branch) {
				sqlStatement.prepare(conn);
			}
		}
	}

	@Override
	public int execute(Map<String, Integer> cardinality4paraInSchema, Map<String, Map<Object, List<Object>>> partitionUsed){
		double randomValue = Math.random();
		if (randomValue > 0.99999999) {
			randomValue = randomValue - 0.000000001;
		}

		for (int i = 0; i < cumulativeRatios.length; i++) {
			if (randomValue < cumulativeRatios[i]) {
				for (int j = 0; j < branches.get(i).size(); j++) {
					int flag = branches.get(i).get(j).execute(cardinality4paraInSchema, partitionUsed);
					if (flag != 1) {
						return flag;
					}
				}
				break;
			}
		}

		return 1;
	}

	@Override
	public int execute(Map<String, Integer> cardinality4paraInSchema, Map<String, Map<Object, List<Object>>> partitionUsed, Statement stmt) {
		double randomValue = Math.random();
		if (randomValue > 0.99999999) {
			randomValue = randomValue - 0.000000001;
		}

		for (int i = 0; i < cumulativeRatios.length; i++) {
			if (randomValue < cumulativeRatios[i]) {
				for (int j = 0; j < branches.get(i).size(); j++) {
					int flag = branches.get(i).get(j).execute(cardinality4paraInSchema, partitionUsed, stmt);
					if (flag != 1) {
						return flag;
					}
				}
				break;
			}
		}

		return 1;
	}

	public List<List<SqlStatement>> getBranches() {
		return branches;
	}

	@Override
	public Map<String, String> getParaId2Name() {
		Map<String, String> paraId2Name = new HashMap<>();
		for (List<SqlStatement> branch: branches ) {
			for (SqlStatement sql : branch) {
				paraId2Name.putAll(sql.getParaId2Name());
			}
		}
		return paraId2Name;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for (List<SqlStatement> branch : branches) {
			sb.append(branch);
			sb.append("\n\t\t---------------------------------");
		}
		return "\n\t\tBranch [branchRatios=" + Arrays.toString(branchRatios) + ", branches=" + sb.toString() + "]";
	}

	// added by zsy 用作比较事务模板是否相等,其实branch中的暂时无用，只是为了完整性
	@Override
	public int hashCode() {
		int result = 0;
		for (List<SqlStatement> sqlList : this.branches) {
			for(SqlStatement sql:sqlList) {
				result = result + sql.hashCode();
			}
		}
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof Branch)) {
			return false;
		}
		Branch otherBranch = (Branch) obj;
		if (this.branches.size() != otherBranch.branches.size()) {
			return false;
		}
		for (int i = 0; i < this.branches.size(); ++i) {
			if (this.branches.get(i).size()!=otherBranch.branches.get(i).size()) {
				return false;
			}
			for(int j=0;j<this.branches.get(i).size();++j) {
				if(!this.branches.get(i).get(j).equals(otherBranch.branches.get(i).get(j))) {
					return false;
				}
			}
		}
		return true;
	}
}
