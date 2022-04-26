package transactionlogic;

import java.util.Arrays;
import java.util.List;

/**
 * 参数依赖关系图中的参数节点，对于数值完全相同的参数，它们共用一个参数节点对象
 */
public class ParameterNode {

	// 该列表中可能含有多个参数标识符，相应参数的数值是完全相同的
	private List<String> identifiers = null;

	// 所有依赖项。该列表可为空，表示不依赖于其他参数或者返回结果集元素。注意：所有依赖项中的概率之和必须小于等于1
	// **bug fix：当前dependencies中仅含等于和包含依赖关系，线性依赖关系单独存放于linearDependencies中
	private List<ParameterDependency> dependencies = null;

	// **bug fix：下面两个成员仅针对“等于”和“包含”依赖关系~
	// 累积概率和：为方便数据生成时确定依赖关系
	private double[] cumulativeProbabilities = null;
	// 所有依赖关系的概率和，剩余概率（1-probabilitySum）应根据相应数据分布生成参数
	private double probabilitySum;
	
	// **bug fix：后面追加的成员
	// 存放所有线性依赖关系，因为目前的线性依赖关系是假设依赖项不为空的，因此当依赖项位于分支逻辑中时，相应线性依赖关系可能不可用~
	private List<ParameterDependency> linearDependencies = null;
	
	
	public ParameterNode(List<String> identifiers) {
		super();
		this.identifiers = identifiers;
	}

	public List<String> getIdentifiers() {
		return identifiers;
	}
	
	public void setDependencies(List<ParameterDependency> dependencies) {
		this.dependencies = dependencies;
	}

	public List<ParameterDependency> getDependencies() {
		return dependencies;
	}

	public double[] getCumulativeProbabilities() {
		return cumulativeProbabilities;
	}
	
	public double getProbabilitySum() {
		return probabilitySum;
	}

	// 在需要利用参数依赖关系生成模拟负载前调用该函数~
	public void initCumulativeProbabilities() {
		cumulativeProbabilities = new double[dependencies.size()];
		if (dependencies.size() == 0) {
			probabilitySum = 0;
			return;
		}
		cumulativeProbabilities[0] = dependencies.get(0).getProbability();
		for (int i = 1; i < dependencies.size(); i++) {
			cumulativeProbabilities[i] = cumulativeProbabilities[i - 1] + dependencies.get(i).getProbability();
		}
		
		probabilitySum = cumulativeProbabilities[cumulativeProbabilities.length - 1];
	}

	public void setLinearDependencies(List<ParameterDependency> linearDependencies) {
		this.linearDependencies = linearDependencies;
	}
	
	public List<ParameterDependency> getLinearDependencies() {
		return linearDependencies;
	}

	@Override
	public String toString() {
		return "ParameterNode [identifiers=" + identifiers + ", dependencies=" + dependencies
				+ ", cumulativeProbabilities=" + Arrays.toString(cumulativeProbabilities) + ", probabilitySum="
				+ probabilitySum + ", linearDependencies=" + linearDependencies + "]";
	}
}
