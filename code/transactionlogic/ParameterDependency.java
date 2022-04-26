package transactionlogic;

/**
 * 参数的依赖关系，可表示三类依赖关系：等于依赖关系、包含依赖关系和线性依赖关系
 */
public class ParameterDependency {

	// 依赖的对象，可能是一个输入参数，也可能是一个返回结果集元素
	private String identifier = null;

	// 该依赖关系所占的比例（出现的概率）
	private double probability;

	// 依赖类型。0：相等；1：包含；2：线性关系
	private int dependencyType;

	// 若是线性关系，需要给出线性关系的表达式系数。若不是线性关系，下面两个变量可忽略
	private double a, b;

	public ParameterDependency(String identifier, double probability, int dependencyType) {
		super();
		this.identifier = identifier;
		this.probability = probability;
		this.dependencyType = dependencyType;
	}

	public ParameterDependency(String identifier, double probability, int dependencyType, double a, double b) {
		super();
		this.identifier = identifier;
		this.probability = probability;
		this.dependencyType = dependencyType;
		this.a = a;
		this.b = b;
	}

	public String getIdentifier() {
		return identifier;
	}

	public double getProbability() {
		return probability;
	}

	public int getDependencyType() {
		return dependencyType;
	}

	public double getCoefficientA() {
		return a;
	}

	public double getCoefficientB() {
		return b;
	}

	@Override
	public String toString() {
		return "ParameterDependency [identifier=" + identifier + ", probability=" + probability + ", dependencyType="
				+ dependencyType + ", a=" + a + ", b=" + b + "]";
	}
}
