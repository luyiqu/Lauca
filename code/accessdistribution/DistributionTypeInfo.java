package accessdistribution;

//参数的数据分布类型 信息类
public class DistributionTypeInfo {
	// 参数数据分布类型
	// 0：ContinuousParaDistribution（1: real(double); 2: decimal(BigDecimal); 3: datetime(long);）；1：IntegerParaDistribution；2：VarcharParaDistribution；
	// 3：SequentialCtnsParaDistribution（主键和外键）；4：SequentialIntParaDistribution（int）；5：SequentialVcharParaDistribution(varchar)
	int distributionType;

	// distributionType=0 时，需额外指定参数数据类型
	int dataType;

	// 可能需要的属性信息
	long columnMinValue, columnMaxValue;
	long columnCardinality;
	double coefficient;
	int minLength, maxLength;
	String[] seedStrings = null;

	// distributionType = 0, 3（3其实不需要dataType）
	public DistributionTypeInfo(int distributionType, int dataType) {
		super();
		this.distributionType = distributionType;
		this.dataType = dataType;
	}

	// distributionType = 1, 4
	public DistributionTypeInfo(int distributionType, long columnMinValue, long columnMaxValue, long columnCardinality,
			double coefficient) {
		super();
		this.distributionType = distributionType;
		this.columnMinValue = columnMinValue;
		this.columnMaxValue = columnMaxValue;
		this.columnCardinality = columnCardinality;
		this.coefficient = coefficient;
	}

	// distributionType = 2, 5
	public DistributionTypeInfo(int distributionType, long columnCardinality, int minLength, int maxLength,
			String[] seedStrings) {
		super();
		this.distributionType = distributionType;
		this.columnCardinality = columnCardinality;
		this.minLength = minLength;
		this.maxLength = maxLength;
		this.seedStrings = seedStrings;
	}

	@Override
	public String toString() {
		return "DistributionTypeInfo [distributionType=" + distributionType + ", dataType=" + dataType
				+ ", columnMinValue=" + columnMinValue + ", columnMaxValue=" + columnMaxValue + ", columnCardinality="
				+ columnCardinality + ", coefficient=" + coefficient + ", minLength=" + minLength + ", maxLength="
				+ maxLength + ", size of seedStrings=" + (seedStrings == null ? 0 : seedStrings.length) + "]";
	}
}
