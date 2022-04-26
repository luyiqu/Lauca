package accessdistribution;

/**
 * 基于连续时间窗口的数据访问分布，体现了相邻时间窗口数据分布之间的连续性
 * 对于非等值过滤型参数，这种连续性是没有物理意义的，因此连续时间窗口数据访问分布一般仅考虑整型和字符型参数
 */
public abstract class SequentialParaDistribution extends DataAccessDistribution {

	// 当前时间窗口中各分区的输入参数与前一个时间窗口输入参数重复的比例（对cache，buffer性能影响显著）
	// 在负载的生成过程中控制这个比例可能使得负载生成器成为性能测试瓶颈，因此可以离线生成好候选输入参数集
	protected double[] intervalParaRepeatRatios = null;

	public SequentialParaDistribution(double[] hFItemFrequencies, long[] intervalCardinalities,
			double[] intervalFrequencies, double[] intervalParaRepeatRatios) {
		super(hFItemFrequencies, intervalCardinalities, intervalFrequencies);
		this.intervalParaRepeatRatios = intervalParaRepeatRatios;
	}
}
