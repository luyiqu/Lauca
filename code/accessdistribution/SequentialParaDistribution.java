package accessdistribution;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * 基于连续时间窗口的数据访问分布，体现了相邻时间窗口数据分布之间的连续性
 * 对于非等值过滤型参数，这种连续性是没有物理意义的，因此连续时间窗口数据访问分布一般仅考虑整型和字符型参数
 */
public abstract class SequentialParaDistribution extends DataAccessDistribution {

	// 当前时间窗口中各分区的输入参数与前段时间窗口输入参数重复的比例（对cache，buffer性能影响显著），按时间倒序保存
	// 在负载的生成过程中控制这个比例可能使得负载生成器成为性能测试瓶颈，因此可以离线生成好候选输入参数集
	protected double[][] intervalParaRepeatRatios = null;

	public SequentialParaDistribution(double[] hFItemFrequencies, long[] intervalCardinalities,
			double[] intervalFrequencies, double[][] intervalParaRepeatRatios) {
		super(hFItemFrequencies, intervalCardinalities, intervalFrequencies);
		if (intervalParaRepeatRatios == null){
			return;
		}
		this.intervalParaRepeatRatios = new double[intervalParaRepeatRatios.length][];
		if (intervalParaRepeatRatios[0] == null){
			return;
		}

		for (int i = 0; i < intervalParaRepeatRatios.length; i++) {
			try{
				this.intervalParaRepeatRatios[i] = new double[intervalParaRepeatRatios[i].length];
				System.arraycopy(intervalParaRepeatRatios[i], 0, this.intervalParaRepeatRatios[i], 0, intervalParaRepeatRatios[i].length);
			}
			catch (Exception e){
				e.printStackTrace();
			}
		}
	}

	public SequentialParaDistribution(double[] hFItemFrequencies, long[] intervalCardinalities,
									  double[] intervalFrequencies, double[][] intervalParaRepeatRatios, ArrayList<ArrayList<Double>> quantilePerInterval) {
		super(hFItemFrequencies, intervalCardinalities, intervalFrequencies, quantilePerInterval);
		if (intervalParaRepeatRatios == null){
			return;
		}
		this.intervalParaRepeatRatios = new double[intervalParaRepeatRatios.length][];
		if (intervalParaRepeatRatios[0] == null){
			return;
		}

		for (int i = 0; i < intervalParaRepeatRatios.length; i++) {
			try{
				this.intervalParaRepeatRatios[i] = new double[intervalParaRepeatRatios[i].length];
//				System.out.println(Arrays.toString(intervalParaRepeatRatios[i]));
				System.arraycopy(intervalParaRepeatRatios[i], 0, this.intervalParaRepeatRatios[i], 0, intervalParaRepeatRatios[i].length);
			}
			catch (Exception e){
				e.printStackTrace();
			}
		}
//		System.out.println();
	}

	public SequentialParaDistribution(SequentialParaDistribution sequentialParaDistribution){
		super(sequentialParaDistribution);
		this.intervalParaRepeatRatios = new double[sequentialParaDistribution.intervalParaRepeatRatios.length][];
		int cnt = 0;
		for (double[] repeatRadio :	sequentialParaDistribution.intervalParaRepeatRatios ) {
			this.intervalParaRepeatRatios[cnt] = new double[repeatRadio.length];
			System.arraycopy(repeatRadio, 0, this.intervalParaRepeatRatios[cnt], 0, repeatRadio.length);
			cnt ++;
		}

	}
}
