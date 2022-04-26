package workloadgenerator;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import abstraction.Transaction;
import accessdistribution.DataAccessDistribution;

// 测试负载的抽象类
public class Workload {

	private List<Transaction> transactions = null;
	private double[] cumulativeProbabilities = null;

	private Logger logger = Logger.getLogger(Workload.class);
	
	public Workload(List<Transaction> transactions) {
		super();
		this.transactions = transactions;
	}

	// 深拷贝Workload对象，以支持多线程的负载生成
	public Workload(Workload workload) {
		this.transactions = new ArrayList<>();
		for (Transaction transaction : workload.transactions) {
			this.transactions.add(new Transaction(transaction));
		}
		logger.info("负载对象深拷贝成功！");
	}

	public void initTransactions(Connection conn) {
//		System.out.println("initTransaction***********");
		for (int i = 0; i < transactions.size(); i++) {
			transactions.get(i).init(conn);
		}
		logger.info("对负载中所有事务对象初始化成功！");
	}

	// 设置当前时间窗口的事务比例，不同时间窗口各事务所占的比例可能是动态变化的
	public void adjustTransactionRatio(Map<String, Double> txName2Ratio) {
		
		// bug fix，某个时间窗口可能出现吞吐为0的情况
		Iterator<Entry<String, Double>> iter = txName2Ratio.entrySet().iterator();
		double sum = 0;
		while (iter.hasNext()) {
			sum += iter.next().getValue();
		}
		if (sum == 0) { // 暂用上一个时间窗口的事务比例
			return;
		}
		// ------ bug fix
		
		for (int i = 0; i < transactions.size(); i++) {
			String txName = transactions.get(i).getName();
			if (txName2Ratio.containsKey(txName)) {
				transactions.get(i).setRatio(txName2Ratio.get(txName));
			} else {
				transactions.get(i).setRatio(0);
			}
		}
		initCumulativeProbabilities();
	}
	
	// 将当前时间窗口的参数数据分布设置到各个事务的SQL操作中，负载生成时会依据事务逻辑和参数数据分布生成具体SQL参数
	public void adjustParameterDistribution(
			Map<String, Map<String, DataAccessDistribution>> txName2ParaId2Distribution) {
		for (int i = 0; i < transactions.size(); i++) {
			String txName = transactions.get(i).getName();
			Map<String, DataAccessDistribution> paraId2Distribution = txName2ParaId2Distribution.get(txName);
			transactions.get(i).setSqlParaDistribution(paraId2Distribution, 1);
		}
	}
	
	// 设置全负载周期 SQL参数的 数据分布
	public void setFullLifeCycleParameterDistribution(
			Map<String, Map<String, DataAccessDistribution>> txName2ParaId2FullLifeCycleDistribution) {
		for (int i = 0; i < transactions.size(); i++) {
			String txName = transactions.get(i).getName();
			Map<String, DataAccessDistribution> paraId2Distribution = 
					txName2ParaId2FullLifeCycleDistribution.get(txName);
			transactions.get(i).setSqlParaDistribution(paraId2Distribution, 0);
		}
	}

	// 避免使用定时器时并行地发起负载请求~
	public synchronized float execute() {
		double randomValue = Math.random();
		if (randomValue > 0.99999999) {
			randomValue = randomValue - 0.000000001;
		}
		
		float responceTime = -Float.MAX_VALUE;
		for (int i = 0; i < cumulativeProbabilities.length; i++) {
			if (randomValue < cumulativeProbabilities[i]) {
				responceTime = transactions.get(i).exectue();
				break;
			}
		}
		
		return responceTime;
	}

	private void initCumulativeProbabilities() {
		cumulativeProbabilities = new double[transactions.size()];
		cumulativeProbabilities[0] = transactions.get(0).getRatio();
		for (int i = 1; i < transactions.size(); i++) {
			cumulativeProbabilities[i] = cumulativeProbabilities[i - 1] + transactions.get(i).getRatio();
		}
		
		if (cumulativeProbabilities[cumulativeProbabilities.length - 1] <= 0.99999999) {
			logger.error("所有事务的概率之和小于1！ " + Arrays.toString(cumulativeProbabilities));
		}
	}
}
