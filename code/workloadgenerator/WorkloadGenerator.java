package workloadgenerator;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import accessdistribution.DataAccessDistribution;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;

import config.Configurations;
import util.DBConnector;

/**
 * 负载生成器，可部署在多个节点上，每个节点上独立启动即可
 */
public class WorkloadGenerator {

	// 所有节点上的测试客户端数量（一个测试客户端就是一个测试线程、每个测试线程独享一个数据库连接）
	private int allThreadNum;
	// 当前节点上的测试客户端数量
	private int localThreadNum;

	private Workload workload = null;
	private DBConnector dbConnector = null;




	// 按时间窗口顺序依次存放的负载吞吐量信息
	private List<WindowThroughput> windowThroughputList = null;

	// 按时间窗口顺序依次存放的SQL参数 数据分布信息，数据结构描述：列表（事务名称 -> 参数标识符 -> 参数数据分布）
	private List<Map<String, Map<String, DataAccessDistribution>>> windowDistributionList = null;

	// 所有SQL参数的全生命周期 数据访问分布（利用采样的数据统计得到的）
	private Map<String, Map<String, DataAccessDistribution>> txName2ParaId2FullLifeCycleDistribution = null;

	private Logger logger = Logger.getLogger(WorkloadGenerator.class);

	public WorkloadGenerator(Workload workload) {
		this.workload = workload;
	}

	public WorkloadGenerator(int allThreadNum, int localThreadNum, Workload workload, DBConnector dbConnector) {
		super();
		this.allThreadNum = allThreadNum;
		this.localThreadNum = localThreadNum;
		this.workload = workload;
		this.dbConnector = dbConnector;
	}

	// Input para 'txName2ThroughputList'：事务名 -> 按时间窗口顺序依次存放的事务吞吐量
	public void constructWindowThroughputList(Map<String, List<Integer>> txName2ThroughputList) {
		windowThroughputList = new ArrayList<>();

		Iterator<Entry<String, List<Integer>>> iter = txName2ThroughputList.entrySet().iterator();
		List<Entry<String, List<Integer>>> entryList = new ArrayList<>();
		while (iter.hasNext()) {
			entryList.add(iter.next());
		}


		// maxWindowNum：最长的某个事务的时间窗口数量
		int maxWindowNum = Integer.MIN_VALUE;
		for (int i = 0; i < entryList.size(); i++) {
			if (entryList.get(i).getValue().size() > maxWindowNum) {
				maxWindowNum = entryList.get(i).getValue().size();
			}
		}

		for (int i = 0; i < maxWindowNum; i++) {

//			System.out.println("Time: "+maxWindowNum);
			int throughput = 0;
			for (int j = 0; j < entryList.size(); j++) {
				if (entryList.get(j).getValue().size() > i) {
					throughput += entryList.get(j).getValue().get(i);
				}
			}
			Map<String, Double> txName2Ratio = new HashMap<>();
			for (int j = 0; j < entryList.size(); j++) {
				Entry<String, List<Integer>> entry = entryList.get(j);
				if (entry.getValue().size() > i) {
					if (throughput == 0) {
						txName2Ratio.put(entry.getKey(), 0d); // bug fix，throughput可能为0
					} else {
						txName2Ratio.put(entry.getKey(), entry.getValue().get(i) / (double) throughput);
					}
				} else {
					txName2Ratio.put(entry.getKey(), 0d);
				}
			}
			int throughputPerSec = Math.round(throughput / (float) Configurations.getTimeWindowSize());
			windowThroughputList.add(new WindowThroughput(throughputPerSec, txName2Ratio)); //每秒的吞吐&该秒中事务所占的比例
//			System.out.println("throughputPerSec: "+throughputPerSec);
//			System.out.println("txName2Ratio: "+txName2Ratio);
		}

		logger.info("\n\tWorkloadGenerator.constructWindowThroughputList -> the size of windowThroughputList: "
				+ windowThroughputList.size());
	}

	public void setWindowDistributionList(
			List<Map<String, Map<String, DataAccessDistribution>>> windowDistributionList) {
		this.windowDistributionList = windowDistributionList;
//		System.out.println("I am in setWindowDistributionList");
		logger.info("\n\tWorkloadGenerator.setWindowDistributionList -> the size of windowDistributionList: "
				+ windowDistributionList.size());
		logger.info(windowDistributionList);
//		System.out.println(windowDistributionList);
//		System.out.println("setWindowDistributionList is finished");
	}

	public void setTxName2ParaId2FullLifeCycleDistribution(
			Map<String, Map<String, DataAccessDistribution>> txName2ParaId2FullLifeCycleDistribution) {
		this.txName2ParaId2FullLifeCycleDistribution = txName2ParaId2FullLifeCycleDistribution;
	}

	// 启动当前节点上的所有测试客户端
	public void startAllThreads(CountDownLatch countDownLatch) {
		Thread[] threads = new Thread[localThreadNum];

		// TODO 在这里更新txName2ParaId2FullLifeCycleDistribution
		Map<String, Map<String, DataAccessDistribution>> fullLifeCycleDistribution = new HashMap<>();


		for (int i = 0; i < localThreadNum; i++) {
			// 这里的Workload一定要深拷贝，以避免多测试线程共享Workload对象而发生错误
			Workload workloadCopy = new Workload(workload);

			Connection conn = null;
			String databaseType = Configurations.getDatabaseType().toLowerCase();
			if (databaseType.equals("mysql")||databaseType.equals("tidb")) {
				conn = dbConnector.getMySQLConnection();
			} else if (databaseType.equals("postgresql")) {
				conn = dbConnector.getPostgreSQLConnection();
			} else if (databaseType.equals("oracle")) {
				conn = dbConnector.getOracleConnection();
			}

			try {
				conn.setAutoCommit(false);
				int txIsolation = Configurations.getTransactionIsolation();
				if (txIsolation == 0) {
					conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
				} else if (txIsolation == 1) {
					conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
				} else if (txIsolation == 2) {
					conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
				} else if (txIsolation == 3) {
					conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
			workloadCopy.initTransactions(conn);

			// 数据访问分布信息类必须可以由多线程共享（可以被共享的前提是生成随机数据时不可更新对象属性值）
			// 因此我们在生成随机数据时弃用了'intervalInnerIndexes'属性（其实也可以为每个负载生成线程深拷贝一个数据分布信息对象，但是比较麻烦，就放弃了）
			workloadCopy.setFullLifeCycleParameterDistribution(txName2ParaId2FullLifeCycleDistribution);

			threads[i] = new Thread(new WorkloadGeneratorThread(workloadCopy, allThreadNum, windowThroughputList,
					windowDistributionList,countDownLatch));
			threads[i].start();
		}
		logger.info("所有负载生成线程启动成功！ localThreadNum = " + localThreadNum);
//		for(int i = 0;i < localThreadNum;i++){
//			try {
//				threads[i].join();
//			}catch (InterruptedException e){
//
//			}
//		}
	}

	public void setAllThreadNum(int allThreadNum) {
		this.allThreadNum = allThreadNum;
	}

	public void setLocalThreadNum(int localThreadNum) {
		this.localThreadNum = localThreadNum;
	}

	public void setDbConnector(DBConnector dbConnector) {
		this.dbConnector = dbConnector;
	}
	
	
}

// 具体的负载生成线程，这里假设所有传入的引用对象是线程安全的
class WorkloadGeneratorThread implements Runnable {

	private Workload workload = null; // 必须深拷贝
	private int allThreadNum;
	// 下面两个引用可被多线程共享
	private List<WindowThroughput> windowThroughputList = null;
	private List<Map<String, Map<String, DataAccessDistribution>>> windowDistributionList = null;

	//cdl作用为了等monitor线程和起数据库链接的线程都等建好数据库链接之后，开启事务的测试，因为建立链接所耗的时间很长
	private CountDownLatch cdl = null;

	public WorkloadGeneratorThread(Workload workload, int allThreadNum, List<WindowThroughput> windowThroughputList,
			List<Map<String, Map<String, DataAccessDistribution>>> windowDistributionList,CountDownLatch countDownLatch) {
		super();
		this.workload = workload;
		this.allThreadNum = allThreadNum;
		this.windowThroughputList = windowThroughputList;
		this.windowDistributionList = windowDistributionList;
		this.cdl = countDownLatch;
	}

	@Override
	public void run() {

		cdl.countDown();
		try {
			cdl.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		// timeWindowMillis：一个时间窗口的毫秒数
		int timeWindowMillis = Configurations.getTimeWindowSize() * 1000;
		// 负载加载类型：0，以最大吞吐压；1：按指定吞吐压
		int loadingType = Configurations.getLoadingType();

//		long testTimeLength = Configurations.getTestTimeLength();
		long threadStartTime = System.currentTimeMillis();
//		long threadEndTime = threadStartTime + testTimeLength * 1000;
		if (loadingType == 0) {
			//
			long threadEndTime = threadStartTime + windowThroughputList.size() * timeWindowMillis;
			//---modified by lyqu
			int windowIndex = 0;
			workload.adjustTransactionRatio(windowThroughputList.get(windowIndex).txName2Ratio);
			workload.adjustParameterDistribution(windowDistributionList.get(windowIndex));

			while (true) {
				long currentTime = System.currentTimeMillis();
				if (currentTime >= threadEndTime) {
					break;
				}
				int tmpIndex = (int) (currentTime - threadStartTime) / timeWindowMillis;
				if (tmpIndex > windowIndex) {
					windowIndex = tmpIndex;
//					System.out.println("testing1");
					workload.adjustTransactionRatio(windowThroughputList.get(windowIndex).txName2Ratio);
					workload.adjustParameterDistribution(windowDistributionList.get(windowIndex));
//					System.out.println("testing2");
				}
				float responceTime = 0;
				responceTime = workload.execute();

//				System.out.println(responceTime);//qly输出: 先看一下responceTime
				Monitor.addResponceTime(responceTime);
			}
		} else if (loadingType == 1) {
			// windowTimer是用来不断切换时间窗口的（事务吞吐 -- 总吞吐量和各事务比例；参数数据分布）
			Timer windowTimer = new Timer();
//			System.out.println("WG1");
			windowTimer.scheduleAtFixedRate(new TimerTask() {
				@Override
				public void run() {
					int windowIndex = (int) ((System.currentTimeMillis() - threadStartTime) / timeWindowMillis);
					if (windowIndex >= windowThroughputList.size() || windowIndex >= windowDistributionList.size()) {
						windowTimer.cancel();
					}
//					System.out.println("WG2");
					workload.adjustParameterDistribution(windowDistributionList.get(windowIndex));  //这里出现问题
//					System.out.println("WG3");
					WindowThroughput windowThroughput = windowThroughputList.get(windowIndex);
//					System.out.println("WG4");
					workload.adjustTransactionRatio(windowThroughput.txName2Ratio);
//					System.out.println("WG5");
					// 单个测试线程每秒的事务请求量
					double singleThreadThroughput = 0;
					if (Configurations.getFixedThroughput() != -1) {
						singleThreadThroughput = (double) Configurations.getFixedThroughput() / allThreadNum;
					} else {
						// 事务吞吐扩展因子
//						System.out.println("WG6");
						double throughputScaleFactor = Configurations.getThroughputScaleFactor();
						singleThreadThroughput = windowThroughput.throughput * throughputScaleFactor / allThreadNum;
					}

					// TODO （简单地调整了一下，对于超低吞吐的负载可能有点误差）
					if (singleThreadThroughput > 0 && singleThreadThroughput < 1) {
						singleThreadThroughput = 1;
					}
					if (singleThreadThroughput == 0) {
						return; // 当前时间窗口不加载负载
					}

					// 一秒钟（1000ms）平摊给每个事务请求的时间段
					long period = Math.round(1000 / singleThreadThroughput);

					long currentWindowStartTime = System.currentTimeMillis();
//					System.out.println("WG7");
					Timer transactionRequestTimer = new Timer();
					transactionRequestTimer.scheduleAtFixedRate(new TimerTask() {
						@Override
						public void run() {
							if (System.currentTimeMillis() - currentWindowStartTime >= timeWindowMillis) {
								transactionRequestTimer.cancel();
							}
							float responceTime = 0;
							responceTime = workload.execute();

							Monitor.addResponceTime(responceTime);
						}
					}, 0, period); // 不断发起事务请求

				}
			}, 0, timeWindowMillis); // 不断切换时间窗口

		} // else if (loadingType == 1)
	}
}

// 一个时间窗口的总事务吞吐量和各个事务的比例
// 当前我们只考虑了事务吞吐、事务比例的动态变化，未考虑事务中分支、循环等事务逻辑的变化（事务逻辑我们默认是不变的）TODO
class WindowThroughput {

	int throughput;
	Map<String, Double> txName2Ratio = null;

	public WindowThroughput(int throughput, Map<String, Double> txName2Ratio) {
		super();
		this.throughput = throughput;
		this.txName2Ratio = txName2Ratio;
	}

	@Override
	public String toString() {
		return "WindowThroughput [throughput=" + throughput + ", txName2Ratio=" + txName2Ratio + "]";
	}
}
