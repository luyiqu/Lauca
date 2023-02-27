package workloadgenerator;

import java.util.*;
import java.util.concurrent.CountDownLatch;

import config.Configurations;

/**
 * 实时统计当前的吞吐、时延（平均时延，80%时延，90%时延，95%时延和99时延）以及出错回滚事务的吞吐
 */
public class Monitor implements Runnable {

	// 统计窗口时间大小，单位为s，默认为1s
	private int statWindowSize = 1;

	// 多个Vector<Float>是为了降低冲突
	private static volatile List<Vector<Float>> responceTimeInfo = null;
	// Vector<Float>的数量
	private static int vectorNum = Configurations.getLocalTestThreadNum() * 5;

	//cdl作用为了等monitor线程和起数据库链接的线程都等建好数据库链接之后，开启事务的测试，因为建立链接所耗的时间很长
	private CountDownLatch cdl = null;
	public Monitor() {
		super();
		responceTimeInfo = newResponceTimeInfo();
	}

	public Monitor(int statWindowSize, CountDownLatch countDownLatch) {
		super();
		this.statWindowSize = statWindowSize;
		responceTimeInfo = newResponceTimeInfo();
		this.cdl = countDownLatch;
	}
	
	
	// 支持累积平均失败事务吞吐的计算 -- 实验需求
	private static int timesCount = 0;
	private static float cumuFailureThroughput = 0;
	
	// 支持累积平均死锁事务吞吐的计算 -- 实验需求
	private static float cumuDeadlockThroughput = 0;
	
	
	@Override
	public void run() {
		cdl.countDown();
		try {
			cdl.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("successThroughput, avgResponceTime, 80%ResponceTime, 90%ResponceTime, " 
				+ "95%ResponceTime, 99%ResponceTime, failureThroughput, cumuFailureThroughput, "
				+ "deadlockThroughput, cumuDeadlockThroughput");

		Timer windowTimer = new Timer();
		windowTimer.scheduleAtFixedRate(new TimerTask() {
			@Override
			public void run() {

				List<Vector<Float>> tmp = responceTimeInfo;
				responceTimeInfo = newResponceTimeInfo();

				List<Float> allResponceTimes = new ArrayList<>();
				for (Vector<Float> floats : tmp) {
					allResponceTimes.addAll(floats);
				}

				// failureTxNum: 非死锁导致的执行失败事务数；deadlockTxNum：死锁导致的执行失败事务数
				int successTxNum = 0, failureTxNum = 0, deadlockTxNum = 0, errorNum = 0;
				float successTxResponceTimeSum = 0;

				for (Float allResponceTime : allResponceTimes) {
					if (allResponceTime > 0) { //感觉bug在这边，失败吞吐的响应时间真的为0吗？
						successTxNum++;
						successTxResponceTimeSum += allResponceTime;
					} else if (allResponceTime == 0) {
						failureTxNum++;
					} else if (allResponceTime == -1) {
						deadlockTxNum++;
					} else {
						errorNum++;
					}
				}

				// 其实errorNum必然为0
				if (errorNum != 0) {
					System.err.println("errorNum不为0， ERROR!!!");
				}

				float failureThroughput = (float)failureTxNum / statWindowSize;
				float deadlockThroughput = (float)deadlockTxNum / statWindowSize;
				float successThroughput = (float)successTxNum / statWindowSize;
				float avgResponceTime = successTxResponceTimeSum / successTxNum;

				// 支持累积平均失败事务吞吐的计算 -- 实验需求
				timesCount++;
				cumuFailureThroughput += failureThroughput;
				cumuDeadlockThroughput += deadlockThroughput;
				
				if (allResponceTimes.size() == failureTxNum + deadlockTxNum + errorNum) {
					System.out.println("No successful transaction!!!, " + failureThroughput);
				} else {
					Collections.sort(allResponceTimes);
					float _80ResponceTime = allResponceTimes.get((int)(0.8 * successTxNum) + failureTxNum + deadlockTxNum + errorNum);
					float _90ResponceTime = allResponceTimes.get((int)(0.9 * successTxNum) + failureTxNum + deadlockTxNum + errorNum);
					float _95ResponceTime = allResponceTimes.get((int)(0.95 * successTxNum) + failureTxNum + deadlockTxNum + errorNum);
					float _99ResponceTime = allResponceTimes.get((int)(0.99 * successTxNum) + failureTxNum + deadlockTxNum + errorNum);

					System.out.println(successThroughput + ", " + avgResponceTime + ", " + _80ResponceTime + ", " + _90ResponceTime 
							+ ", " + _95ResponceTime + ", " + _99ResponceTime + ", " + failureThroughput + ", " 
							+ (cumuFailureThroughput / timesCount) + ", " + deadlockThroughput + ", " + (cumuDeadlockThroughput / timesCount));
				}
			}
		}, 1000, statWindowSize * 1000L);
	}

	private List<Vector<Float>> newResponceTimeInfo() {
		List<Vector<Float>> responceTimeInfoTmp = new ArrayList<>();
		for (int i = 0; i < vectorNum; i++) {
			responceTimeInfoTmp.add(new Vector<>());
		}
		return responceTimeInfoTmp;
	}

	public static void addResponceTime(float responceTime) {
		int randomIndex = (int)(Math.random() * vectorNum);
		responceTimeInfo.get(randomIndex).add(responceTime);
	}
}
