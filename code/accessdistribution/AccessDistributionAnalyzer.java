package accessdistribution;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

import abstraction.*;
import org.apache.log4j.PropertyConfigurator;

import config.Configurations;
import input.TableInfoSerializer;
import input.TraceInfo;
import input.WorkloadReader;
import input.StoredProcedureReader;
import transactionlogic.OperationData;
import transactionlogic.Preprocessor;
import transactionlogic.RunningLogReader;
import transactionlogic.TransactionData;
import transactionlogic.TxLogicAnalyzer;
import util.DBConnector;
import workloadgenerator.Monitor;
import workloadgenerator.Workload;
import workloadgenerator.WorkloadGenerator;

/**
 * 数据访问分布的分析器：通过读取应用端运行日志（也可以是数据库系统的workload trace）分析获取所有参数的数据分布
 */
public class AccessDistributionAnalyzer {

	public static void main(String[] args) {

		// 实验数据需要 - 20190615
		long startTime1 = System.currentTimeMillis();

		PropertyConfigurator.configure(".//lib//log4j.properties");
		TableInfoSerializer serializer = new TableInfoSerializer();
		List<Table> tables = serializer.read(new File(".//testdata//tables2.obj"));
		WorkloadReader workloadReader = new WorkloadReader(tables);
		List<Transaction> transactions = workloadReader.read(new File(".//testdata//tpcc-transactions.txt"));

		Preprocessor preprocessor = new Preprocessor();
		preprocessor.constructOperationTemplateAndDistInfo(transactions);
		Map<String, Map<Integer, OperationData>> txName2OperationId2Template = preprocessor
				.getTxName2OperationId2Template();
		Map<String, Map<Integer, DistributionTypeInfo[]>> txName2OperationId2paraDistTypeInfos = preprocessor
				.getTxName2OperationId2paraDistTypeInfos();
		Map<String, Map<String, DistributionTypeInfo>> txName2ParaId2DistTypeInfo = getParaDistributionTypeInfo(
				txName2OperationId2paraDistTypeInfos);

		RunningLogReader runningLogReader = new RunningLogReader(txName2OperationId2Template);
		runningLogReader.read(new File(".//testdata//log4txlogic"));
		Iterator<Entry<String, List<TransactionData>>> txDataIter = runningLogReader.getTxName2TxDataList().entrySet()
				.iterator();

		int timeWindowSize = Configurations.getTimeWindowSize();
		int statThreadNum = Configurations.getStatThreadNum();
		Map<String, List<List<String>>> txName2StatParameters = new HashMap<>();
		Map<String, Map<String, Double>> txName2ParaId2AvgRunTimes = new HashMap<>();
		Map<String, BlockingQueue<String>> LogSplitterQueueMap = new HashMap<>();
		List<BlockingQueue<WindowData>> windowDataBlockingQueues = new ArrayList<>();
		for (int i = 0; i < statThreadNum; i++) {
			BlockingQueue<WindowData> windowDataBlockingQueue = new ArrayBlockingQueue<>(20);
			windowDataBlockingQueues.add(windowDataBlockingQueue);
		}
		CountDownLatch cdl = new CountDownLatch(transactions.size());
		while (txDataIter.hasNext()) {
			Entry<String, List<TransactionData>> entry = txDataIter.next();

//			System.out.println(entry.getKey());

			TxLogicAnalyzer txLogicAnalyzer = new TxLogicAnalyzer();
			txLogicAnalyzer.obtainTxLogic(entry.getValue());

			Map<Integer, OperationData> operationDataTemplates = txName2OperationId2Template.get(entry.getKey());
			Map<Integer, OperationData> sortedOperationDataTemplates = new TreeMap<>();
			sortedOperationDataTemplates.putAll(operationDataTemplates);
			Iterator<Entry<Integer, OperationData>> operationDataTemplateIter = sortedOperationDataTemplates.entrySet()
					.iterator();

			List<List<String>> statParameters = txLogicAnalyzer.getStatParameters(operationDataTemplateIter);
			txName2StatParameters.put(entry.getKey(), statParameters);

			txName2ParaId2AvgRunTimes.put(entry.getKey(), new HashMap<>());
			txName2ParaId2AvgRunTimes.get(entry.getKey()).put("1_0",
					txLogicAnalyzer.getOperationId2AvgRunTimes().get(1));

			BlockingQueue<String> logBlockingQueue = new ArrayBlockingQueue<>(10000);
			LogSplitterQueueMap.put(entry.getKey(), logBlockingQueue);
			new Thread(new LogSplitter(entry.getKey(), logBlockingQueue, timeWindowSize, windowDataBlockingQueues,
					txName2StatParameters, cdl)).start();

			// for testing workload generator! -- 为每个事务设置事务逻辑信息
			for (int i = 0; i < transactions.size(); i++) {
				if (transactions.get(i).getName().equals(entry.getKey())) {
					transactions.get(i).setTransactionLogicInfo(txLogicAnalyzer.getParameterNodeMap(),
							txLogicAnalyzer.getMultipleLogicMap(), txLogicAnalyzer.getOperationId2AvgRunTimes());
					break;
				}
			}
		}

		// 实验数据需要 - 20190615
		long startTime2 = System.currentTimeMillis();

		DistributionCounter.init(txName2StatParameters);
		DistributionCounter.setTxName2ParaId2AvgRunTimes(txName2ParaId2AvgRunTimes);

		for (int i = 0; i < statThreadNum; i++) {
			new Thread(new DistributionStatisticalThread(windowDataBlockingQueues.get(i), txName2ParaId2DistTypeInfo))
					.start();
		}

		File[] logFiles = new File(".//testdata//log4dist").listFiles();
		// 对日志文件进行排序 - 序号大的在前，最后一个lauca.log文件名修改为lauca.log.0
		// 文件名的格式为: 'lauca.log.xx'
		Arrays.sort(logFiles, new Comparator<File>() {
			@Override
			public int compare(File file1, File file2) {
//				int sequenceNumber1 = Integer.parseInt(file1.getName().split("\\.")[2]);
//				int sequenceNumber2 = Integer.parseInt(file2.getName().split("\\.")[2]);
				String[] fileName1 = file1.getName().split("\\.");
				String[] fileName2 = file2.getName().split("\\.");
				int sequenceNumber1 = (fileName1.length > 2) ? Integer.parseInt(fileName1[2]) : 0;
				int sequenceNumber2 = (fileName2.length > 2) ? Integer.parseInt(fileName2[2]) : 0;
				if (sequenceNumber1 < sequenceNumber2) {
					return 1;
				} else if (sequenceNumber1 > sequenceNumber2) {
					return -1;
				} else {
					return 0;
				}
			}
		});
		new Thread(new LogReader(logFiles, LogSplitterQueueMap)).start();

		try {
			cdl.await();
			// 关闭所有分布统计线程（DistributionStatisticalThread）
			for (int i = 0; i < windowDataBlockingQueues.size(); i++) {
				windowDataBlockingQueues.get(i).put(new WindowData("txName", "paraIdentifier", -1, null));
			}

			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		// 基于采样的数据，统计全局数据访问分布
		DistributionCounter.countFullLifeCycleDistribution();

		// 实验数据需要 - 20190615
		System.out.println(System.currentTimeMillis() - startTime1);
		System.out.println(System.currentTimeMillis() - startTime2);
		System.exit(0);

		// for testing workload generator!
		System.out.println("***********************************************");
		int allThreadNum = 20;
		int localThreadNum = 20;
		Workload workload = new Workload(transactions);
//		String ip = "10.11.1.193";
//		String port = "13306";
//		String dbName = "lauca";
		String ip = "10.11.6.125";
		String port = "13306";
		String dbName = "lauca_tpcc_sf10_0122";
		String userName = "root";
		String passwd = "root";
		DBConnector dbConnector = new DBConnector(ip, port, dbName, userName, passwd);

		WorkloadGenerator workloadGenerator = new WorkloadGenerator(allThreadNum, localThreadNum, workload,
				dbConnector);
		workloadGenerator.constructWindowThroughputList(DistributionCounter.getTxName2ThroughputList());
		workloadGenerator.setWindowDistributionList(DistributionCounter.getWindowDistributionList());
		workloadGenerator
				.setTxName2ParaId2FullLifeCycleDistribution(DistributionCounter.getTxName2ParaId2GlobalDistribution());

		CountDownLatch countDownLatch = new CountDownLatch(allThreadNum+1);
		new Thread(new Monitor(2,countDownLatch)).start();

		workloadGenerator.startAllThreads(countDownLatch);

		try {
			Thread.sleep(100000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("Over!");
		System.exit(0);
	}

	// 简单封装以便调用，返回项服务于后续的负载生成，返回项中已经包含了事务逻辑的信息
	public static List<Transaction> startRun(List<Table> tables) {
//		TableInfoSerializer serializer = new TableInfoSerializer();
//		List<Table> tables = serializer.read(new File(Configurations.getDataCharacteristicSaveFile()));
		WorkloadReader workloadReader;
		if(Configurations.isUseStoredProcedure()){
			StoredProcedureReader storedProcedureReader = new StoredProcedureReader();
			List<StoredProcedure> storedProcedures = storedProcedureReader.read(new File(Configurations.getStoredProcedureSaveFile()));
			workloadReader = new WorkloadReader(tables,storedProcedures);
		}
		else{
			workloadReader = new WorkloadReader(tables);
		}
		List<Transaction> transactions;
		if (Configurations.isUseSkywalking()) {
			transactions = workloadReader.readLaucaLog(new File(Configurations. getLaucaLogDir()));
//		} else if (Configurations.isUseTidbLog()) {
//			transactions = workloadReader.readTidbLog(new File(Configurations.getTidbLogFile()));
		} else {
			transactions = workloadReader.read(new File(Configurations.getTransactionTemplatesFile()));
		}

		Preprocessor preprocessor = new Preprocessor();
		preprocessor.constructOperationTemplateAndDistInfo(transactions);
		Map<String, Map<Integer, OperationData>> txName2OperationId2Template = preprocessor
				.getTxName2OperationId2Template();
		Map<String, Map<Integer, DistributionTypeInfo[]>> txName2OperationId2paraDistTypeInfos = preprocessor
				.getTxName2OperationId2paraDistTypeInfos();
		Map<String, Map<String, DistributionTypeInfo>> txName2ParaId2DistTypeInfo = getParaDistributionTypeInfo(
				txName2OperationId2paraDistTypeInfos);

		RunningLogReader runningLogReader = new RunningLogReader(txName2OperationId2Template);
		if (Configurations.isUseSkywalking()) {
			runningLogReader.readTrace(workloadReader.txnId2txnTrace, workloadReader.txnId2txnTemplateID);
		} else {
			runningLogReader.read(new File(Configurations.getRunningLogDir4TxLogic()));
		}
//		System.out.println("Size1: "+runningLogReader.getTxName2TxDataList().get("Transaction1").size());
//		System.out.println("Size2: "+runningLogReader.getTxName2TxDataList().get("Transaction2").size());
		Iterator<Entry<String, List<TransactionData>>> txDataIter = runningLogReader.getTxName2TxDataList().entrySet()
				.iterator();

		int timeWindowSize = Configurations.getTimeWindowSize();
		int statThreadNum = Configurations.getStatThreadNum();

		Map<String, List<List<String>>> txName2StatParameters = new HashMap<>();
		Map<String, Map<String, Double>> txName2ParaId2AvgRunTimes = new HashMap<>();
		Map<String, BlockingQueue<String>> LogSplitterQueueMap = new HashMap<>();
		List<BlockingQueue<WindowData>> windowDataBlockingQueues = new ArrayList<>();
		for (int i = 0; i < statThreadNum; i++) {
			BlockingQueue<WindowData> windowDataBlockingQueue = new ArrayBlockingQueue<>(20);
			windowDataBlockingQueues.add(windowDataBlockingQueue);
		}
		// cdl的作用：待所有LogSplitter线程结束后显示关闭DistributionStatisticalThread
		// CountDownLatch cdl = new CountDownLatch(transactions.size());
		// bug fix: 可能有些事务模板没有实例数据
		CountDownLatch cdl = new CountDownLatch(runningLogReader.getTxName2TxDataList().size());

		// 针对每个事务模板的处理
		while (txDataIter.hasNext()) {
			Entry<String, List<TransactionData>> entry = txDataIter.next();
//			System.out.println("-----------------------------------");
//			System.out.println(entry.getKey());

			TxLogicAnalyzer txLogicAnalyzer = new TxLogicAnalyzer();
			txLogicAnalyzer.obtainTxLogic(entry.getValue());


			Map<Integer, OperationData> operationDataTemplates = txName2OperationId2Template.get(entry.getKey());
			Map<Integer, OperationData> sortedOperationDataTemplates = new TreeMap<>();
			sortedOperationDataTemplates.putAll(operationDataTemplates); //qly：按照key值的升序进行排序
			Iterator<Entry<Integer, OperationData>> operationDataTemplateIter = sortedOperationDataTemplates.entrySet()
					.iterator(); //qly: 迭代器实现了对key值排序
			List<List<String>> statParameters = txLogicAnalyzer.getStatParameters(operationDataTemplateIter);
			txName2StatParameters.put(entry.getKey(), statParameters);

			// txName2ParaId2AvgRunTimes用来计算事务吞吐
			txName2ParaId2AvgRunTimes.put(entry.getKey(), new HashMap<>());
			txName2ParaId2AvgRunTimes.get(entry.getKey()).put("1_0",
					txLogicAnalyzer.getOperationId2AvgRunTimes().get(1));

			BlockingQueue<String> logBlockingQueue = new ArrayBlockingQueue<>(10000);
			LogSplitterQueueMap.put(entry.getKey(), logBlockingQueue); // qly: txname:logBlockingQueue,logBlockingQueue为引用，会在LogSplitterQueueMap中被put进数据 ~
			new Thread(new LogSplitter(entry.getKey(), logBlockingQueue, timeWindowSize, windowDataBlockingQueues,
					txName2StatParameters, cdl)).start();

			// for workload generator! -- 为每个事务设置事务逻辑信息
			for (int i = 0; i < transactions.size(); i++) {
				if (transactions.get(i).getName().equals(entry.getKey())) {
					transactions.get(i).setTransactionLogicInfo(txLogicAnalyzer.getParameterNodeMap(),
							txLogicAnalyzer.getMultipleLogicMap(), txLogicAnalyzer.getOperationId2AvgRunTimes());
					break;
				} //qly:在这里才把事务逻辑，每个操作平均执行的次数和Multiple逻辑关系加入transaction中
			}
		}
//		System.out.println("*********2*********\n"+transactions);
		// 输出指定事务逻辑信息 by zsy
//		for (int i = 0; i < transactions.size(); i++) {
//			for (String para : transactions.get(i).getParameterNodeMap().keySet()) {
//				if (transactions.get(i).getParameterNodeMap().get(para).getDependencies().size() > 2) {
//					System.out.println(transactions.get(i).getName() + " " + para);
//					System.out.println(transactions.get(i).getParameterNodeMap().get(para).getDependencies());
//					System.out.println(transactions.get(i).getMultipleLogicMap().get("multiple_" + para));
//				}
//			}
//			if (transactions.get(i).getName().equals("NewOrder")) {
//				// 循环执行次数
//				System.out.println("操作7和操作8的平均执行次数（理论上应该一样）：" + transactions.get(i).getOperationId2AvgRunTimes().get(7)
//						+ "和" + transactions.get(i).getOperationId2AvgRunTimes().get(9));
//				break;
//			}
//		}

		//added by lyqu
		/** 将txnId2txnTrace中已经在事务逻辑中存在的参数值删掉，因为它们在生成的时候肯定是按照事务逻辑生成的*/
		/** 目前直接将txnId2txnTrace直接改写了 */
		DeleteLogicalTxnPara deleteLogicalTxnPara = new DeleteLogicalTxnPara(workloadReader.txnId2txnTemplateID, workloadReader.txnId2txnTrace, transactions);

		deleteLogicalTxnPara.changeForm();
		deleteLogicalTxnPara.deleteTxnLogicPara();

		DistributionCounter.init(txName2StatParameters);
		DistributionCounter.setTxName2ParaId2AvgRunTimes(txName2ParaId2AvgRunTimes);
		for (int i = 0; i < statThreadNum; i++) {
			new Thread(new DistributionStatisticalThread(windowDataBlockingQueues.get(i), txName2ParaId2DistTypeInfo))
					.start();
		}

//

//		System.out.println(deleteLogicalTxnPara.getTxnId2txnTraceAfterDelete());

		//TODO :LYQU 遍历一遍，将存在事务逻辑的参数值 用占位符取代。
		if (Configurations.isUseSkywalking()) {
			new Thread(new LogReaderForTidb(workloadReader.txnId2txnTrace, workloadReader.txnId2txnTemplateID,
					LogSplitterQueueMap)).start();
		//TODO: 同上
		} else {
			File[] logFiles = new File(Configurations.getRunningLogDir4Distribution()).listFiles();
			// 对日志文件进行排序 - 序号大的在前，最后一个lauca.log文件名修改为lauca.log.0
			// 文件名的格式为: 'lauca.log.xx'
			Arrays.sort(logFiles, new Comparator<File>() {
				@Override
				public int compare(File file1, File file2) {
//				int sequenceNumber1 = Integer.parseInt(file1.getName().split("\\.")[2]);
//				int sequenceNumber2 = Integer.parseInt(file2.getName().split("\\.")[2]);
					String[] fileName1 = file1.getName().split("\\.");
					String[] fileName2 = file2.getName().split("\\.");
					int sequenceNumber1 = (fileName1.length > 2) ? Integer.parseInt(fileName1[2]) : 0;
					int sequenceNumber2 = (fileName2.length > 2) ? Integer.parseInt(fileName2[2]) : 0;
					if (sequenceNumber1 < sequenceNumber2) {
						return 1;
					} else if (sequenceNumber1 > sequenceNumber2) {
						return -1;
					} else {
						return 0;
					}
				}
			});

			new Thread(new LogReader(logFiles, LogSplitterQueueMap)).start();
		}
		try {
			cdl.await();
			// 关闭所有分布统计线程（DistributionStatisticalThread）
			for (int i = 0; i < windowDataBlockingQueues.size(); i++) {
				windowDataBlockingQueues.get(i).put(new WindowData("txName", "paraIdentifier", -1, null));
			}

			Thread.sleep(3000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}


		// 输出指定事务访问分布信息 by zsy
		// 事务名称 -> 参数标示符 -> 数据分布信息列表
//		for (int i = 3; i < 5; ++i) {
//			SequentialCtnsParaDistribution dis = (SequentialCtnsParaDistribution) DistributionCounter
//					.getTxName2ParaId2DistributionList().get("NewOrder").get("1_2").get(i);
//			System.out.println(dis);
//			System.out.println();
//		}

		// 基于采样的数据，统计全局数据访问分布
		DistributionCounter.countFullLifeCycleDistribution();
//		System.out.println("DistributionCounter\n"+DistributionCounter.getTxName2ParaId2DistributionList());



		//todo 20200127 支持主动回滚




		return transactions;
	}

	private static Map<String, Map<String, DistributionTypeInfo>> getParaDistributionTypeInfo(
			Map<String, Map<Integer, DistributionTypeInfo[]>> txName2OperationId2paraDistTypeInfos) {
		// 事务名称 -> 参数标识符（operationId + "_" + paraIndex） -> 数据分布类型信息
		Map<String, Map<String, DistributionTypeInfo>> txName2ParaId2DistTypeInfo = new HashMap<>();

		Iterator<Entry<String, Map<Integer, DistributionTypeInfo[]>>> iter = txName2OperationId2paraDistTypeInfos
				.entrySet().iterator();

		while (iter.hasNext()) {
			Entry<String, Map<Integer, DistributionTypeInfo[]>> entry = iter.next();
			txName2ParaId2DistTypeInfo.put(entry.getKey(), new HashMap<>());

			Iterator<Entry<Integer, DistributionTypeInfo[]>> iter2 = entry.getValue().entrySet().iterator();
			while (iter2.hasNext()) {
				Entry<Integer, DistributionTypeInfo[]> entry2 = iter2.next();
				int operationId = entry2.getKey();
				DistributionTypeInfo[] paraDistTypeInfos = entry2.getValue();
				for (int i = 0; paraDistTypeInfos != null && i < paraDistTypeInfos.length; i++) {
					String paraIdentifier = operationId + "_" + i;
					txName2ParaId2DistTypeInfo.get(entry.getKey()).put(paraIdentifier, paraDistTypeInfos[i]);
				}
			}
		}

		return txName2ParaId2DistTypeInfo;
	}
}

/**
 * 单线程读取日志文件，然后根据事务名称将事务日志分流到各个事务的LogSplitter线程中进行时间窗口划分
 */
class LogReader implements Runnable {

	private File[] logFiles = null; // 支持输入一个日志文件夹
	// 数据结构：事务名称 -> 相应事务日志切分线程的阻塞队列
	private Map<String, BlockingQueue<String>> LogSplitterQueueMap = null;

	public LogReader(File[] logFiles, Map<String, BlockingQueue<String>> LogSplitterQueueMap) {
		super();
		this.logFiles = logFiles;
		this.LogSplitterQueueMap = LogSplitterQueueMap;
	}

	@Override
	public void run() {
		System.out.println("为获取数据访问分布而读取的负载轨迹：");
		String logPrefix = "lauca;";
		for (File logFile : logFiles) {
			try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(logFile), "utf-8"))) {
				System.out.println(logFile.getName());
				String inputLine = null;
				while ((inputLine = br.readLine()) != null) {
					// 日志格式：lauca; current time(long型); 事务名称; 操作id; para1, para2, ...
					if (inputLine.startsWith(logPrefix)) {
						String[] arr = inputLine.split(";", 4);
						//TODO: 为每个事务都构建，需要的时候再做吧。将已经有参数依赖的数据删掉~

						// arr[2]：事务名称； arr[1]：日志时间；arr[3]：操作id和输入参数
						LogSplitterQueueMap.get(arr[2]).put(arr[1] + ";" + arr[3]);
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		try {
			// 通知LogSplitter线程 日志文件已读取结束
			Iterator<Entry<String, BlockingQueue<String>>> iter = LogSplitterQueueMap.entrySet().iterator();
			while (iter.hasNext()) {
				// 日志时间; 操作id和输入参数
				iter.next().getValue().put("-1; end");
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}

/**
 * 单线程读取TraceInfo，然后根据事务名称将事务日志分流到各个事务的LogSplitter线程中进行时间窗口划分
 * 
 * @author Shuyan Zhang
 */
class LogReaderForTidb implements Runnable {

	// 数据结构：事务名称 -> 相应事务日志切分线程的阻塞队列
	private Map<String, BlockingQueue<String>> LogSplitterQueueMap = null;
	private Map<Long, List<TraceInfo>> txnId2txnTrace = null;
	private Map<Long, Integer> txnId2txnTemplateID = null;

	public LogReaderForTidb(Map<Long, List<TraceInfo>> txnId2txnTrace, Map<Long, Integer> txnId2txnTemplateID,
			Map<String, BlockingQueue<String>> LogSplitterQueueMap) {
		super();
		this.txnId2txnTrace = txnId2txnTrace;
		this.txnId2txnTemplateID = txnId2txnTemplateID;
		this.LogSplitterQueueMap = LogSplitterQueueMap;
	}

	@Override
	public void run() {
		for (Entry<Long, List<TraceInfo>> txnIdAndtxnTrace : this.txnId2txnTrace.entrySet()) {
//			System.out.println("************** AFTER ********");  //qly : 顺序没问题
			String txnName = "Transaction" + this.txnId2txnTemplateID.get(txnIdAndtxnTrace.getKey());
			for (TraceInfo trace : txnIdAndtxnTrace.getValue()) {
				String prefix = trace.operationTS + ";" + trace.operationID + ";";  //qly: trace.operationTS是日志时间
				List<String> paras = trace.parameters;
				String info = new String(prefix);
				for (String para : paras) {
					info = info + para + ",";   //qly: info 为 事务名称; 操作id; para1, para2, ...
				}
				info.substring(0, info.length() - 1);
				try {
					LogSplitterQueueMap.get(txnName).put(info);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		try {
			// 通知LogSplitter线程 日志文件已读取结束
			Iterator<Entry<String, BlockingQueue<String>>> iter = LogSplitterQueueMap.entrySet().iterator();
			while (iter.hasNext()) {
				// 日志时间; 操作id和输入参数
				iter.next().getValue().put("-1; end");
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}

/**
 * 针对每类事务（某个事务模板）都会起一个LogSplitter线程。其主要作用是根据日志中的时间将日志划分成到一个个时间窗口中，
 * 时间窗口的大小由用户设置。这里需要注意日志可能会有轻微的乱序，需要有针对性的处理（假设日志乱序的幅度小于设置的时间窗口大小）
 */
class LogSplitter implements Runnable {

	private String txName = null;
	private BlockingQueue<String> logBlockingQueue = null;
	// 时间窗口大小，单位为秒
	private int timeWindowSize;

	// 将一个个时间窗口的日志数据传给DistributionStatisticalThread进行数据分布统计
	private List<BlockingQueue<WindowData>> windowDataBlockingQueues = null; //qly: 一个windowData是一条日志吗，还是说一秒？，
																			// 感觉像List中一条数据是一秒

	// cdl的作用：待所有LogSplitter线程结束后显示关闭DistributionStatisticalThread
	private CountDownLatch cdl = null;

	// 日志处理中需要的数据结构：事务名称 -> 参数标示符 -> 一个时间窗口内某个参数的所有原始数据（还可过滤掉不需要统计数据分布的参数数据）

	private Map<String, Map<String, List<String>>> txName2ParaId2DataList = null;

	// 下面两个成员变量是为了保证所有事务的日志起始时间都相同
	private static volatile boolean settedFlag = false;
	private static volatile long classCurrentWindowStartTime;

	public LogSplitter(String txName, BlockingQueue<String> logBlockingQueue, int timeWindowSize,
			List<BlockingQueue<WindowData>> windowDataBlockingQueues,
			Map<String, List<List<String>>> txName2StatParameters, CountDownLatch cdl) {
		super();
		this.txName = txName;
		this.logBlockingQueue = logBlockingQueue;
		this.timeWindowSize = timeWindowSize;
		this.windowDataBlockingQueues = windowDataBlockingQueues;
		this.cdl = cdl;

		//System.out.println("I am in Construction LogSplitter ******* " + logBlockingQueue);   (此时logBlockingQueue为空)
		// 构造txName2ParaId2DataList，只有需要统计数据分布的参数数据才会被传到统计线程那去
		// 其实都统计了

		txName2ParaId2DataList = new HashMap<>();
		Iterator<Entry<String, List<List<String>>>> iter = txName2StatParameters.entrySet().iterator();
		while (iter.hasNext()) {
			Entry<String, List<List<String>>> entry = iter.next();
			Map<String, List<String>> paraId2DataList = new HashMap<>();
			txName2ParaId2DataList.put(entry.getKey(), paraId2DataList);

			for (List<String> parameters : entry.getValue()) {
				for (String parameter : parameters) {
					String[] arr = parameter.split("_");
					int operationId = Integer.parseInt(arr[0]);
					int paraIndex = Integer.parseInt(arr[2]);
					String paraIdentifier = operationId + "_" + paraIndex;
					paraId2DataList.put(paraIdentifier, new ArrayList<>()); //qly: 如1_2,[]
				}
			}
		}
	}

	//qly: TODO: 这里之前可以将存在事务逻辑的参数删掉
	@Override
	public void run() {
		try {
			// 日志中的时间是以毫秒为单位的
			//System.out.println("1111111 logBlockingQueue:\n"+logBlockingQueue);  //此时为空
			//System.out.println("1111 ThreadName:\n"+Thread.currentThread().getName());
			int timeWindowMillis = timeWindowSize * 1000;

			// priorWindowLog & currentWindowLog：支持乱序日志的处理，但是日志的乱序幅度必须小于时间窗口大小
			// priorWindowLog存储上一个时间窗口的日志，currentWindowLog存储当前时间窗口的日志
			List<String> priorWindowLog = new ArrayList<>();
			List<String> currentWindowLog = new ArrayList<>();

			// priorWindowStartTime存储的是上一个时间窗口的起始时间点，currentWindowStartTime存储的是当前时间窗口的起始时间点
			long priorWindowStartTime, currentWindowStartTime;

			synchronized (LogSplitter.class) {   //qly: 拦截所有线程，只能让一个线程访问(这里一个事务一个线程）
				// 使得第一个时间窗口的日志一定不会落到priorWindowLog中
				priorWindowStartTime = -timeWindowMillis;

				if (!settedFlag) {
					String[] arr = logBlockingQueue.take().split(";", 2);  //qly : 这里不都是空的吗！！！ 存疑 ~
//					System.out.println("****************** I am in LogSplitter ******************"+logBlockingQueue+"\n"+ arr); //跑的时候只会出现一次，每次还都不一样
					// 第一个时间窗口的日志用currentWindowLog存储~
					currentWindowLog.add(arr[1]);
					currentWindowStartTime = Long.parseLong(arr[0]);

					classCurrentWindowStartTime = currentWindowStartTime;
					settedFlag = true;
				} else {
					currentWindowStartTime = classCurrentWindowStartTime;
				}
			}

			// 标示当前日志行是否已成功划分到某个时间窗口中
			boolean processedFlag = true;
			String[] arr = null;
//			System.out.println("********* Before **********");
			while (true) {
				if (processedFlag) {
//					System.out.println("111processedFlag************ \n"+logBlockingQueue);  qly: 存在值，它是怎么来的？？ 存疑 ~
					// 当前日志格式：日志时间; 操作id; para1, para2, ...
					arr = logBlockingQueue.take().split(";", 2);
				}

				long logTime = Long.parseLong(arr[0]);
				if (logTime < 0) {// 为什么会小于0？
					routeData(txName, currentWindowStartTime, priorWindowLog);
					routeData(txName, currentWindowStartTime + timeWindowMillis, currentWindowLog);
					break;
				}

				if (logTime - priorWindowStartTime <= timeWindowMillis) {
					priorWindowLog.add(arr[1]);
					processedFlag = true;
				} else if (logTime - currentWindowStartTime <= timeWindowMillis) {
					currentWindowLog.add(arr[1]);
					processedFlag = true;
				} else {
					if (priorWindowLog.size() > 0) {
						routeData(txName, currentWindowStartTime, priorWindowLog);
					}

					priorWindowStartTime = currentWindowStartTime;
					currentWindowStartTime = currentWindowStartTime + timeWindowMillis;

					priorWindowLog = currentWindowLog;
					currentWindowLog = new ArrayList<>();
					processedFlag = false;
				}
			}
//			System.out.println("222 processedFlag************ \n"+logBlockingQueue);  // qly: 取空了，已经处理完了

			cdl.countDown();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// 将当前时间窗口中的各个参数的日志数据路由给统计线程进行数据分布的统计。这里我们需要保证将同一个参数的数据确定性地路由到某个特定
	// 的统计线程中，这样我们便可以保证对于同一个参数，数据分布的统计在时间上是有序的~（方便针对基于连续时间窗口数据访问分布的统计）
	private void routeData(String txName, long windowTime, List<String> windowLog) {
		Map<String, List<String>> paraId2DataList = txName2ParaId2DataList.get(txName);  //qly 传进来的时候paraId2DataList是空的~
		for (String operationData : windowLog) {   // qly windowLog日志格式： 操作id; para1, para2, ...
			String[] arr = operationData.split(";");
			String operationId = arr[0].trim();
			String[] parameters = arr[1].split(",");
			for (int i = 0; i < parameters.length; i++) {
				String identifier = operationId + "_" + i;
				// 过滤掉不需要统计数据访问分布的参数数据
				//qly TODO: 目前没有根据概率过滤呢 TODO 20201222 在这里将值为 #@# 的删掉！
				//todo: 20210127 这里删的太早了，之后还得统计呢
				if (paraId2DataList.containsKey(identifier) ) { /* && !parameters[i].equals("#@#") */
					paraId2DataList.get(identifier).add(parameters[i].trim());
				}
			}
		}

		Iterator<Entry<String, List<String>>> iter = paraId2DataList.entrySet().iterator();
		while (iter.hasNext()) {
			Entry<String, List<String>> entry = iter.next();
			String tmp = txName + entry.getKey();
			int idx = Math.abs(tmp.hashCode()) % windowDataBlockingQueues.size();
			WindowData windowParaData = new WindowData(txName, entry.getKey(), windowTime, entry.getValue());
			try {
				windowDataBlockingQueues.get(idx).put(windowParaData);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			paraId2DataList.put(entry.getKey(), new ArrayList<>());
		}
	}
}

class DistributionStatisticalThread implements Runnable {

	private BlockingQueue<WindowData> windowDataBlockingQueue = null;
	// 事务名称 -> 参数标示符（operationId + "_" + paraIndex） -> 参数访问分布的类型信息
	private Map<String, Map<String, DistributionTypeInfo>> txName2ParaId2DistTypeInfo = null;

	public DistributionStatisticalThread(BlockingQueue<WindowData> windowDataBlockingQueue,
			Map<String, Map<String, DistributionTypeInfo>> txName2ParaId2DistTypeInfo) {
		super();
		this.windowDataBlockingQueue = windowDataBlockingQueue;
		this.txName2ParaId2DistTypeInfo = txName2ParaId2DistTypeInfo;
	}

	@Override
	public void run() {
		try {
			while (true) {
				WindowData windowData = windowDataBlockingQueue.take();
				if (windowData.windowTime < 0) {
					break;
				}

				DistributionTypeInfo distTypeInfo = txName2ParaId2DistTypeInfo.get(windowData.txName)
						.get(windowData.paraIdentifier);

				DistributionCounter.count(windowData.txName, windowData.paraIdentifier, windowData.windowTime,
						distTypeInfo, windowData.data);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}

class WindowData {

	String txName = null;
	// 格式：operationId_index index从0开始
	String paraIdentifier = null;
	// 应该是窗口结束时间
	long windowTime;
	List<String> data = null;

	public WindowData(String txName, String paraIdentifier, long windowTime, List<String> data) {
		super();
		this.txName = txName;
		this.paraIdentifier = paraIdentifier;
		this.windowTime = windowTime;
		this.data = data;
	}

	@Override
	public String toString() {
		return "WindowData [txName=" + txName + ", paraIdentifier=" + paraIdentifier + ", windowTime=" + windowTime
				+ ", size of data=" + data.size() + "]";
	}
}
