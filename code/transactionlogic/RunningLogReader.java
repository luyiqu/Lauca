package transactionlogic;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import abstraction.StoredProcedure;
import abstraction.Table;
import abstraction.Transaction;
import config.Configurations;
import input.StoredProcedureReader;
import input.TableInfoSerializer;
import input.TraceInfo;
import input.WorkloadReader;

/**
 * 读取供事务逻辑分析的运行日志
 */
public class RunningLogReader {
	/** 事务名称->旗下各个事务实例的数据，read方法或readTidbTrace方法调用后被填充 */
	public  Map<String, List<TransactionData>> txName2TxDataList = null;

	// 控制每个事务的数据量（事务逻辑分析不需要大量的事务实例数据）
	private Map<String, Integer> txName2Amount = null;
	// 每个事务的最大事务实例数据量 -- 由用户配置
	private int maxSizeOfTxDataList;

	// 每个事务模板中的操作数
	private Map<String, Integer> txName2OperationNum = null;

	// 事务名称 -> 操作id -> OperationData模板
	private Map<String, Map<Integer, OperationData>> txName2OperationId2Template = null;

	//爲了統計主動回滾的比例 added by lyqu
	public static Map<String,Integer> txName2AllAmount = new HashMap<>();
	//----
	public RunningLogReader(Map<String, Map<Integer, OperationData>> txName2OperationId2Template) {
		this.txName2OperationId2Template = txName2OperationId2Template;

		txName2TxDataList = new HashMap<>();
		txName2Amount = new HashMap<>();
		maxSizeOfTxDataList = Configurations.getMaxSizeOfTxDataList();
		txName2OperationNum = new HashMap<>();

		for (Entry<String, Map<Integer, OperationData>> entry : txName2OperationId2Template.entrySet()) {
			txName2OperationNum.put(entry.getKey(), entry.getValue().size());
		}
	}

	/**
	 * 使用Tidb的general log或Oracle应用端打出的log，此时已经读出参数和返回集，只需进一步处理
	 * 
	 * @param txnId2txnTrace txnId2txnTemplateID
	 * @author Shuyan Zhang
	 */
	public void readTrace(Map<Long, List<TraceInfo>> txnId2txnTrace, Map<Long, Integer> txnId2txnTemplateID) {
		// 处理每一个事务实例
		for (Entry<Long, List<TraceInfo>> txnIdAndtxnTrace : txnId2txnTrace.entrySet()) {
//			List<String> operationLogs = iter.next().getValue();
			List<OperationData> operationDataList = new ArrayList<>();
			Set<Integer> operationIdSet = new HashSet<>();
			String txName = "Transaction" + txnId2txnTemplateID.get(txnIdAndtxnTrace.getKey());
			for (TraceInfo oneTrace : txnIdAndtxnTrace.getValue()) {
				int operationId = oneTrace.operationID;
//				for (int i = 0; i < oneTrace.parameters.size(); ++i) {
				try{
					operationDataList
							.add(txName2OperationId2Template.get(txName).get(operationId).newInstance(oneTrace));
					operationIdSet.add(operationId);
				}catch (Exception e){
					e.printStackTrace();
					System.out.println(txName);
					System.out.println(txName2OperationId2Template.get(txName).get(operationId));
					System.out.println("*************");
					System.out.println(oneTrace);

				}

//				}
			}
//			System.out.println("******************");
//			System.out.println(operationDataList);
//			for (String operationLog : operationLogs) {
//				String[] arr = operationLog.split(";", 3);
//				txName = arr[0].trim(); // for循环内txName都是一样的
//				int operationId = Integer.parseInt(arr[1].trim());
//				operationDataList
//						.add(txName2OperationId2Template.get(txName).get(operationId).newInstance(arr[2].trim()));
//				operationIdSet.add(operationId);
//			}
			// This sort is guaranteed to be stable!
			Collections.sort(operationDataList);

			// 控制每个事务模板的实例数
			if (!txName2Amount.containsKey(txName)) {
				txName2Amount.put(txName, 1);
			} else {
				txName2Amount.put(txName, txName2Amount.get(txName) + 1);
				if (txName2Amount.get(txName) > maxSizeOfTxDataList) {
					continue;
				}
			}

			int operationNum = txName2OperationNum.get(txName);
			Object[] operationDatas = new Object[operationNum];
			int[] operationTypes = new int[operationNum];
			int idx = 0;
			for (int i = 1; i <= operationNum; i++) {
				if (operationIdSet.contains(i)) {
					List<OperationData> tmp = new ArrayList<>();
					for (; idx < operationDataList.size() && operationDataList.get(idx).getOperationId() == i; idx++) {
						tmp.add(operationDataList.get(idx));
					}
					if (tmp.size() == 1) {// 不是循环中的操作或者只执行了一次
						operationTypes[i - 1] = 0;
						operationDatas[i - 1] = tmp.get(0);
					} else {// 是循环中的操作
						operationTypes[i - 1] = 1;
						operationDatas[i - 1] = tmp;
					}
				} else {// 可能是未执行的分支
					operationTypes[i - 1] = -1;
					operationDatas[i - 1] = null;
				}
			}
			if (!txName2TxDataList.containsKey(txName)) {
//				System.out.println("txName: "+txName);
				txName2TxDataList.put(txName, new ArrayList<TransactionData>());
			}
			txName2TxDataList.get(txName).add(new TransactionData(operationDatas, operationTypes));
		}
		//added by lyqu
		for (Entry<Long, List<TraceInfo>> txnIdAndtxnTrace : txnId2txnTrace.entrySet()) {
			String txName = "Transaction" + txnId2txnTemplateID.get(txnIdAndtxnTrace.getKey());
			// 控制每个事务模板的实例数
			if (!txName2AllAmount.containsKey(txName)) {
				txName2AllAmount.put(txName, 1);
			} else {
				txName2AllAmount.put(txName, txName2AllAmount.get(txName) + 1);

			}
		}
		//----

	}

	// 供事务逻辑分析的日志量不需要很大,每个事务模板10000个左右实例数据即可
	public void read(File runningLogDir) {

		// 全局事务id -> 该事务实例的所有操作日志
		Map<Integer, List<String>> globalId2OperationLogs = new HashMap<>();
		File[] runningLogFiles = runningLogDir.listFiles();

		// 文件名的格式为: 'lauca.log.xx'
		Arrays.sort(runningLogFiles, new Comparator<File>() {
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

		// globalIdMaxNum: 控制事务实例的数量，避免内存溢出，降低复杂度
		// bufferTime: 尽可能保证拿到的事务实例数据是完整的
		int globalIdMaxNum = 100000, bufferTime = 10000;
		boolean enoughFlag = false;
		// log格式为: 'lauca; 全局事务id; 事务名称; 操作id; para1, para2, ...; res1, res2, ...# ...'
		System.out.println("为获取事务逻辑而读取的负载轨迹（或者叫运行日志，即论文中的workload trace），需注意日志读取的顺序：");
		loop: for (int i = 0; i < runningLogFiles.length; i++) {
			File runningLogFile = runningLogFiles[i];
//			System.out.println(runningLogFile);
			try (BufferedReader br = new BufferedReader(
					new InputStreamReader(new FileInputStream(runningLogFile), "utf-8"))) {
				System.out.println(runningLogFile.getName());
				String inputLine = null;
				String logPrefix = "lauca;";
				while ((inputLine = br.readLine()) != null) {
					if (inputLine.startsWith(logPrefix)) {
						String[] arr = inputLine.split(";", 3);
						int globalId = Integer.parseInt(arr[1].trim());
						if (!globalId2OperationLogs.containsKey(globalId)) {
							if (enoughFlag) {
								if (bufferTime-- < 0) {
									break loop;
								} else {
									continue;
								}
							}
							globalId2OperationLogs.put(globalId, new ArrayList<String>());
						}
						globalId2OperationLogs.get(globalId).add(arr[2].trim());
						if (globalId2OperationLogs.size() >= globalIdMaxNum) {
							enoughFlag = true;
						}
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		// 当前log格式为: '事务名称; 操作id; para1, para2, ...; res1, res2, ...# ...'
		Iterator<Entry<Integer, List<String>>> iter = globalId2OperationLogs.entrySet().iterator();
		// 处理每一个事务实例
		while (iter.hasNext()) {
			List<String> operationLogs = iter.next().getValue();
			List<OperationData> operationDataList = new ArrayList<>();
			Set<Integer> operationIdSet = new HashSet<>();
			String txName = null;
			for (String operationLog : operationLogs) {
				String[] arr = operationLog.split(";", 3);
				txName = arr[0].trim(); // for循环内txName都是一样的
				int operationId = Integer.parseInt(arr[1].trim());

//				System.out.println("txName2Operation2Template: "+txName2OperationId2Template.get(txName).get(operationId));
				operationDataList
						.add(txName2OperationId2Template.get(txName).get(operationId).newInstance(arr[2].trim()));
				operationIdSet.add(operationId);
			}
			// This sort is guaranteed to be stable!
			Collections.sort(operationDataList);

			// 控制每个事务模板的实例数
			if (!txName2Amount.containsKey(txName)) {
				txName2Amount.put(txName, 1);
			} else {
				txName2Amount.put(txName, txName2Amount.get(txName) + 1);
				if (txName2Amount.get(txName) > maxSizeOfTxDataList) {
					continue;
				}
			}

			int operationNum = txName2OperationNum.get(txName);
			Object[] operationDatas = new Object[operationNum];
			int[] operationTypes = new int[operationNum];
			int idx = 0;
			for (int i = 1; i <= operationNum; i++) {
				if (operationIdSet.contains(i)) {
					List<OperationData> tmp = new ArrayList<>();
					for (; idx < operationDataList.size() && operationDataList.get(idx).getOperationId() == i; idx++) {
						tmp.add(operationDataList.get(idx));
					}
					if (tmp.size() == 1) {// 不是循环中的操作或者只执行了一次
						operationTypes[i - 1] = 0;
						operationDatas[i - 1] = tmp.get(0);
					} else {// 是循环中的操作
						operationTypes[i - 1] = 1;
						operationDatas[i - 1] = tmp;
					}
				} else {// 可能是未执行的分支
					operationTypes[i - 1] = -1;
					operationDatas[i - 1] = null;
				}
			}
			if (!txName2TxDataList.containsKey(txName)) {
				txName2TxDataList.put(txName, new ArrayList<TransactionData>());
			}
			txName2TxDataList.get(txName).add(new TransactionData(operationDatas, operationTypes));
		}
	}

	public Map<String, List<TransactionData>> getTxName2TxDataList() {
		return txName2TxDataList;
	}

	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		TableInfoSerializer serializer = new TableInfoSerializer();
		List<Table> tables = serializer.read(new File("D://dataCharacteristicSaveFile.obj"));
		StoredProcedureReader storedProcedureReader = new StoredProcedureReader();
		List<StoredProcedure> storedProcedures = storedProcedureReader.read(new File("D://storedProcedure.txt"));
		WorkloadReader workloadReader = new WorkloadReader(tables,storedProcedures);
		List<Transaction> transactions = workloadReader.read(new File(".//testdata//tpcc-transactions.txt"));
		// System.out.println(transactions);

		Preprocessor preprocessor = new Preprocessor();
		preprocessor.constructOperationTemplateAndDistInfo(transactions);

		RunningLogReader runningLogReader = new RunningLogReader(preprocessor.getTxName2OperationId2Template());
		runningLogReader.read(new File(".//testdata//log4txlogic"));

		System.out.println(runningLogReader.txName2Amount);
		Iterator<Entry<String, List<TransactionData>>> iter = runningLogReader.txName2TxDataList.entrySet().iterator();
		while (iter.hasNext()) {
			Entry<String, List<TransactionData>> entry = iter.next();
			System.out.println(entry.getKey() + ": " + entry.getValue().size());

			TransactionData tmp = entry.getValue().get(0);
			System.out.println(Arrays.toString(tmp.getOperationTypes()));
			for (int i = 0; i < tmp.getOperationTypes().length; i++) {
				if (tmp.getOperationTypes()[i] == 0) {
					System.out.println((OperationData) tmp.getOperationDatas()[i]);
				} else if (tmp.getOperationTypes()[i] == 1) {
					System.out.println((ArrayList<OperationData>) tmp.getOperationDatas()[i]);
				} else {
					System.out.println("null");
				}
			}

			tmp = entry.getValue().get(1);
			System.out.println(Arrays.toString(tmp.getOperationTypes()));
			for (int i = 0; i < tmp.getOperationTypes().length; i++) {
				if (tmp.getOperationTypes()[i] == 0) {
					System.out.println((OperationData) tmp.getOperationDatas()[i]);
				} else if (tmp.getOperationTypes()[i] == 1) {
					System.out.println((ArrayList<OperationData>) tmp.getOperationDatas()[i]);
				} else {
					System.out.println("null");
				}
			}
		}
	}
}
