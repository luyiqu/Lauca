package input;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import abstraction.*;
import accessdistribution.DataAccessDistribution;
import accessdistribution.DeleteLogicalTxnPara;
import accessdistribution.SequentialParaDistribution;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.sun.org.apache.bcel.internal.generic.GOTO;
import config.Configurations;
import org.apache.log4j.Logger;
import serializable.DataAccessDistributionAdapter;
import serializable.SequentialParaDistributionAdapter;
import serializable.SqlStatementAdapter;
import serializable.TransactionBlockAdapter;

import javax.swing.plaf.IconUIResource;


public class WorkloadReader {

	private Logger logger = Logger.getLogger(input.WorkloadReader.class);

	private List<Table> tables = null;
	private List<StoredProcedure> storedProcedures = null;
	/** 记录TiDB日志/Oracle日志中负载轨迹的信息,一条sql对应一个TraceInfo;注意由于抛弃了回滚事务，里面的事务号不是连续的 */
	public Map<Long, List<TraceInfo>> txnId2txnTrace = new HashMap<>();
	/** 记录每个事务实例对应的事务模板号（相当于事务名）,模板号从1开始 */
	public Map<Long, Integer> txnId2txnTemplateID = new HashMap<>();
//	public Map<String,Integer> rollbackTxname2BlockId2Count = new HashMap<>();  //added by lyqu 支持主动回滚
	public static Map<Long,List<String>> rollbacktxnId2txnInstance = new HashMap<>();  //added by lyqu
	public WorkloadReader(List<Table> tables) {
		this.tables = tables;
	}

	public WorkloadReader(List<Table> tables, List<StoredProcedure> storedProcedures){
		this.tables = tables;
		this.storedProcedures = storedProcedures;
	}

	public static void main(String[] args) {
//		PropertyConfigurator.configure(".//lib//log4j.properties");
		TableInfoSerializer serializer = new TableInfoSerializer();
		//List<Table> tables = serializer.read(new File(".//testdata//dataCharacteristicSaveFile.obj"));
		//TableInfoSerializer serializer = new TableInfoSerializer();
		List<Table> tables = serializer.read(new File("E://dataCharacteristicSaveFile.obj"));

//		StoredProcedureReader storedProcedureReader = new StoredProcedureReader();
//		List<StoredProcedure> storedProcedures = storedProcedureReader.read(new File("D://storedProcedure.txt"));
//		WorkloadReader workloadReader = new WorkloadReader(tables,storedProcedures);
		WorkloadReader workloadReader = new WorkloadReader(tables);
//		List<Transaction> transactions = workloadReader.read(new File(".//testdata//tpcc-transactions.txt"));
//		System.out.println(transactions);
//		PrintStream ps = null;
//		try {
//			ps = new PrintStream(new FileOutputStream("D:\\Desktop\\out.txt"));
//		} catch (FileNotFoundException e) {
//			e.printStackTrace();
//		}
//		System.setOut(ps);

//		List<Transaction> transactions = workloadReader.readTidbLog(new File("D:\\Desktop\\tidb429.log"));
		List<Transaction> transactions = workloadReader.readLaucaLog(new File("E:\\lauca-log"));
//		try {
//			File file = new File("E:\\txLogicSaveFile.obj");
//			Long filelength = file.length();
//			byte[] filecontent = new byte[filelength.intValue()];
//			FileInputStream in = new FileInputStream(file);
//			in.read(filecontent);
//			in.close();
//			Gson gson = new GsonBuilder().registerTypeAdapter(TransactionBlock.class, new TransactionBlockAdapter())
//					.registerTypeAdapter(SqlStatement.class, new SqlStatementAdapter())
//					.registerTypeAdapter(DataAccessDistribution.class, new DataAccessDistributionAdapter())
//					.registerTypeAdapter(SequentialParaDistribution.class, new SequentialParaDistributionAdapter())
//					.create();
//			transactions = Arrays.asList(gson.fromJson(new String(filecontent, "UTF-8"), Transaction[].class));
//		}catch (Exception e){
//			System.out.println("transaction read error");
//		}

//		DeleteLogicalTxnPara deleteLogicalTxnPara = new DeleteLogicalTxnPara(workloadReader.txnId2txnTemplateID, workloadReader.txnId2txnTrace, transactions);
//
//		deleteLogicalTxnPara.changeForm();
//		deleteLogicalTxnPara.deleteTxnLogicPara();
//		System.out.println(deleteLogicalTxnPara.getTxnId2txnTraceAfterDelete());
//		System.out.println(workloadReader.txnId2txnTrace);



		//		System.out.println("*********************");
//		System.out.println(transactions.get(0));
//		for (Transaction txn : transactions) {
//			System.out.println(txn.getName());
//			for (TransactionBlock tb : txn.getTransactionBlocks()) {
//				if (tb instanceof Multiple) {
//					System.out.println("Loop");
//					for (SqlStatement ss : ((Multiple) tb).getSqls()) {
//						System.out.println(ss.sql);
//						String s="";
//						for(int i=0;i<ss.getParaDataTypes().length;++i) {
//							s+=ss.getParaDataTypes()[i]+" ";
//						}
//						System.out.println(s);
//					}
//					System.out.println("EndLoop");
//				} else {
//					System.out.println(((SqlStatement) tb).sql);
//					String s="";
//					for(int i=0;i<((SqlStatement) tb).getParaDataTypes().length;++i) {
//						s+=((SqlStatement) tb).getParaDataTypes()[i]+" ";
//					}
//					System.out.println(s);
//				}
//			}
//		}

//		// 每个事务模板所以选择一个事务实例打印
//		for (int i = 1; i <= transactions.size(); ++i) {
//			System.out.println(i);
//			long ran = (long) (Math.random() * (workloadReader.txnId2txnTemplateID.size() - 30));
//			long j;
//			for (j = ran; j < workloadReader.txnId2txnTemplateID.size(); ++j) {
//				if (workloadReader.txnId2txnTemplateID.get(j) != null
//						&& workloadReader.txnId2txnTemplateID.get(j) == i) {
//					break;
//				}
//			}
//			System.out.println(workloadReader.txnId2txnTrace.get(j));
//
//		}
//		System.out.println(workloadReader.txnId2txnTrace.size());

		// 打印所有事务实例
//		for (Entry<Long, List<TraceInfo>> id2info : workloadReader.txnId2txnTrace.entrySet()) {
//			System.out.println(workloadReader.txnId2txnTemplateID.get(id2info.getKey()));
//			System.out.println(id2info.getValue());
//		}

//		for (long j = 0; j < workloadReader.txnId2txnTemplateID.size(); ++j) {
//			if (workloadReader.txnId2txnTemplateID.get(j) != null
//					&& workloadReader.txnId2txnTemplateID.get(j) == 2) {
//				System.out.println(workloadReader.txnId2txnTrace.get(j));
//			}
//		}

	}

	/**
	 * 从Oracle客户端打的log中获取事务模板，同时获取负载轨迹
	 *
	 * @param logDir 存日志的目录
	 * @return
	 * @author Shuyan Zhang
	 */
	@SuppressWarnings("static-access")
	public List<Transaction> readLaucaLog(File logDir) {

		// 读取所有log
		// 格式：[timestamp=...][connId=……][sql=...][para=p1,p2,p3][para=p1,p2,p3]...[res=col1,col2,col3][res=col1,col2,col3]...
		File[] logFiles = logDir.listFiles();

		// 文件名的格式为: 'lauca.log.xx'，降序读
		Arrays.sort(logFiles, new Comparator<File>() {
			@Override
			public int compare(File file1, File file2) {
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

		//读取匿名化的中间状态文件，得到原表名、列名与匿名的对应
		AnonymityInfoSerializer ais = new AnonymityInfoSerializer();
		File infile = new File(Configurations.getAnonymitySaveFile());
		Anonymity anonymity = new Anonymity();
		if(Configurations.isEnableAnonymity()){
			anonymity = ais.read(infile);
			System.out.println(anonymity);
		}

		//todo 20200127 加入主动回滚 added by lyqu
//		HashMap<String,Integer> sqlRollbackCount = new HashMap<>();
//		HashMap<String,Integer> sqlTotalCount = new HashMap<>();
//		String beforeSql = null;
		//--
		List<input.OracleLog> oraclelogs = new ArrayList<>();
		for (int f = 0; f < logFiles.length; ++f) {
			System.out.println(logFiles[f]);
			try (BufferedReader br = new BufferedReader(
					new InputStreamReader(new FileInputStream(logFiles[f]), "utf-8"))) {
				String inputLine = null;
				// 解析每一行Log
				//记录上一条 判断是否重复
				String beforeline = inputLine;
				while ((inputLine = br.readLine()) != null) {
					// 过滤掉空行
					if (inputLine.matches("[\\s]*")) {
						continue;
					}

					if(beforeline!=null&&beforeline.equals(inputLine)){
//						System.out.println("重复~~:  "+inputLine);
						continue;
					}
//					beforeline = inputLine;
					String[] log = inputLine.split("\\[|\\]");
					ArrayList<String> refineLog = new ArrayList<>();
					for (String s : log) {
						if (!s.matches("[ ]*")) {
							refineLog.add(s);
						}
					}
					// 是否是lauca需要的log
					if (refineLog.size() < 3 || !refineLog.get(1).startsWith("connId")) {
						continue;
					}


					input.OracleLog oracleLog = new input.OracleLog();
					for (int i = 0; i < refineLog.size(); ++i) {
						String[] nameAndValue = refineLog.get(i).split("=", 2);
//						System.out.println(nameAndValue[0]);
						switch (nameAndValue[0]) {
							case "timestamp":
								oracleLog.operationTS = Long.parseLong(nameAndValue[1]);
								break;
							case "connId":
								oracleLog.connId = Integer.parseInt(nameAndValue[1]);
								break;
							case "sql":
//								System.out.println(refineLog.get(i));
//								if(nameAndValue[1].equals("rollback")){
////									System.out.println(beforeSql);
////									if(!sqlRollbackCount.containsKey(beforeSql)){
////										sqlRollbackCount.put(beforeSql,1);
////									}else{
////										sqlRollbackCount.put(beforeSql,sqlRollbackCount.get(beforeSql)+1);
////									}
//								}
								if(Configurations.isEnableAnonymity())
									oracleLog.sql = anonymity.sql2Anonymity(nameAndValue[1]);
								else
									oracleLog.sql = nameAndValue[1];
//								beforeSql = nameAndValue[1];
//								if(!sqlTotalCount.containsKey(nameAndValue[1])){
//									sqlTotalCount.put(nameAndValue[1],1);
//								}else{
//									sqlTotalCount.put(nameAndValue[1],sqlTotalCount.get(nameAndValue[1])+1);
//								}
								break;
							case "batchpara":
								oracleLog.setBatched(true);
							case "para":
								String[] paraArray = nameAndValue[1].split("\\,");
								oracleLog.parameters = new ArrayList<String>();
								// 去掉参数头尾引号
								for (int j = 0; j < paraArray.length; ++j) {
									if (paraArray[j].length() > 0 && paraArray[j].charAt(0) == '"') {
										paraArray[j] = paraArray[j].substring(1, paraArray[j].length() - 1);
									}
								}
								oracleLog.parameters = Arrays.asList(paraArray);
								break;
							case "res":
								String[] resArray = nameAndValue[1].split("\\,");
								if (oracleLog.results == null) {
									oracleLog.results = new ArrayList<List<String>>();
								}
								oracleLog.results.add(Arrays.asList(resArray));
								break;
							default:
								System.out.println(refineLog.get(i));
								System.out.println("日志格式不合法！");

						}

					}
					oraclelogs.add(oracleLog);

				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

//		System.out.println(generalLogs);

		// 将事务实例提取出来 （事务号->里面一条条sql）
		Map<Long, List<String>> txnId2txnInstance = new HashMap<>();


		// 记录每个链接当前在处理的事务号，没有在处理事务则为-1
		Map<Long, Long> connId2txnId = new HashMap<>();
		long txnId = 1;
		for (input.OracleLog aLog : oraclelogs) {
			// 将CRUD操作加入
			if (aLog.sql != null && aLog.sql.matches(
					"(CALL|call|Call|SELECT|Select|select|UPDATE|Update|update|INSERT|Insert|insert|DELETE|Delete|delete|REPLACE|Replace|replace)[\\s\\S]+")) {
				// SqlParser里再淘汰这种特殊的事务
//				if (aLog.sql.matches("(SELECT|Select|select)[\\s]+(@@)[\\s\\S]+")) {
//					continue;
//				}
				// 多个空白字符变成一个
				aLog.sql = aLog.sql.replaceAll("[\\s]+", " ");
				// 变成小写方便找关键词
				String tempSql = aLog.sql.toLowerCase();

				// 处理between谓词，col between A and B 转换成 col ≥ A and col ≤ B
				int betweenIndex;
				// 找到所有的between关键词
				while ((betweenIndex = tempSql.indexOf(" between ")) != -1) {
					// 找到col名字
					String firstHalf = aLog.sql.substring(0, betweenIndex);
					String[] subStrings = firstHalf.split(" ");
					String colName = subStrings[subStrings.length - 1];
					// 找到between后第一个and
					int andIndex = tempSql.indexOf("and", betweenIndex);
					// 在and后插入“ col ≤”
					StringBuffer sqlCopy = new StringBuffer(aLog.sql);
					sqlCopy.insert(andIndex + 3, " " + colName + " <=");

					sqlCopy.replace(betweenIndex + 1, betweenIndex + 8, ">=");
					aLog.sql = new String(sqlCopy);
					tempSql = aLog.sql.toLowerCase();
				}

				// 这是一个事务中的第一条sql
				if (connId2txnId.get(aLog.connId) == null || connId2txnId.get(aLog.connId) == -1) {
					connId2txnId.put(aLog.connId, txnId);
					this.txnId2txnTrace.put(txnId, new ArrayList<>());
					txnId2txnInstance.put(txnId, new ArrayList<>());
					txnId++;
				}
				txnId2txnInstance.get(connId2txnId.get(aLog.connId)).add(aLog.sql);
				//负载轨迹信息
				TraceInfo oneTrace = new TraceInfo();
				// 把操作时间戳信息加进oneTrace去
				oneTrace.operationTS = aLog.operationTS;
				oneTrace.results = aLog.results;
				oneTrace.parameters = aLog.parameters;
				oneTrace.isBatched = aLog.isBatched;
				this.txnId2txnTrace.get(connId2txnId.get(aLog.connId)).add(oneTrace);

			} else if (aLog.sql != null && aLog.sql.equals("commit")) {
				// 将连接对应事务号置为-1
				connId2txnId.put(aLog.connId, (long) -1);
			} else if (aLog.sql != null && aLog.sql.equals("rollback")) {
				// TODO 现在的处理是，回滚的事务就不要了，不然会找到一堆没用的事务模板.对应的轨迹也直接不要了
				rollbacktxnId2txnInstance.put(connId2txnId.get(aLog.connId),txnId2txnInstance.get(connId2txnId.get(aLog.connId))); //added by lyqu
				txnId2txnInstance.remove(connId2txnId.get(aLog.connId));
				this.txnId2txnTrace.remove(connId2txnId.get(aLog.connId));
				connId2txnId.put(aLog.connId, (long) -1);
			}
		}

//		boolean one = true;
//		for(Long txnId2 : txnId2txnInstance.keySet()){
//			if(one){
//				System.out.println(txnId2txnInstance.get(txnId2));
//				System.out.println(txnId2txnTrace.get(txnId2));
//				one = false;
//			}
//
//			if(txnId2txnInstance.get(txnId2).size() != txnId2txnTrace.get(txnId2).size()){
//				System.out.println(txnId2txnInstance.get(txnId));
//			}
//		}

		// for test
//		for (Entry<Long, List<String>> txn : txnId2txnInstance.entrySet()) {
//			System.out.println(txn.getKey());
//			for (int i = 0; i < txn.getValue().size(); ++i) {
//				System.out.println(txn.getValue().get(i));
//				System.out.println(this.txnId2txnTrace.get(txn.getKey()).get(i));
//			}
//		}

		// 提取出参数，并将sql语句中参数的位置换成？，提取事务模板
		Map<Long, List<TransactionBlock>> txnId2txnTemplate = new HashMap<>();
		SqlParser sqlParser;
		if(Configurations.isUseStoredProcedure()){
			sqlParser = new SqlParser(tables,storedProcedures);
		}
		else {
			sqlParser = new SqlParser(tables);
		}
		
		

		// 处理一个事务实例
		outer: for (Entry<Long, List<String>> txnIdAndInstance : txnId2txnInstance.entrySet()) {
			List<SqlStatement> txnStatements = new ArrayList<>();
			List<TraceInfo> txnTrace = this.txnId2txnTrace.get(txnIdAndInstance.getKey());
			// 处理每个带参数的sql
			for (int i = 0; i < txnIdAndInstance.getValue().size(); ++i) {
				String sql = txnIdAndInstance.getValue().get(i);
				List<String> sqlParas = new ArrayList<String>();
				SqlStatement currentStatement;
				if (sql.toLowerCase().startsWith("select")) {
					// 是预编译的sql模板
					if (txnTrace.get(i).parameters != null) {
						// 将参数传进去主要是为了调整日期类型的格式
//						System.out.println("Template");
						currentStatement = sqlParser.parseReadSqlTemplate(sql, -1, txnTrace.get(i).parameters);
					} else {
//						System.out.println("Statement");
						currentStatement = sqlParser.parseReadSqlStatement(sql, -1, sqlParas);
					}
					if (currentStatement == null) {
						// 不是啥正经select语句,如果这个事务实例只有这一句，那么整个实例都不要了；否则就只把这一句抛弃
						if (txnIdAndInstance.getValue().size() == 1) {
							continue outer;
						} else {
							txnIdAndInstance.getValue().remove(i);
							txnTrace.remove(i);
							--i;
							continue;
						}
					}
					// 将返回项中的timestamp转为datetime
					if (txnTrace.get(txnStatements.size()).results != null) {
						int[] returnTypes = ((ReadOperation) currentStatement).getReturnDataTypes();
						// 筛选出返回项中的null值，换成0,空串或false
						for (List<String> res : txnTrace.get(txnStatements.size()).results) {
							for (int j = 0; j < res.size(); ++j) {
								if (res.get(j).equals("NULL") || res.get(j).equals("null")) {
									if (returnTypes[j] < 4) {
										res.set(j, "0");
									} else if (returnTypes[j] == 4) {
										res.set(j, "");
									} else if (returnTypes[j] == 5) {
										res.set(j, "false");
									}
								}
							}
						}
						for (int j = 0; j < returnTypes.length; ++j) {
							if (returnTypes[j] == 3) {
								Timestamp ts = new Timestamp(0);
								for (List<String> res : txnTrace.get(txnStatements.size()).results) {
									if (res.get(j).contains("-")) {
										res.set(j, "" + ts.valueOf(res.get(j)).getTime());
									}
								}
							}
						}
					}
				} else {
					// 是预编译的sql模板
					if (txnTrace.get(i).parameters != null) {
						// 将参数传进去主要是为了调整日期类型的格式
//						System.out.println("Tem");
						currentStatement = sqlParser.parseWriteSqlTemplate(sql, txnTrace.get(i).isBatched, -1,
								txnTrace.get(i).parameters);
					} else {
//						System.out.println("State");
						currentStatement = sqlParser.parseWriteSqlStatement(sql, false, -1, sqlParas);  //qly: 这不是预编译的
					}
				}
				txnStatements.add(currentStatement);
				if (txnTrace.get(txnStatements.size() - 1).parameters == null) {
					txnTrace.get(txnStatements.size() - 1).parameters = sqlParas;
				}
			}

//			System.out.println("txnStatements: "+txnStatements.size());
//			if(txnTrace.size()== 46){
//				for(SqlStatement tra:txnStatements){
//					System.out.println(tra.sql);
//				}
//			}
			int operationID = 1;
			// 检查是否处于循环结构，构建事务块
			List<TransactionBlock> txnBlocks = new ArrayList<>();
			int loopEndIndex = -1;// 最后一个循环体结束的位置，由于不支持循环嵌套，这个位置以前不可能再有循环了。
//			System.out.println("TxnStatements.size(): "+txnStatements.size() +":照理说这里应该是70");
			for (int i = 0; i < txnStatements.size(); ++i) {
				SqlStatement iterSqlStatement = txnStatements.get(i);
				// 先检查是否在当前循环中
				if (loopEndIndex != -1) {
					Multiple loopBlock = (Multiple) txnBlocks.get(loopEndIndex);
					if (iterSqlStatement.sql.equals(loopBlock.getSqls().get(0).sql)) {

						// 在，后面连续几条sql也不处理了
//						System.out.println("loopBlock.getSqls().size(): "+loopBlock.getSqls().size() +":照理说这里应该是7");
						for (int cnt = 0; cnt < loopBlock.getSqls().size(); cnt++) {
							try {
								txnTrace.get(i + cnt).operationID = loopBlock.getSqls().get(cnt).operationId;
							} catch (java.lang.IndexOutOfBoundsException e) {  //qly: 感觉是因为rollback造成的
//								System.out.println("I am IndexOutOfBoundsException");
//								System.out.println(iterSqlStatement.sql);
							}
						}
						i += loopBlock.getSqls().size() - 1;
						continue;
					}
				}

				// 检查是否构成新循环
				int j;// 遍历事务块的序号
				for (j = loopEndIndex + 1; j < txnBlocks.size(); ++j) {

					SqlStatement previousSqlStatement = (SqlStatement) txnBlocks.get(j);
					// 出现循环了
					if (previousSqlStatement.sql.equals(iterSqlStatement.sql)) {
						loopEndIndex = j;
						// 构造循环结构

						List<SqlStatement> loopSqls = new ArrayList<>();
						boolean multipleBatchExecute = false;
						for (int k = j; k < txnBlocks.size(); ++k) {
//							if(txnTrace.size()== 46){
//								System.out.println(txnBlocks.size());
//								System.out.println(k);
//							}

							loopSqls.add((SqlStatement) txnBlocks.get(k));
							try {


								txnTrace.get(i + k - j).operationID = ((SqlStatement) txnBlocks.get(k)).operationId;

							}catch (Exception e){
//								if(txnTrace.size()== 46){
								System.out.println(txnTrace);
								System.out.println(i+k-j);
								System.out.println(k);
								System.out.println(txnBlocks);
//								}
								System.out.println(iterSqlStatement.sql);
								System.out.println(e);
								System.exit(1);
							}


						}

						//modified by lyqu
//						Multiple aLoop = new Multiple(loopSqls, false);
//						Multiple aLoop = new Multiple(loopSqls,true);
						multipleBatch:for(SqlStatement stat : loopSqls){
							if(stat.getClass().getName().equals("abstraction.WriteOperation")){
								WriteOperation op = (WriteOperation) stat;
								if(op.isBatchExecute()){
									multipleBatchExecute = true;
									break multipleBatch;
								}
							}
						}
						Multiple aLoop = new Multiple(loopSqls, multipleBatchExecute);
						//------lyqu
//
						//遍历整个aLoop中的sql，查看isBatch为true，只有存在一个isBatch就吧Multiple设为true  20201022
						txnBlocks = txnBlocks.subList(0, j);
						txnBlocks.add(aLoop);
						i += loopSqls.size() - 1;
						break;
					}
				}
				// 没有循环
				if (j == txnBlocks.size()) {
					iterSqlStatement.operationId = operationID;
					txnBlocks.add(iterSqlStatement);
					txnTrace.get(i).operationID = operationID++;
				}
			}
			// 搞好了放进去
			txnId2txnTemplate.put(txnIdAndInstance.getKey(), txnBlocks);
		}

//		System.out.println("*******  "+txnId2txnTemplate.size());

		// 去除重复的事务模板
		List<Transaction> transactions = new ArrayList<>();
		// 给每个事务模板分配的事务模板号
		Map<Transaction, Integer> txnTemplate2txnTemplateID = new HashMap<>();
		int txnTempID = 1;
		// 处理一个模板
//		int qly = 0;
//		int qly2= 0;
		for (Entry<Long, List<TransactionBlock>> txnIdAndtxnTemplate : txnId2txnTemplate.entrySet()) {
			// 事务名称形如Transaction1,Transaction2...
//			System.out.println("qly: \n"+qly);
//			System.out.println("qly2: \n"+qly2);
			Transaction txn = new Transaction("Transaction" + txnTempID, 0, true, txnIdAndtxnTemplate.getValue());
			// 该模板是否已存在
//			System.out.println(txn.getName()+"\n");
			Integer myTID = txnTemplate2txnTemplateID.get(txn);
//			System.out.println(myTID);
			if (myTID == null) {
				txnTemplate2txnTemplateID.put(txn, txnTempID);
				this.txnId2txnTemplateID.put(txnIdAndtxnTemplate.getKey(), txnTempID);
				transactions.add(txn);
				txnTempID++;
//				qly2++;
			} else {
				this.txnId2txnTemplateID.put(txnIdAndtxnTemplate.getKey(), myTID);
			}
//			qly++;
		}

//		System.out.println(rollbacktxnId2txnInstance);



//		for(int i = 0; i < transactions.size();i++){
//			int size = transactions.get(i).getTransactionBlocks().size();
//			System.out.println(transactions.get(i).getTransactionBlocks());
//			System.out.println(size);
//		}



		return transactions;

	}

	/**
	 * 从Oracle客户端打的log中获取事务模板，同时获取负载轨迹
	 *
	 * @param logDir 存日志的目录
	 * @return
	 * @author Shuyan Zhang
	 */
	@SuppressWarnings("static-access")
	@Deprecated
	public List<Transaction> readOracleLogOldVersion(File logDir) {

		// 读取所有log
		// 格式：[timestamp][txnId=3][sql=……][res=col1,col2,col3][res=col1,col2,col3]...
		File[] logFiles = logDir.listFiles();

		// 文件名的格式为: 'lauca.log.xx'
		Arrays.sort(logFiles, new Comparator<File>() {
			@Override
			public int compare(File file1, File file2) {
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

		List<input.OracleLogOldVersion> oraclelogs = new ArrayList<>();
		for (int f = 0; f < logFiles.length; ++f) {
			try (BufferedReader br = new BufferedReader(
					new InputStreamReader(new FileInputStream(logFiles[f]), "utf-8"))) {
				String inputLine = null;
				while ((inputLine = br.readLine()) != null) {
					// 过滤掉空行
					if (inputLine.matches("[\\s]*")) {
						continue;
					}
					String[] log = inputLine.split("\\[|\\]");
					ArrayList<String> refineLog = new ArrayList<>();
					for (String s : log) {
						if (!s.matches("[ ]*")) {
							refineLog.add(s);
						}
					}

					// 是我们打出的OracleLog
					if (refineLog.size() > 2 && refineLog.get(1).startsWith("txnId")) {

						// 解析出操作的時間戳
						long timestamp = 0;
						Timestamp ts = new Timestamp(0);
						timestamp = ts.valueOf(refineLog.get(0).replaceAll("[/]", "-")).getTime();
						// 解析出操作所属事务号
						Long txnId = Long.parseLong(refineLog.get(1).split("=", 2)[1]);
						// 解析出sql语句
						String sql = refineLog.get(2).split("=", 2)[1];
						// 解析出返回结果集
						List<List<String>> results = new ArrayList<List<String>>();
						for (int i = 3; i < refineLog.size(); ++i) {
							List<String> result = Arrays.asList(refineLog.get(i).split("=", 2)[1].split("\\,"));
							results.add(result);

						}
						oraclelogs.add(new input.OracleLogOldVersion(timestamp, txnId, sql, results));
					}

				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

//		System.out.println(generalLogs);

		// 将事务实例提取出来 （事务号->里面一条条sql）
		Map<Long, List<String>> txnId2txnInstance = new HashMap<>();
		for (input.OracleLogOldVersion aLog : oraclelogs) {
			// 将CRUD操作加入
			if (aLog.sql != null && aLog.sql.matches(
					"(SELECT|Select|select|UPDATE|Update|update|INSERT|Insert|insert|DELETE|Delete|delete|REPLACE|Replace|replace)[\\s\\S]+")) {
				// SqlParser里再淘汰这种特殊的事务
//				if (aLog.sql.matches("(SELECT|Select|select)[\\s]+(@@)[\\s\\S]+")) {
//					continue;
//				}
				// 多个空白字符变成一个
				aLog.sql = aLog.sql.replaceAll("[\\s]+", " ");
				// 变成小写方便找关键词
				String tempSql = aLog.sql.toLowerCase();

				// 处理between谓词，col between A and B 转换成 col ≥ A and col ≤ B
				int betweenIndex;
				// 找到所有的between关键词
				while ((betweenIndex = tempSql.indexOf(" between ")) != -1) {
					// 找到col名字
					String firstHalf = aLog.sql.substring(0, betweenIndex);
					String[] subStrings = firstHalf.split(" ");
					String colName = subStrings[subStrings.length - 1];
					// 找到between后第一个and
					int andIndex = tempSql.indexOf("and", betweenIndex);
					// 在and后插入“ col ≤”
					StringBuffer sqlCopy = new StringBuffer(aLog.sql);
					sqlCopy.insert(andIndex + 3, " " + colName + " <=");

					sqlCopy.replace(betweenIndex + 1, betweenIndex + 8, ">=");
					aLog.sql = new String(sqlCopy);
					tempSql = aLog.sql.toLowerCase();
				}

				// 这是该事务第一条sql
				if (!txnId2txnInstance.containsKey(aLog.txnId)) {
					this.txnId2txnTrace.put(aLog.txnId, new ArrayList<>());
					txnId2txnInstance.put(aLog.txnId, new ArrayList<>());
				}
				txnId2txnInstance.get(aLog.txnId).add(aLog.sql);

				TraceInfo oneTrace = new TraceInfo();
				// 把操作时间戳信息加进oneTrace去
				oneTrace.operationTS = aLog.operationTS;
				oneTrace.results = aLog.results;
				this.txnId2txnTrace.get(aLog.txnId).add(oneTrace);

			} else if (aLog.sql != null && aLog.sql.equals("commit")) {
				// 暂时没啥要做
			} else if (aLog.sql != null && aLog.sql.equals("rollback")) {
				// TODO 现在的处理是，回滚的事务就不要了，不然会找到一堆没用的事务模板.对应的轨迹也直接不要了
				txnId2txnInstance.remove(aLog.txnId);
				this.txnId2txnTrace.remove(aLog.txnId);
			}
		}

		// for test
//		for (Entry<Long, List<String>> txn : txnId2txnInstance.entrySet()) {
//			System.out.println(txn.getKey());
//			for (int i = 0; i < txn.getValue().size(); ++i) {
//				System.out.println(txn.getValue().get(i));
//				System.out.println(this.txnId2txnTrace.get(txn.getKey()).get(i));
//			}
//		}

		// 提取出参数，并将sql语句中参数的位置换成？，提取事务模板
		Map<Long, List<TransactionBlock>> txnId2txnTemplate = new HashMap<>();
		SqlParser sqlParser = new SqlParser(tables);

		// 处理一个事务实例
		outer: for (Entry<Long, List<String>> txnIdAndInstance : txnId2txnInstance.entrySet()) {
			List<SqlStatement> txnStatements = new ArrayList<>();
			List<TraceInfo> txnTrace = this.txnId2txnTrace.get(txnIdAndInstance.getKey());
			// 处理每个带参数的sql
			for (int i = 0; i < txnIdAndInstance.getValue().size(); ++i) {
				String sql = txnIdAndInstance.getValue().get(i);
				List<String> sqlParas = new ArrayList<String>();
				SqlStatement currentStatement;
				if (sql.toLowerCase().startsWith("select")) {
					currentStatement = sqlParser.parseReadSqlStatement(sql, -1, sqlParas);
					if (currentStatement == null) {
						// 不是啥正经select语句,如果这个事务实例只有这一句，那么整个实例都不要了；否则就只把这一句抛弃
						if (txnIdAndInstance.getValue().size() == 1) {
							continue outer;
						} else {
							txnIdAndInstance.getValue().remove(i);
							txnTrace.remove(i);
							--i;
							continue;
						}
					}
					// 将返回项中的timestamp转为datetime
					if (txnTrace.get(txnStatements.size()).results.size() > 0) {
						int[] returnTypes = ((ReadOperation) currentStatement).getReturnDataTypes();
						// 筛选出返回项中的null值，换成0,空串或false
						for (List<String> res : txnTrace.get(txnStatements.size()).results) {
							for (int j = 0; j < res.size(); ++j) {
								if (res.get(j).equals("NULL") || res.get(j).equals("null")) {
									if (returnTypes[j] < 4) {
										res.set(j, "0");
									} else if (returnTypes[j] == 4) {
										res.set(j, "");
									} else if (returnTypes[j] == 5) {
										res.set(j, "false");
									}
								}
							}
						}
						for (int j = 0; j < returnTypes.length; ++j) {
							if (returnTypes[j] == 3) {
								Timestamp ts = new Timestamp(0);
								for (List<String> res : txnTrace.get(txnStatements.size()).results) {
									if (res.get(j).contains("-")) {
										res.set(j, "" + ts.valueOf(res.get(j)).getTime());
									}
								}
							}
						}
					}
				} else {
					currentStatement = sqlParser.parseWriteSqlStatement(sql, false, -1, sqlParas);
				}
				txnStatements.add(currentStatement);
//				txnTrace.get(txnStatements.size() - 1).parameters = new ArrayList<List<String>>();
//				txnTrace.get(txnStatements.size() - 1).parameters.add(sqlParas);
				txnTrace.get(txnStatements.size() - 1).parameters = sqlParas;
			}

			int operationID = 1;
			// 检查是否处于循环结构，构建事务块
			List<TransactionBlock> txnBlocks = new ArrayList<>();
			int loopEndIndex = -1;// 最后一个循环体结束的位置，由于不支持循环嵌套，这个位置以前不可能再有循环了。
			for (int i = 0; i < txnStatements.size(); ++i) {
				SqlStatement iterSqlStatement = txnStatements.get(i);
				// 先检查是否在当前循环中
				if (loopEndIndex != -1) {
					Multiple loopBlock = (Multiple) txnBlocks.get(loopEndIndex);
					if (iterSqlStatement.sql.equals(loopBlock.getSqls().get(0).sql)) {
						// 在，后面连续几条sql也不处理了
						for (int cnt = 0; cnt < loopBlock.getSqls().size(); cnt++) {
							try {
								txnTrace.get(i + cnt).operationID = loopBlock.getSqls().get(cnt).operationId;
							} catch (java.lang.IndexOutOfBoundsException e) {
								System.out.println(iterSqlStatement.sql);
							}
						}
						i += loopBlock.getSqls().size() - 1;
						continue;
					}
				}

				// 检查是否构成新循环
				int j;// 遍历事务块的序号
				for (j = loopEndIndex + 1; j < txnBlocks.size(); ++j) {
					SqlStatement previousSqlStatement = (SqlStatement) txnBlocks.get(j);
					// 出现循环了
					if (previousSqlStatement.sql.equals(iterSqlStatement.sql)) {
						loopEndIndex = j;
						// 构造循环结构
						List<SqlStatement> loopSqls = new ArrayList<>();
						for (int k = j; k < txnBlocks.size(); ++k) {
							loopSqls.add((SqlStatement) txnBlocks.get(k));
							txnTrace.get(i + k - j).operationID = ((SqlStatement) txnBlocks.get(k)).operationId;
						}
						Multiple aLoop = new Multiple(loopSqls, false);
						txnBlocks = txnBlocks.subList(0, j);
						txnBlocks.add(aLoop);
						i += loopSqls.size() - 1;
						break;
					}
				}
				// 没有循环
				if (j == txnBlocks.size()) {
					iterSqlStatement.operationId = operationID;
					txnBlocks.add(iterSqlStatement);
					txnTrace.get(i).operationID = operationID++;
				}
			}
			// 搞好了放进去
			txnId2txnTemplate.put(txnIdAndInstance.getKey(), txnBlocks);
		}

		// 去除重复的事务模板
		List<Transaction> transactions = new ArrayList<>();
		// 给每个事务模板分配的事务模板号
		Map<Transaction, Integer> txnTemplate2txnTemplateID = new HashMap<>();
		int txnTempID = 1;
		// 处理一个模板
		for (Entry<Long, List<TransactionBlock>> txnIdAndtxnTemplate : txnId2txnTemplate.entrySet()) {
			// 事务名称形如Transaction1,Transaction2...
			Transaction txn = new Transaction("Transaction" + txnTempID, 0, true, txnIdAndtxnTemplate.getValue());
			// 该模板是否已存在
			Integer myTID = txnTemplate2txnTemplateID.get(txn);
			if (myTID == null) {
				txnTemplate2txnTemplateID.put(txn, txnTempID);
				this.txnId2txnTemplateID.put(txnIdAndtxnTemplate.getKey(), txnTempID);
				transactions.add(txn);
				txnTempID++;
			} else {
				this.txnId2txnTemplateID.put(txnIdAndtxnTemplate.getKey(), myTID);
			}
		}

		return transactions;

	}

	/**
	 * 从Tidb的general log中获取事务模板，同时获取负载轨迹
	 *
	 * @param logFile 日志文件
	 * @return
	 * @author Shuyan Zhang
	 */
	@SuppressWarnings("static-access")
	public List<Transaction> readTidbLog(File logFile) {

		// 读取所有generalLog
		// 格式：[时间] [INFO] [session号] [GENERAL_LOG] [conn=870] [user=root@10.11.6.120]
		// [schemaVersion=788] [txnStartTS=0] [current_db=] [txn_mode=PESSIMISTIC]
		// [sql="..."]
		List<input.GeneralLog> generalLogs = new ArrayList<>();

		try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(logFile), "utf-8"))) {
			String inputLine = null;
			while ((inputLine = br.readLine()) != null) {
				// 过滤掉空行
				if (inputLine.matches("[\\s]*")) {
					continue;
				}
				String[] log = inputLine.split("\\[|\\]");
				ArrayList<String> refineLog = new ArrayList<>();
				for (String s : log) {
					if (!s.matches("[ ]*")) {
						refineLog.add(s);
					}
				}
				// 解析出操作的時間戳
				long timestamp = -1;
				Timestamp ts = new Timestamp(0);
				if (refineLog.get(0).length() > 23) {
					timestamp = ts.valueOf(refineLog.get(0).substring(0, 23).replaceAll("[/]", "-")).getTime();
				}
				// 此条是GENERAL_LOG
				Map<String, String> name2Value = new HashMap<>();
				if (refineLog.size() > 3 && refineLog.get(3).equals("GENERAL_LOG")) {
//					ArrayList<String> logItems = new ArrayList<>();
					for (int i = 4; i < refineLog.size(); ++i) {
						String[] nameAndValue = refineLog.get(i).split("=", 2);
						if (nameAndValue.length > 1) {
							name2Value.put(nameAndValue[0], nameAndValue[1]);
						} else {
							name2Value.put(nameAndValue[0], "");
						}

					}
					if (name2Value.containsKey("sql")) {
						// 把sql前后的引号去掉
						String tmpSql = name2Value.get("sql");
						if (!tmpSql.equals("commit") && !tmpSql.equals("rollback")) {
							name2Value.put("sql", tmpSql.substring(1, tmpSql.length() - 1));
						}
						generalLogs.add(new input.GeneralLog(Long.parseLong(name2Value.get("conn")), name2Value.get("user"),
								Long.parseLong(name2Value.get("schemaVersion")),
								Long.parseLong(name2Value.get("txnStartTS")), name2Value.get("current_db"),
								name2Value.get("txn_mode"), name2Value.get("sql"), null, timestamp));
					} else if (name2Value.containsKey("res")) {
						String tmpRes = log[log.length - 2];
						// 把时间前后的\"去掉
						tmpRes = tmpRes.replaceAll("(\\\\\")", "");
						String[] sRes = tmpRes.split("\\,");
						generalLogs.add(new input.GeneralLog(Long.parseLong(name2Value.get("conn")), name2Value.get("user"),
								Long.parseLong(name2Value.get("schemaVersion")),
								Long.parseLong(name2Value.get("txnStartTS")), name2Value.get("current_db"),
								name2Value.get("txn_mode"), null, Arrays.asList(sRes), timestamp));
					}

				}

			}
		} catch (IOException e) {
			e.printStackTrace();
		}

//		System.out.println(generalLogs);

		// 将事务实例提取出来
		Map<Long, List<String>> txnId2txnInstance = new HashMap<>();
		// 记录每个链接当前在处理的事务号，没有在处理事务则为-1
		Map<Long, Long> connId2txnId = new HashMap<>();
		long txnId = 1;
//		Map<Long, Map<Long, List<String>>> connId2txnStartTs2txnInstance = new HashMap<>();
		for (input.GeneralLog aLog : generalLogs) {
			// 将CRUD操作加入
			if (aLog.sql != null && aLog.sql.matches(
					"(SELECT|Select|select|UPDATE|Update|update|INSERT|Insert|insert|DELETE|Delete|delete|REPLACE|Replace|replace)[\\s\\S]+")) {
				// SqlParser里再淘汰这种特殊的事务
//				if (aLog.sql.matches("(SELECT|Select|select)[\\s]+(@@)[\\s\\S]+")) {
//					continue;
//				}
				// 多个空白字符变成一个
				aLog.sql = aLog.sql.replaceAll("[\\s]+", " ");
				// 变成小写方便找关键词
				String tempSql = aLog.sql.toLowerCase();

				// 处理between谓词，col between A and B 转换成 col ≥ A and col ≤ B
				int betweenIndex;
				// 找到所有的between关键词
				while ((betweenIndex = tempSql.indexOf(" between ")) != -1) {
					// 找到col名字
					String firstHalf = aLog.sql.substring(0, betweenIndex);
					String[] subStrings = firstHalf.split(" ");
					String colName = subStrings[subStrings.length - 1];
					// 找到between后第一个and
					int andIndex = tempSql.indexOf("and", betweenIndex);
					// 在and后插入“ col ≤”
					StringBuffer sqlCopy = new StringBuffer(aLog.sql);
					sqlCopy.insert(andIndex + 3, " " + colName + " <=");

					sqlCopy.replace(betweenIndex + 1, betweenIndex + 8, ">=");
					aLog.sql = new String(sqlCopy);
					tempSql = aLog.sql.toLowerCase();
				}

				// tidb日志中一个事务第一条sql的时间戳为0
				if (aLog.txnStartTS == 0) {
					connId2txnId.put(aLog.connId, txnId);
					this.txnId2txnTrace.put(txnId, new ArrayList<>());
					txnId2txnInstance.put(txnId, new ArrayList<>());
					txnId++;
				}
				txnId2txnInstance.get(connId2txnId.get(aLog.connId)).add(aLog.sql);

				TraceInfo oneTrace = new TraceInfo();
				// 把操作时间戳信息加进oneTrace去
				oneTrace.operationTS = aLog.operationTS;
				oneTrace.results = new ArrayList<List<String>>();
				this.txnId2txnTrace.get(connId2txnId.get(aLog.connId)).add(oneTrace);

			} else if (aLog.sql != null && aLog.sql.equals("commit")) {
				// 将连接对应事务号置为-1
				connId2txnId.put(aLog.connId, (long) -1);
			} else if (aLog.sql != null && aLog.sql.equals("rollback")) {
				// TODO 现在的处理是，回滚的事务就不要了，不然会找到一堆没用的事务模板.对应的轨迹也直接不要了
				txnId2txnInstance.remove(connId2txnId.get(aLog.connId));
				this.txnId2txnTrace.remove(connId2txnId.get(aLog.connId));
				connId2txnId.put(aLog.connId, (long) -1);
			} else if (aLog.result != null) {
				// 检查该链接是否在处理事务
				if (connId2txnId.get(aLog.connId) == null || connId2txnId.get(aLog.connId) == -1) {
					continue;
				}
				List<TraceInfo> tralist = this.txnId2txnTrace.get(connId2txnId.get(aLog.connId));
				TraceInfo oneTrace = tralist.get(tralist.size() - 1);
				// 把返回结果集加进oneTrace去
				oneTrace.results.add(aLog.result);
			}
		}

		// for test
//		for (Entry<Long, List<String>> txn : txnId2txnInstance.entrySet()) {
//			System.out.println(txn.getKey());
//			for (int i = 0; i < txn.getValue().size(); ++i) {
//				System.out.println(txn.getValue().get(i));
//				System.out.println(this.txnId2txnTrace.get(txn.getKey()).get(i));
//			}
//		}

		// 提取出参数，并将sql语句中参数的位置换成？，提取事务模板
		Map<Long, List<TransactionBlock>> txnId2txnTemplate = new HashMap<>();
		SqlParser sqlParser = new SqlParser(tables);

		// 处理一个事务实例
		outer: for (Entry<Long, List<String>> txnIdAndInstance : txnId2txnInstance.entrySet()) {
			List<SqlStatement> txnStatements = new ArrayList<>();
			List<TraceInfo> txnTrace = this.txnId2txnTrace.get(txnIdAndInstance.getKey());
			// 处理每个带参数的sql
			for (int i = 0; i < txnIdAndInstance.getValue().size(); ++i) {
				String sql = txnIdAndInstance.getValue().get(i);
				List<String> sqlParas = new ArrayList<String>();
				SqlStatement currentStatement;
				if (sql.toLowerCase().startsWith("select")) {
					currentStatement = sqlParser.parseReadSqlStatement(sql, -1, sqlParas);
					if (currentStatement == null) {
						// 不是啥正经select语句,如果这个事务实例只有这一句，那么整个实例都不要了；否则就只把这一句抛弃
						if (txnIdAndInstance.getValue().size() == 1) {
							continue outer;
						} else {
							txnIdAndInstance.getValue().remove(i);
							txnTrace.remove(i);
							--i;
							continue;
						}
					}
					// 将返回项中的timestamp转为datetime
					if (txnTrace.get(txnStatements.size()).results.size() > 0) {
						int[] returnTypes = ((ReadOperation) currentStatement).getReturnDataTypes();
						// 筛选出返回项中的null值，换成0,空串或false
						for (List<String> res : txnTrace.get(txnStatements.size()).results) {
							for (int j = 0; j < res.size(); ++j) {
								if (res.get(j).equals("NULL") || res.get(j).equals("null")) {
									if (returnTypes[j] < 4) {
										res.set(j, "0");
									} else if (returnTypes[j] == 4) {
										res.set(j, "");
									} else if (returnTypes[j] == 5) {
										res.set(j, "false");
									}
								}
							}
						}
						for (int j = 0; j < returnTypes.length; ++j) {
							if (returnTypes[j] == 3) {
								Timestamp ts = new Timestamp(0);
								for (List<String> res : txnTrace.get(txnStatements.size()).results) {
									if (res.get(j).contains("-")) {
										res.set(j, "" + ts.valueOf(res.get(j)).getTime());
									}
								}
							}
						}
					}
				} else {
					currentStatement = sqlParser.parseWriteSqlStatement(sql, false, -1, sqlParas);
				}
				txnStatements.add(currentStatement);
//				txnTrace.get(txnStatements.size() - 1).parameters = new ArrayList<List<String>>();
//				txnTrace.get(txnStatements.size() - 1).parameters.add(sqlParas);
				txnTrace.get(txnStatements.size() - 1).parameters = sqlParas;
			}

			int operationID = 1;
			// 检查是否处于循环结构，构建事务块
			List<TransactionBlock> txnBlocks = new ArrayList<>();
			int loopEndIndex = -1;// 最后一个循环体结束的位置，由于不支持循环嵌套，这个位置以前不可能再有循环了。
			for (int i = 0; i < txnStatements.size(); ++i) {
				SqlStatement iterSqlStatement = txnStatements.get(i);
				// 先检查是否在当前循环中
				if (loopEndIndex != -1) {
					Multiple loopBlock = (Multiple) txnBlocks.get(loopEndIndex);
					if (iterSqlStatement.sql.equals(loopBlock.getSqls().get(0).sql)) {
						// 在，后面连续几条sql也不处理了
						for (int cnt = 0; cnt < loopBlock.getSqls().size(); cnt++) {
							try {
								txnTrace.get(i + cnt).operationID = loopBlock.getSqls().get(cnt).operationId;
							} catch (java.lang.IndexOutOfBoundsException e) {
								System.out.println(iterSqlStatement.sql);
							}
						}
						i += loopBlock.getSqls().size() - 1;
						continue;
					}
				}

				// 检查是否构成新循环
				int j;// 遍历事务块的序号
				for (j = loopEndIndex + 1; j < txnBlocks.size(); ++j) {
					SqlStatement previousSqlStatement = (SqlStatement) txnBlocks.get(j);
					// 出现循环了
					if (previousSqlStatement.sql.equals(iterSqlStatement.sql)) {
						loopEndIndex = j;
						// 构造循环结构
						ArrayList<SqlStatement> loopSqls = new ArrayList<>();
						for (int k = j; k < txnBlocks.size(); ++k) {
							loopSqls.add((SqlStatement) txnBlocks.get(k));
							txnTrace.get(i + k - j).operationID = ((SqlStatement) txnBlocks.get(k)).operationId;
						}
						Multiple aLoop = new Multiple(loopSqls, false);
						txnBlocks = txnBlocks.subList(0, j);
						txnBlocks.add(aLoop);
						i += loopSqls.size() - 1;
						break;
					}
				}
				// 没有循环
				if (j == txnBlocks.size()) {
					iterSqlStatement.operationId = operationID;
					txnBlocks.add(iterSqlStatement);
					txnTrace.get(i).operationID = operationID++;
				}
			}
			// 搞好了放进去
			txnId2txnTemplate.put(txnIdAndInstance.getKey(), txnBlocks);
		}

		// 去除重复的事务模板
		List<Transaction> transactions = new ArrayList<>();
		// 给每个事务模板分配的事务模板号
		Map<Transaction, Integer> txnTemplate2txnTemplateID = new HashMap<>();
		int txnTempID = 1;
		// 处理一个模板
		for (Entry<Long, List<TransactionBlock>> txnIdAndtxnTemplate : txnId2txnTemplate.entrySet()) {
			// 事务名称形如Transaction1,Transaction2...
			Transaction txn = new Transaction("Transaction" + txnTempID, 0, true, txnIdAndtxnTemplate.getValue());
			// 该模板是否已存在
			Integer myTID = txnTemplate2txnTemplateID.get(txn);
			if (myTID == null) {
				txnTemplate2txnTemplateID.put(txn, txnTempID);
				this.txnId2txnTemplateID.put(txnIdAndtxnTemplate.getKey(), txnTempID);
				transactions.add(txn);
				txnTempID++;
			} else {
				this.txnId2txnTemplateID.put(txnIdAndtxnTemplate.getKey(), myTID);
			}
		}

		return transactions;
	}

	/**
	 * 从现成的事务模板文件中获取事务模板
	 *
	 * @param workloadFile 事务模板文件
	 * @return
	 */
	public List<Transaction> read(File workloadFile) {
		List<String> inputLines = new ArrayList<>();
		try (BufferedReader br = new BufferedReader(
				new InputStreamReader(new FileInputStream(workloadFile), "utf-8"))) {
			String inputLine = null;
			while ((inputLine = br.readLine()) != null) {
				// 过滤掉空行和注释行
				if (inputLine.matches("[\\s]*") || inputLine.matches("[ ]*##[\\s\\S]*")) {
					continue;
				}
				inputLines.add(inputLine);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		//读取匿名化的中间状态文件，得到原表名、列名与匿名的对应

		AnonymityInfoSerializer ais = new AnonymityInfoSerializer();
		File infile = new File(Configurations.getAnonymitySaveFile());
		Anonymity anonymity = new Anonymity();
//		anonymity = ais.read(infile);
		if(Configurations.isEnableAnonymity()){
			anonymity = ais.read(infile);
			System.out.println(anonymity);
		}



		SqlParser sqlParser = new SqlParser(tables);
		List<Transaction> transactions = new ArrayList<>();

		for (int i = 0; i < inputLines.size(); i++) {
			if (inputLines.get(i).matches("[ \\t]*(TX|tx|Tx)[ \\t]*\\[[\\s\\S^\\]]+\\][ \\t]*")) {
				String[] arr = inputLines.get(i)
						.substring(inputLines.get(i).indexOf('[') + 1, inputLines.get(i).lastIndexOf(']')).trim()
						.split(",");
				String name = arr[0].trim(); // 事务名称
				// 当前事务在负载中的比例，目前ratio的设置值好像也没有什么用~
				double ratio = arr[1].trim().equals("?") ? 0 : Double.parseDouble(arr[1].trim());
				boolean prepared = Boolean.parseBoolean(arr[2].trim()); // 事务是否预编译执行
				ArrayList<TransactionBlock> transactionBlocks = new ArrayList<>();

				int operationId = 1;
				int j = i + 1;
				String sql = null;
				for (; j < inputLines.size() && !inputLines.get(j).trim().toLowerCase().equals("endtx"); j++) {
					// 对于SQL操作的首关键字目前仅支持三种大小写形式：全大写、首字母大写、全小写
					sql = inputLines.get(j).trim();
					if(Configurations.isEnableAnonymity()){
						sql = anonymity.sql2Anonymity(inputLines.get(j).trim());

						System.out.println(sql);
					}

					if (sql.matches("[ \\t]*(SELECT|Select|select)[\\s\\S]+")) {
						transactionBlocks.add(
								sqlParser.parseReadSqlTemplate(sql, operationId++, new ArrayList<>()));
					} else if (inputLines.get(j).matches("[ \\t]*(UPDATE|Update|update|INSERT|"
							+ "Insert|insert|DELETE|Delete|delete|REPLACE|Replace|replace)[\\s\\S]+")) {
						transactionBlocks.add(sqlParser.parseWriteSqlTemplate(sql, false, operationId++,
								new ArrayList<>()));
					} else if (inputLines.get(j).matches("[ \\t]*(MULTIPLE|Multiple|multiple)[\\s\\S]*")) {
						int index1 = sql.indexOf('[');
						int index2 = sql.indexOf(']');
						boolean batchExecute = false;
						if (index1 != -1 && index2 != -1 && inputLines.get(j).
								substring(index1 + 1, index2).trim().toLowerCase().equals("batch")) {
							batchExecute = true;
						}

						// sqls：multiple内的多个SQL操作
						List<SqlStatement> sqls = new ArrayList<>();
						int k = j + 1;
						for (; !inputLines.get(k).trim().toLowerCase().equals("endmultiple"); k++) {

							sql = inputLines.get(k).trim();
							if(Configurations.isEnableAnonymity()){
								sql = anonymity.sql2Anonymity(inputLines.get(k).trim());
								System.out.println(sql);
							}



							if (inputLines.get(k).matches("[ \\t]*(SELECT|Select|select)[\\s\\S]+")) {
//								System.out.println("22222222222222");
								sqls.add(sqlParser.parseReadSqlTemplate(sql, operationId++,
										new ArrayList<>()));
							} else if (inputLines.get(k).matches("[ \\t]*(UPDATE|Update|update|INSERT|"
									+ "Insert|insert|DELETE|Delete|delete|REPLACE|Replace|replace)[\\s\\S]+")) {
								sqls.add(sqlParser.parseWriteSqlTemplate(sql, batchExecute,
										operationId++, new ArrayList<>()));
							}
						}
						j = k;
						transactionBlocks.add(new Multiple(sqls, batchExecute));
					} else if (inputLines.get(j).matches("[ \\t]*(BRANCH|Branch|branch)[ \\t]*")) {
						List<List<SqlStatement>> branches = new ArrayList<>();
						branches.add(new ArrayList<>());
						int k = j + 1;
						for (; !inputLines.get(k).trim().toLowerCase().equals("endbranch"); k++) {
							if (inputLines.get(k).trim().toLowerCase().equals("separator")) {
								branches.add(new ArrayList<>());
							} else {
								sql = inputLines.get(k).trim();
								if(Configurations.isEnableAnonymity()){
									sql = anonymity.sql2Anonymity(inputLines.get(k).trim());
									System.out.println(sql);
								}


								if (inputLines.get(k).matches("[ \\t]*(SELECT|Select|select)[\\s\\S]+")) {
//									System.out.println("3333333333333");
									branches.get(branches.size() - 1).add(sqlParser.parseReadSqlTemplate(
											sql, operationId++, new ArrayList<>()));
								} else if (inputLines.get(k).matches("[ \\t]*(UPDATE|Update|update|INSERT|"
										+ "Insert|insert|DELETE|Delete|delete|REPLACE|Replace|replace)[\\s\\S]+")) {
									branches.get(branches.size() - 1).add(sqlParser.parseWriteSqlTemplate(
											sql, false, operationId++, new ArrayList<>()));
								}
							}
						}
						j = k;
						transactionBlocks.add(new Branch(branches));
					} else {
						logger.error("Unrecognized operation: \n\t" + inputLines.get(j));
					}
				} // for -- transaction
				i = j;

				// logger.debug(new Transaction(name, ratio, prepared, transactionBlocks));
				transactions.add(new Transaction(name, ratio, prepared, transactionBlocks));
			}
		}
		return transactions;
	}

}

/**
 * 对应Oracle客户端打出的log。每条log存一句sql。如果有返回值，将返回值一并存入。
 *
 * @author Shuyan Zhang
 *
 */
class OracleLog {
	long operationTS;
	long connId;
	/** 可能是完整sql语句，也可能是sql模板 */
	String sql;
	/** 预编译的sql需要参数，如果不是预编译的，则该字段为null */
	List<String> parameters = null;
	/** 如果是select语句，存储该语句的返回结果集 */
	List<List<String>> results = null;
	/** 是否批处理 */
	boolean isBatched = false;

	public OracleLog(long operationTS, long connId, String sql, List<String> parameters, List<List<String>> results) {
		this.operationTS = operationTS;
		this.connId = connId;
		this.sql = sql;
		this.parameters = parameters;
		this.results = results;
	}

	public OracleLog() {

	}

	public void setBatched(boolean isBatched) {
		this.isBatched = isBatched;
	}

	public boolean getBatched(){
		return this.isBatched;
	}

	@Override
	public String toString() {
		return "OracleLog [operationTS=" + operationTS + ", connId=" + connId + ", sql=" + sql + ", parameters="
				+ parameters + ", results=" + results + "]";
	}

}

/**
 * 对应Oracle客户端打出的log。每 public OracleLog() { super();
 * }条log存一句sql。如果有返回值，将返回值一并存入。
 *
 * @author Shuyan Zhang
 *
 */
@Deprecated
class OracleLogOldVersion {
	long operationTS;
	long txnId;
	String sql;
	List<List<String>> results;

	public OracleLogOldVersion(long operationTS, long txnId, String sql, List<List<String>> results) {
		super();
		this.operationTS = operationTS;
		this.txnId = txnId;
		this.sql = sql;
		this.results = results;
	}

	@Override
	public String toString() {
		return "OracleLogOldVersion [operationTS=" + operationTS + ", txnId=" + txnId + ", sql=" + sql + ", results="
				+ results + "]";
	}

}

/**
 * Tidb的GeneralLog。每条log存一句sql或一行返回值。多行返回值会对应多条log
 *
 * @author Shuyan Zhang
 *
 */
class GeneralLog {
	long connId;
	String user;
	long schemaVersion;
	long txnStartTS;
	String current_db;
	String txn_mode;
	String sql;
	List<String> result;
	long operationTS;

	public GeneralLog(long connId, String user, long schemaVersion, long txnStartTs, String current_db, String txn_mode,
					  String sql, List<String> result, long operationTS) {
		this.connId = connId;
		this.user = user;
		this.schemaVersion = schemaVersion;
		this.txnStartTS = txnStartTs;
		this.current_db = current_db;
		this.txn_mode = txn_mode;
		this.sql = sql;
		this.result = result;
		this.operationTS = operationTS;
	}

	@Override
	public String toString() {
		return "GeneralLog [connId:" + connId + ", user:" + user + ", schemaVersion:" + schemaVersion + ", txnStartTS:"
				+ txnStartTS + ", current_db:" + current_db + ", txn_mode:" + txn_mode + ", sql:" + sql + ", res:"
				+ result + "]\n";
	}
}
