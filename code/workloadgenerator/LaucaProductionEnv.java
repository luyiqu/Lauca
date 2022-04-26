package workloadgenerator;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.*;

import abstraction.*;
import accessdistribution.*;
import input.*;
import org.apache.log4j.PropertyConfigurator;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import config.Configurations;
import config.ConfigurationsReader;
import abstraction.SqlStatement;
import serializable.DataAccessDistributionAdapter;
import serializable.DistributionCounter4Serial;
import serializable.SequentialParaDistributionAdapter;
import serializable.SqlStatementAdapter;
import serializable.TransactionBlockAdapter;
import transactionlogic.OperationData;
import transactionlogic.Preprocessor;
import transactionlogic.RunningLogReader;

public class LaucaProductionEnv {

	public static void main(String[] args) {

		if (args.length < 2) {
			System.out.println("请指定配置文件以及需执行的操作...");
			return;
		}

		// 读取配置文件
		ConfigurationsReader.read(new File(args[0]));

		LaucaProductionEnv lauca = new LaucaProductionEnv();
		PropertyConfigurator.configure(Configurations.getLog4jConfigFile());
		// 然后根据运行参数，调用相应的功能模块
		for (int i = 1; i < args.length; i++) {
			switch (args[i].trim()) {
				case "--getDataCharacteristics":
					lauca.getDataCharacteristics();
					break;
				case "--getWorkloadCharacteristics":
					lauca.getWorkloadCharacteristics();
					break;
				default:
					System.out.println("无法识别的参数！ " + args[i].trim());
			}
		}

	}

	private void getDataCharacteristics() {
		//读取表模式
		//SchemaReader schemaReader = new SchemaReader();
		//List<Table> tables = schemaReader.read(new File(Configurations.getDatabaseSchemaFile()));
		List<Table> tables = new ArrayList<>();
		//连接真实数据库，自动统计 数据表大小、外键扩展因子、属性的基本数据特征 这些信息
		DBStatisticsCollector collector = new DBStatisticsCollector(Configurations.getOriginalDatabaseIp(),
				Configurations.getOriginalDatabasePort(), Configurations.getOriginalDatabaseName(),
				Configurations.getOriginalDatabaseUserName(), Configurations.getOriginalDatabasePasswd(), tables);
		collector.run();
		tables = collector.getTables();

		// 对于每张表：初始化支持键值属性生成的相关数据结构
		for (int i = 0; i < tables.size(); i++) {
			tables.get(i).init(tables);
		}
		//System.out.println(tables);

		Anonymity anonymity = new Anonymity();
		//是否需要匿名化
		if(Configurations.isEnableAnonymity()){
			tables = anonymity.Schema2Anonymity(tables);
			//保存匿名化的对应文件，用于后续事务模板的匿名化
			AnonymityInfoSerializer ais = new AnonymityInfoSerializer();
			File outfile = new File(Configurations.getAnonymitySaveFile());
			if(outfile.exists()) {
				outfile.delete();
			}
			try {
				outfile.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
			ais.write(anonymity,outfile);
			System.out.println(anonymity);
		}

		//保存数据特征的中间状态文件.//testdata//dataCharacteristicSaveFile.obj
		TableInfoSerializer serializer = new TableInfoSerializer();
		File file = new File(Configurations.getDataCharacteristicSaveFile());
		if (file.exists()) {
			file.delete();
		}
		try {
			file.createNewFile();
		} catch (IOException e) {
			e.printStackTrace();
		}
		serializer.write(tables, file);
		//System.out.println(tables);
	}

	private void getWorkloadCharacteristics() {
		TableInfoSerializer serializer = new TableInfoSerializer();
		List<Table> tables = serializer.read(new File(Configurations.getDataCharacteristicSaveFile()));

		List<Transaction> transactions = AccessDistributionAnalyzer.startRun(tables);

//		Map<String,Integer> rollbackTxname2BlockId2Count = new HashMap<>();  //added by lyqu 支持主动回滚
		if(Configurations.isEnableRollbackProbability()){
			Map<String,Integer> rollbackTxname2BlockId2Count = new HashMap<>();
			rollbacktxnIdLoop:for(Long rollbacktxnId : WorkloadReader.rollbacktxnId2txnInstance.keySet()){
				List<String> txnInstance = WorkloadReader.rollbacktxnId2txnInstance.get(rollbacktxnId);
//			System.out.println(txnInstance);
				if (txnInstance==null){
					continue rollbacktxnIdLoop;
				}
				transactionLoop:for(Transaction transaction : transactions){
					int txnInstanceIndex = 0;
					for(int i = 0;i < transaction.getTransactionBlocks().size();i++){
						TransactionBlock txBlock = transaction.getTransactionBlocks().get(i);
						if(txBlock.getClass().getSimpleName().equals("Multiple")){
//						System.out.println("Multiple");
							Multiple multiple = (Multiple) txBlock;
							//判断multiple段只有一次
							int operationId = multiple.getSqls().get(0).getOperationId();

//						System.out.println(transaction.getOperationId2AvgRunTimes().get(operationId));
							double decimalPart = transaction.getOperationId2AvgRunTimes().get(operationId)% 1;
							int runTimes = new Double(transaction.getOperationId2AvgRunTimes().get(operationId)).intValue();
							if (Math.random() < decimalPart) {
								runTimes += 1;
							}
//						System.out.println(runTimes);
							for(int multipleTime = 0; multipleTime < runTimes;multipleTime++){
//							System.out.println(multiple.getSqls());
								for (SqlStatement sql : multiple.getSqls()) {
//								System.out.println(sql.sql);
//								System.out.println(txnInstance.get(txnInstanceIndex));
									if(sql.sql.equals(txnInstance.get(txnInstanceIndex))){
										if(txnInstanceIndex == txnInstance.size()-1 ){
											//todo 放入map中对次数累加1
											String identifier = transaction.getName()+"_"+i;
											if(!rollbackTxname2BlockId2Count.containsKey(identifier)){
												rollbackTxname2BlockId2Count.put(identifier,1);
											}else{
												rollbackTxname2BlockId2Count.put(identifier,rollbackTxname2BlockId2Count.get(identifier)+1);
											}
											continue rollbacktxnIdLoop;
										}
										txnInstanceIndex += 1;

									}else{
										continue transactionLoop;
									}

								}
							}
						}else{
							SqlStatement tx = (SqlStatement)txBlock;
//						System.out.println(txnInstance);
//						System.out.println(txnInstanceIndex);
							if(tx.sql.equals(txnInstance.get(txnInstanceIndex))){
								if(txnInstanceIndex == txnInstance.size()-1){
									//todo 放入map中对次数累加1
									String identifier = transaction.getName()+"_"+i;
									if(!rollbackTxname2BlockId2Count.containsKey(identifier)){
										rollbackTxname2BlockId2Count.put(identifier,1);
									}else{
										rollbackTxname2BlockId2Count.put(identifier,rollbackTxname2BlockId2Count.get(identifier)+1);
									}
									continue rollbacktxnIdLoop;
								}
								txnInstanceIndex += 1;

							}else{
								continue transactionLoop;
							}
						}
					}
				}
			}
			//todo 分解rollbackTxname block是从0开始的

			for (int i = 0; i < transactions.size(); i++) {
				int size = transactions.get(i).getTransactionBlocks().size();
				double[] rollbackProbabilities = new double[size];
				Arrays.fill(rollbackProbabilities, 0);
				transactions.get(i).setRollbackProbabilities(rollbackProbabilities);
			}
//
//			System.out.println(rollbackTxname2BlockId2Count);
//			System.out.println("*****");

			for(String identifier : rollbackTxname2BlockId2Count.keySet()){
				System.out.println(identifier);
				String txName = identifier.substring(0,identifier.indexOf('_'));
				int blockId = Integer.parseInt(identifier.substring(identifier.lastIndexOf('_')+1));
				for(Transaction transaction : transactions){
					if(transaction.getName().equals(txName)){
						int size = transaction.getTransactionBlocks().size();
						double[] rollbackProbabilities = transaction.getRollbackProbabilities();
						rollbackProbabilities[blockId] += (double)rollbackTxname2BlockId2Count.get(identifier)/RunningLogReader.txName2AllAmount.get(txName);
						transaction.setRollbackProbabilities(rollbackProbabilities);
//					System.out.println(txName);
//					System.out.println(rollbackProbabilities[blockId]);
//					System.out.println(rollbackTxname2BlockId2Count);
//						System.out.println(RunningLogReader.txName2AllAmount.get(txName));
					}

				}
			}
		}else{
			for (int i = 0; i < transactions.size(); i++) {
				int size = transactions.get(i).getTransactionBlocks().size();
				double[] rollbackProbabilities = new double[size];
				Arrays.fill(rollbackProbabilities, 0);
				transactions.get(i).setRollbackProbabilities(rollbackProbabilities);
			}
		}

//		System.out.println(rollbackTxname2BlockId2Count);

//



/** 旧版本
		// bug fix for smallbank workload
		// 加上针对 主动回滚 的支持
		if (Configurations.isEnableRollbackProbability()) {
			// 这里假设事务模板中不存在multiple和branch结构
			for (int i = 0; i < transactions.size(); i++) {
				Map<Integer, Double> operationId2AvgRunTimes = transactions.get(i).getOperationId2AvgRunTimes();
				int size = transactions.get(i).getTransactionBlocks().size();
				double[] rollbackProbabilities = new double[size];
				for (int j = 0; j < size; j++) {
					if (j == size - 1) {
						rollbackProbabilities[j] = 0; // 最后一个操作通过运行次数看不出其回滚概率，这里就假设都不回滚了
					} else {
						// 操作id是从1开始计数的
						rollbackProbabilities[j] = (operationId2AvgRunTimes.get(j + 1)
								- operationId2AvgRunTimes.get(j + 2)) / operationId2AvgRunTimes.get(j + 1);
					}
				}
//				System.out.println(transactions.get(i).getName());
//				System.out.println("rollbackProbabilities:" + Arrays.toString(rollbackProbabilities));
				transactions.get(i).setRollbackProbabilities(rollbackProbabilities);
			}
		} else {
			for (int i = 0; i < transactions.size(); i++) {
				int size = transactions.get(i).getTransactionBlocks().size();
				double[] rollbackProbabilities = new double[size];
				Arrays.fill(rollbackProbabilities, 0);
				transactions.get(i).setRollbackProbabilities(rollbackProbabilities);
			}
		}
*/
		if(Configurations.getFakeColumnRate() != 0){
			 //添加一些无实际意义的column列
			for (Table tal : tables) {
				tal.modifyColumns();
			}
			//		重新init一下table
			for (int i = 0; i < tables.size(); i++) {
				tables.get(i).init(tables);
			}
			List<Table> originTables = serializer.read(new File(Configurations.getDataCharacteristicSaveFile()));
			//将新增column加入transaction中进行序列化~
			//存入增加column的信息
			File file = new File(Configurations.getDataCharacteristicSaveFile());
			if (file.exists()) {
				file.delete();
			}
			try {
				file.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}

			serializer.write(tables, file);

			//added by lyqu 新增column的访问分布，有点问题，先删掉
			FakeTransactionDistribution fakeTransactionDistribution = new FakeTransactionDistribution(transactions,originTables,tables);
			fakeTransactionDistribution.addFakeColumnDistributionAccess();
			transactions = fakeTransactionDistribution.getTransactions();


		}

		//DistributionCounter4Serial中加入新增column的访问分布~
		DistributionCounter4Serial dcs = new DistributionCounter4Serial(transactions);


		// 序列化
		try {
			File file = new File(Configurations.getTxLogicSaveFile());
			if (file.exists()) {
				file.delete();
			}
			file.createNewFile();
			OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(file), "UTF-8");
			Gson gson = new GsonBuilder().registerTypeAdapter(TransactionBlock.class, new TransactionBlockAdapter())
					.registerTypeAdapter(SqlStatement.class, new SqlStatementAdapter())
					.registerTypeAdapter(DataAccessDistribution.class, new DataAccessDistributionAdapter())
					.registerTypeAdapter(SequentialParaDistribution.class, new SequentialParaDistributionAdapter())
					.create();

			String s = gson.toJson(transactions);
			osw.write(s);
			osw.flush();
			osw.close();


			file = new File(Configurations.getDistributionSaveFile());
			if (file.exists()) {
				file.delete();
			}
			file.createNewFile();
			osw = new OutputStreamWriter(new FileOutputStream(file), "UTF-8");
			s = gson.toJson(dcs);
			osw.write(s);
			osw.flush();
			osw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}




	}

}


