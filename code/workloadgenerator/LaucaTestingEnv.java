package workloadgenerator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import abstraction.*;
import com.mysql.jdbc.log.Log;
import input.DdlAutoReaderWOA;
import org.apache.log4j.PropertyConfigurator;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import config.Configurations;
import input.DdlAutoReader;
import accessdistribution.DataAccessDistribution;
import accessdistribution.DistributionCounter;
import accessdistribution.SequentialParaDistribution;
import config.Configurations;
import config.ConfigurationsReader;
import datageneration.DataGenerator;
import input.TableInfoSerializer;
import serializable.DataAccessDistributionAdapter;
import serializable.DistributionCounter4Serial;
import serializable.SequentialParaDistributionAdapter;
import serializable.SqlStatementAdapter;
import serializable.TransactionBlockAdapter;
import util.DBConnector;

public class LaucaTestingEnv {
//	public static long geneTime;
//	public static long updateTime;
	public static long batchExecuteTime;
//	//added by lyqu for debug
//	public static AtomicInteger updateRowCount = new AtomicInteger(0);
//	public static AtomicInteger multipleUpdateRowCount = new AtomicInteger(0);
//	public static AtomicInteger noUpdateRowCount = new AtomicInteger(0);
//	public static AtomicInteger moreUpdateRowCount = new AtomicInteger(0);
//	public static AtomicInteger multipleNoUpdateRowCount = new AtomicInteger(0);

	//---
	public static void main(String[] args) {

		if (args.length < 2) {
			System.out.println("请指定配置文件以及需执行的操作...");
			return;
		}

		// 读取配置文件
		ConfigurationsReader.read(new File(args[0]));

		LaucaTestingEnv lauca = new LaucaTestingEnv();
		PropertyConfigurator.configure(Configurations.getLog4jConfigFile());

		// 然后根据运行参数，调用相应的功能模块
		for (int i = 1; i < args.length; i++) {
			switch (args[i].trim()) {
				case "--geneSyntheticDatabase":
					lauca.geneSyntheticDatabase();
					break;
				case "--loadSyntheticDatabase":
					lauca.loadSyntheticDatabase();
					break;
				case "--geneSyntheticWorkload":
					lauca.geneSyntheticWorkload();
					break;
				default:
					System.out.println("无法识别的参数！ " + args[i].trim());
			}
		}

	}
	//生成测试数据，模拟数据库的原始数据（lauca生成的）放于指定laucaTablesDir路径
	private void geneSyntheticDatabase() {
		TableInfoSerializer serializer = new TableInfoSerializer();
		List<Table> tables = serializer.read(new File(Configurations.getDataCharacteristicSaveFile()));
		File file = new File(Configurations.getLaucaTablesDir());
		if (!file.exists()) {
			file.mkdir();
		}
		DataGenerator dataGenerator = new DataGenerator(Configurations.getMachineId(), Configurations.getMachineNum(),
				Configurations.getSingleMachineThreadNum(), Configurations.getLaucaTablesDir(), tables);
		dataGenerator.setUp();
	}
	//将测试数据加载到测试数据库
	private void loadSyntheticDatabase() {
		DBConnector oriDBConnector = new DBConnector(Configurations.getOriginalDatabaseIp(),
				Configurations.getOriginalDatabasePort(),Configurations.getOriginalDatabaseName(),
				Configurations.getOriginalDatabaseUserName(),Configurations.getOriginalDatabasePasswd());
		DBConnector laucaDBConnector = new DBConnector(Configurations.getLaucaDatabaseIp(),
				Configurations.getLaucaDatabasePort(), Configurations.getLaucaDatabaseName(),
				Configurations.getLaucaDatabaseUserName(), Configurations.getLaucaDatabasePasswd());
		Connection oriConn = null,laucaConn = null;
		String databaseType = Configurations.getDatabaseType().toLowerCase();


		DdlAutoReader ddlAutoReader;
		DdlAutoReaderWOA ddlAutoReaderWOA;
		//先建表语句，再压数据，最后加上外键、索引等约束
		ArrayList<String> FKs_Indexes = new ArrayList<String>();
		switch (databaseType) {
			case "mysql":
			case "tidb":
				oriConn = oriDBConnector.getMySQLConnection();
				laucaConn = laucaDBConnector.getMySQLConnection();
				if (Configurations.isEnableAnonymity()) {
					ddlAutoReader = new DdlAutoReader(Configurations.getOriginalDatabaseUserName(), oriConn, laucaConn);
					FKs_Indexes = ddlAutoReader.createTables4Mysql();
				} else {
					ddlAutoReaderWOA = new DdlAutoReaderWOA(Configurations.getOriginalDatabaseUserName(), oriConn, laucaConn);
					FKs_Indexes = ddlAutoReaderWOA.createTables4Mysql();
				}
				break;
			case "postgresql":
				oriConn = oriDBConnector.getPostgreSQLConnection();
				laucaConn = laucaDBConnector.getPostgreSQLConnection();
				if (Configurations.isEnableAnonymity()) {
					ddlAutoReader = new DdlAutoReader(Configurations.getOriginalDatabaseUserName(), oriConn, laucaConn);
					FKs_Indexes = ddlAutoReader.createTables4Postgres();
				} else {
					ddlAutoReaderWOA = new DdlAutoReaderWOA(Configurations.getOriginalDatabaseUserName(), oriConn, laucaConn);
					FKs_Indexes = ddlAutoReaderWOA.createTables4Postgres();
				}

				break;
			case "oracle":
				oriConn = oriDBConnector.getOracleConnection();
				laucaConn = laucaDBConnector.getOracleConnection();
				if (Configurations.isEnableAnonymity()) {
					ddlAutoReader = new DdlAutoReader(Configurations.getOriginalDatabaseUserName(), oriConn, laucaConn);
					FKs_Indexes = ddlAutoReader.createTables4Oracle();
				} else {
					ddlAutoReaderWOA = new DdlAutoReaderWOA(Configurations.getOriginalDatabaseUserName(), oriConn, laucaConn);
					FKs_Indexes = ddlAutoReaderWOA.createTables4Oracle();
				}
				break;
		}

		try {
			Statement stmt = laucaConn.createStatement();
            //将Table中的数据全部导入数据库中
			File laucaTablesDir = new File(Configurations.getLaucaTablesDir());
			File[] tableDataFiles = laucaTablesDir.listFiles();
			try {
				CountDownLatch countDownLatch = new CountDownLatch(tableDataFiles.length);
				long startTime = System.currentTimeMillis();

				for (File tableDataFile : tableDataFiles) {
					Connection finalLaucaConn = laucaConn;
					new Thread(() -> {
						String tableName = tableDataFile.getName().substring(0, tableDataFile.getName().length() - 6);
						switch (databaseType) {
							case "mysql":
							case "tidb":
								try {
									System.out.println(tableDataFile.getCanonicalPath());
									stmt.execute("LOAD DATA  INFILE '" + tableDataFile.getCanonicalPath() + "' INTO TABLE "
											+ tableName + " FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n'");
								} catch (SQLException | IOException e) {
									throw new RuntimeException(e);
								}
								break;
							case "postgresql":
								CopyManager copyManager = null;
								try {
									copyManager = new CopyManager((BaseConnection) finalLaucaConn);
								} catch (SQLException e) {
									throw new RuntimeException(e);
								}
								try {
									copyManager.copyIn("COPY " + tableName + " FROM stdin DELIMITER as ',';", new FileReader(new File(
											String.valueOf(tableDataFile.getCanonicalPath()))));
								} catch (SQLException | IOException e) {
									throw new RuntimeException(e);
								}
								break;
							case "oracle":
								File ctldir = new File(".//testdata//ctl");
								if (!ctldir.exists()) {
									ctldir.mkdirs();
								}
								TableInfoSerializer serializer = new TableInfoSerializer();
								List<Table> tables = serializer.read(new File(Configurations.getDataCharacteristicSaveFile()));
								Map<String, Table> name2table = new HashMap<String, Table>();
								for (Table table : tables) {
									name2table.put(table.getName(), table);
								}
								try {
									LaucaTestingEnv.oracleLoader(tableDataFile, name2table.get(tableName));
								} catch (IOException e) {
									throw new RuntimeException(e);
								}
								break;
						};

						System.out.println(tableDataFile.getName()+" loaded at" +( System.currentTimeMillis() - startTime));
						countDownLatch.countDown();
					}).start();

				}
				countDownLatch.await();
			}
			catch (Exception e){
				e.printStackTrace();
			}

			System.out.println("数据导入成功！");
			//建外键，UNIQUE，索引等
			stmt.execute("SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0");
			stmt.execute("SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0");
			for(String cons : FKs_Indexes){
				//System.out.println(cons);
				stmt.execute(cons);
			}
			stmt.execute("SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS");
            stmt.execute("SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS");
//			stmt.executeBatch();
			stmt.close();
			oriConn.close();
			laucaConn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	//在测试数据库上运行合成负载
	private void geneSyntheticWorkload() {
		// 反序列化
		List<Transaction> transactions = null;
		DistributionCounter4Serial dcs = null;
		try {
			// 获取事务逻辑
			File file = new File(Configurations.getTxLogicSaveFile());
			long filelength = file.length();
			byte[] filecontent = new byte[(int) filelength];
			FileInputStream in = new FileInputStream(file);
			in.read(filecontent);
			in.close();
			Gson gson = new GsonBuilder().registerTypeAdapter(TransactionBlock.class, new TransactionBlockAdapter())
					.registerTypeAdapter(SqlStatement.class, new SqlStatementAdapter())
					.registerTypeAdapter(DataAccessDistribution.class, new DataAccessDistributionAdapter())
					.registerTypeAdapter(SequentialParaDistribution.class, new SequentialParaDistributionAdapter())
					.create();
			transactions = Arrays.asList(gson.fromJson(new String(filecontent, StandardCharsets.UTF_8), Transaction[].class));

			// 获取访问参数分布
			file = new File(Configurations.getDistributionSaveFile());
			filelength = file.length();
			filecontent = new byte[(int) filelength];
			in = new FileInputStream(file);
			in.read(filecontent);
			in.close();
			dcs = gson.fromJson(new String(filecontent, StandardCharsets.UTF_8), DistributionCounter4Serial.class);
		} catch (Exception e) {
			e.printStackTrace();
		}
		DistributionCounter.deserialInit(dcs);
//		System.out.println("反序列化结束~");

		// 读取配置信息
		int allThreadNum = Configurations.getAllTestThreadNum();
		int localThreadNum = Configurations.getLocalTestThreadNum();
		Workload workload = new Workload(transactions);
		String ip = Configurations.getLaucaDatabaseIp();
		String port = Configurations.getLaucaDatabasePort();
		String dbName = Configurations.getLaucaDatabaseName();
		String userName = Configurations.getLaucaDatabaseUserName();
		String passwd = Configurations.getLaucaDatabasePasswd();

		// 获取，生成数据
		DBConnector dbConnector = new DBConnector(ip, port, dbName, userName, passwd);
		WorkloadGenerator workloadGenerator = new WorkloadGenerator(allThreadNum, localThreadNum, workload,
				dbConnector);
		workloadGenerator.constructWindowThroughputList(DistributionCounter.getTxName2ThroughputList());
		List<Map<String, Map<String, DataAccessDistribution>>> windowDistributionList = DistributionCounter.getWindowDistributionList();
		windowDistributionList = DistributionCounter.windowDistributionAverage(windowDistributionList);
		workloadGenerator.setWindowDistributionList(windowDistributionList);


		workloadGenerator
				.setTxName2ParaId2FullLifeCycleDistribution(DistributionCounter.getTxName2ParaId2GlobalDistribution());
		// 开始执行
		CountDownLatch countDownLatch =new CountDownLatch(allThreadNum+1);
		new Thread(new Monitor(Configurations.getStatWindowSize(),countDownLatch)).start();

		workloadGenerator.startAllThreads(countDownLatch);

		//使用join 等待所有线程结束
		try {
			Thread.sleep(Configurations.getTestTimeLength() * 1000L);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

//		try {
//			Thread.sleep(2*1000);
//		}catch (InterruptedException e){
//			e.printStackTrace();
//		}



//		System.out.println("geneTime: "+ this.geneTime);
//		System.out.println("updateTime: "+this.updateTime);
//		System.out.println("moreUpdateRowCount: "+moreUpdateRowCount);
//		System.out.println("multipleUpdateRowCount: "+multipleUpdateRowCount);
//		System.out.println("multipleNoUpdateRowCount: "+multipleNoUpdateRowCount);



		Stats.printPartitionStats();
		System.exit(0);
	}

	private static void oracleLoader(File dataFile, Table table) throws IOException {
		File ctlfile = new File(".//testdata//ctl//" + "ctl_" + dataFile.getName());
		if (ctlfile.exists()) {
			ctlfile.delete();
		}
		ctlfile.createNewFile();
		// 创建控制文件
		String strctl = "LOAD DATA\r\n" + "INFILE '" + dataFile.getCanonicalPath() + "'\r\n" + "APPEND INTO TABLE "
				+ table.getName() + "\r\n" + "FIELDS TERMINATED BY ','\r\n";
		// 添加列名
		strctl += "(";
		Column[] columns = table.getColumns();
		for (int i = 0; i < columns.length; ++i) {
			strctl += columns[i].getName();
			if (columns[i].getDataType() == 4) {
				strctl += " char(20000)";
			} else if (columns[i].getDataType() == 3) {
				strctl += " date \"yyyy-mm-dd-hh24:mi:ss\"";
			}
			if (i == columns.length - 1) {
				strctl += " TERMINATED BY whitespace)";
			} else {
				strctl += ",";
			}
		}
		FileWriter fw = new FileWriter(ctlfile);
		fw.write(strctl);
		fw.flush();
		fw.close();
		String username = Configurations.getLaucaDatabaseUserName();
		String password = Configurations.getLaucaDatabasePasswd();
		// 要执行的DOS命令
		String dos = "sqlldr " + username + "/" + password + "@lauca" + " control="
				+ ctlfile.getCanonicalPath() + " direct=true log=oracle_dataloader.log";
//		System.out.println(dos);
		Runtime.getRuntime().exec(dos);

	}

}
