package datageneration;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.PropertyConfigurator;

import abstraction.Table;
import config.Configurations;
import input.TableInfoSerializer;

public class DataGenerator {

	// 从0开始编号
	private int machineId;
	private int machineNum;
	private int singleMachineThreadNum;
	private String outputDir = null;
	private List<Table> tables = null;
	
	public DataGenerator(int machineId, int machineNum, int singleMachineThreadNum, String outputDir, List<Table> tables) {
		super();
		this.machineId = machineId;
		this.machineNum = machineNum;
		this.singleMachineThreadNum = singleMachineThreadNum;
		this.outputDir = outputDir;
		this.tables = tables;
	}

	public void setUp() {
		int threadId = machineId * singleMachineThreadNum;
		int threadNum = machineNum * singleMachineThreadNum;
		
		for (int i = 0; i < tables.size(); i++) {
			CountDownLatch cdl = new CountDownLatch(singleMachineThreadNum);
			for (int j = 0; j < singleMachineThreadNum; j++) { //qly： 一个table的一个线程进行调用
				new Thread(new DataGenerationThread(threadId + j, threadNum, tables.get(i).clone(), 
						new File(outputDir + "//" + tables.get(i).getName() + "_" + (threadId + j) + ".txt"), 
						cdl)).start();
			}
			try {
				cdl.await();  //qly: 为保证table按照顺序生成，生成完一个table后，等待所有线程完成后，再开始生成下一个table
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) {
		PropertyConfigurator.configure(".//lib//log4j.properties");
		TableInfoSerializer serializer = new TableInfoSerializer();
		List<Table> tables = serializer.read(new File(".//testdata//tables2.obj"));
		
		DataGenerator dg1 = new DataGenerator(0, 2, 3, ".//testdata//tables", tables);
		dg1.setUp();
		DataGenerator dg2 = new DataGenerator(1, 2, 3, ".//testdata//tables", tables);
		dg2.setUp();
	}
}


class DataGenerationThread implements Runnable {

	private int threadId;
	private int threadNum;
	private Table table = null;
	private File outputFile = null;
	private CountDownLatch cdl = null;
	
	public DataGenerationThread(int threadId, int threadNum, Table table, File outputFile, CountDownLatch cdl) {
		super();
		this.threadId = threadId;
		this.threadNum = threadNum;
		this.table = table;
		this.outputFile = outputFile;
		this.cdl = cdl;
	}

	@Override
	public void run() {
		// 补丁：生成数据时（存储到文件中） 时间类型 需要从long型转化成字符型
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss");
		
		
		// Bug fix for PostgreSQL data import (COPY command)
		if (Configurations.getDatabaseType().toLowerCase().equals("postgresql")) {
			sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ");
		}
		// bug fix ----------
		
		
		table.setThreadNum(threadNum);
		table.setThreadId(threadId);
		try  {
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile), "utf-8"));
			StringBuilder sb = new StringBuilder();
			while (true) {
				Object[] tuple = table.geneTuple(sdf);
				if (tuple == null) {
					break;
				}
				for (int i = 0; i < tuple.length - 1; i++) {
					sb.append(tuple[i]).append(",");
				}
				sb.append(tuple[tuple.length - 1]).append("\r\n");
				//处理null值，导入mysql前，要将null换成\N
				if (sb.toString().contains("null") &&
						(Configurations.getDatabaseType().toLowerCase().equals("mysql")||Configurations.getDatabaseType().toLowerCase().equals("tidb"))){
					bw.write(sb.toString().replace("null","\\N"));
				}
				else bw.write(sb.toString());
				sb.setLength(0);

			}
			cdl.countDown();
			bw.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}