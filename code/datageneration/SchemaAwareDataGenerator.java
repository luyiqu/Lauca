package datageneration;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import org.apache.log4j.PropertyConfigurator;

import abstraction.Table;
import input.DBStatisticsCollector;
import input.SchemaReader;

public class SchemaAwareDataGenerator {

	public static void main(String[] args) {
		
//		PropertyConfigurator.configure(".//lib//log4j.properties");
//		SchemaReader schemaReader = new SchemaReader();
//		List<Table> tables = schemaReader.read(new File(".//testdata//tpcc-schema.txt"));
//		
//		String ip = "10.11.1.193", port = "13306", dbName = "tpcc_lym", userName = "root", passwd = "root";
//		DBStatisticsCollector collector = new DBStatisticsCollector(ip, port, dbName, userName, passwd, tables);
//		collector.run();
//		
//		DataGenerator dg1 = new DataGenerator(0, 1, 3, ".//testdata//tables", tables);
//		dg1.setUp();
		
		try (BufferedReader br = new BufferedReader(new InputStreamReader(new 
				FileInputStream(new File(args[0])), "utf-8"))) {
			String inputLine1 = br.readLine();
			String inputLine2 = br.readLine();
			String[] inputLine3 = br.readLine().split(";");
			String inputLine4 = br.readLine();
			String inputLine5 = br.readLine();
			
			PropertyConfigurator.configure(inputLine1);
			SchemaReader schemaReader = new SchemaReader();
			List<Table> tables = schemaReader.read(new File(inputLine2));
			
			String ip = inputLine3[0], port = inputLine3[1], dbName = inputLine3[2], 
					userName = inputLine3[3], passwd = inputLine3[4];
			DBStatisticsCollector collector = new DBStatisticsCollector(ip, port, dbName, userName, passwd, tables);
			collector.run();
			
			for (int i = 0; i < tables.size(); i++) {
				tables.get(i).init(tables);
			}
			
			System.out.println(tables);
			DataGenerator dg1 = new DataGenerator(0, 1, Integer.parseInt(inputLine5), inputLine4, tables);
			dg1.setUp();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
