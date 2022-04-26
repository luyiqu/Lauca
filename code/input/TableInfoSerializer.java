package input;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;

import org.apache.log4j.PropertyConfigurator;

import abstraction.Table;

public class TableInfoSerializer {

	public static void main(String[] args) {
		PropertyConfigurator.configure(".//lib//log4j.properties");
		SchemaReader schemaReader = new SchemaReader();
		List<Table> tables = schemaReader.read(new File(".//testdata//tpcc-schema.txt"));
		String ip = "10.11.6.125", port = "13306", dbName = "tpcc_sf10_0109", userName = "root", passwd = "root";
		DBStatisticsCollector collector = new DBStatisticsCollector(ip, port, dbName, userName, passwd, tables);
		collector.run();
		tables = collector.getTables();
		TableInfoSerializer serializer = new TableInfoSerializer();
		serializer.write(tables, new File(".//testdata//tables.obj"));
		System.out.println(serializer.read(new File(".//testdata//tables.obj")));
	}
	
	public void write(List<Table> tables, File output) {
		try (ObjectOutputStream writer = new ObjectOutputStream(new FileOutputStream(output))) {
			writer.writeObject(tables);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@SuppressWarnings("unchecked")
	public List<Table> read(File input) {
		try (ObjectInputStream reader = new ObjectInputStream(new FileInputStream(input))) {
			return (List<Table>)reader.readObject();
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
}
