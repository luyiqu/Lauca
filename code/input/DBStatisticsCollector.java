package input;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import abstraction.Column;
import abstraction.ForeignKey;
import abstraction.Table;
import config.Configurations;
import util.DBConnector;

import static config.Configurations.getDataIncreaseRate;

// 连接到源数据库（真实数据库），自动统计 数据表大小、外键扩展因子、属性的基本数据特征 这些信息
public class DBStatisticsCollector {

	// target database (original database)
	private String ip = null;
	private String port = null;
	private String dbName = null;
	private String userName = null;
	private String passwd = null;

	private List<Table> tables = null; // 从schema文件中获取的数据表基本信息

	private Logger logger = Logger.getLogger(DBStatisticsCollector.class);

	public DBStatisticsCollector(String ip, String port, String dbName, String userName, String passwd,
			List<Table> tables) {
		super();
		this.ip = ip;
		this.port = port;
		this.dbName = dbName;
		this.userName = userName;
		this.passwd = passwd;
		this.tables = tables;
	}

	@SuppressWarnings("resource")
	public void run() {
		DBConnector dbConnector = new DBConnector(ip, port, dbName, userName, passwd);

		Connection conn = null;
		String databaseType = Configurations.getDatabaseType().toLowerCase();
		switch (databaseType) {
			case "mysql":
			case "tidb":
				conn = dbConnector.getMySQLConnection();
				break;
			case "postgresql":
				conn = dbConnector.getPostgreSQLConnection();
				break;
			case "oracle":
				conn = dbConnector.getOracleConnection();
				break;
		}



		if(Configurations.isUseAutoSchemaReader()){
			SchemaAutoReader schemaAutoReader = new SchemaAutoReader();
			try{
				tables = schemaAutoReader.read(conn,databaseType,userName);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}else{
			SchemaReader schemaReader = new SchemaReader();
			tables = schemaReader.read(new File(Configurations.getDatabaseSchemaFile()));
		}



		try {
			Statement stmt = conn.createStatement();
			for (int i = 0; i < tables.size(); i++) {
				Table table = tables.get(i);

				{ // 数据集大小的扩展，随机增加或table表的数量
					String sql = "select count(*) from " + table.getName();
					ResultSet rs = stmt.executeQuery(sql);

					rs.next();

					//modified by qly,当设置10%，那么模糊化范围为8%-12%
					long tableSize = rs.getLong(1);
					double rate = Configurations.getDataIncreaseRate();
					double random = Math.random();
					double flag = Math.random();
					double rateFinal;
					if(rate == 0)
					{
						table.setSize(tableSize);
					}
					else
					{
						if(flag > 0.5){
							rateFinal = rate - random * 0.02;
						}
						else {
							rateFinal = rate + random * 0.02;
						}
						tableSize= (long)(tableSize * (1+rateFinal));
						table.setSize(tableSize);
					}



				}

				Column[] columns = table.getColumns();
				for (Column column : columns) {
					ResultSet rs = stmt.executeQuery(
							"select count(*) from " + table.getName() + " where " + column.getName() + " is null");
					rs.next();
					column.setNullRatio((double) rs.getLong(1) / table.getSize());
					rs = stmt.executeQuery("select count(distinct(" + column.getName() + ")) from " + table.getName()
							+ " where " + column.getName() + " is not null");
					rs.next();
					column.setCardinality(rs.getLong(1));

					switch (column.getDataType()) {
						case 0:
						case 1:
						case 2:
						case 3:
							// 注意我们的datetime对应mysql的timestamp
							rs = stmt.executeQuery("select min(" + column.getName() + ") from " + table.getName());
							rs.next();
							double minValue = column.getDataType() != 3 ? rs.getDouble(1) : rs.getTimestamp(1).getTime();
							rs = stmt.executeQuery("select max(" + column.getName() + ") from " + table.getName());
							rs.next();
							double maxValue = column.getDataType() != 3 ? rs.getDouble(1) : rs.getTimestamp(1).getTime();
							column.setPara1(minValue);
							column.setPara2(maxValue);
							break;
						case 4:
							rs = stmt.executeQuery("select avg(length(" + column.getName() + ")) from " + table.getName());
							rs.next();
							double avgLength = rs.getDouble(1);
							rs = stmt.executeQuery("select max(length(" + column.getName() + ")) from " + table.getName());
							rs.next();
							double maxLength = rs.getInt(1);
							column.setPara1(avgLength);
							column.setPara2(maxLength);
							break;
						case 5:
							rs = stmt.executeQuery(
									"select count(*) from " + table.getName() + " where " + column.getName() + " is True");
							rs.next();
							double trueRatio = rs.getLong(1) / ((1 - column.getNullRatio()) * table.getSize());
							column.setPara1(trueRatio);
							column.setPara2(1 - trueRatio);
							break;
					}

					rs.close();
					column.init();
				} // for columns

				// averageReferenceScale
				ForeignKey[] foreignKeys = table.getForeignKeys();
				for (ForeignKey foreignKey : foreignKeys) {
					// 目前外键扩展因子不支持用户指定 TODO
					// The referenced table must be defined before
					for (Table value : tables) {
						if (value.getName().equals(foreignKey.getReferencedTable())) {
							foreignKey.setAverageReferenceScale((double) table.getSize() / value.getSize());
							break;
						}
					}
				}

			} // for tables


			if(Configurations.getDataIncreaseRate() != 0.0){
				System.out.println("模糊化表规模完成~");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		logger.info("All table information after filling the data characteristics:" + tables);
	}

	public List<Table> getTables() {
		return tables;
	}
	//自动读取真实数据库中的数据表基本信息


	public static void main(String[] args) {
		PropertyConfigurator.configure(".//lib//log4j.properties");
		SchemaReader schemaReader = new SchemaReader();
		List<Table> tables = schemaReader.read(new File(".//testdata//tpcc-schema.txt"));
		String ip = "10.11.6.125", port = "13306", dbName = "tpcc_sf10_0109", userName = "root", passwd = "root";
		DBStatisticsCollector collector = new DBStatisticsCollector(ip, port, dbName, userName, passwd, tables);
		collector.run();
	}
}
