package input;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import abstraction.Column;
import abstraction.ForeignKey;
import abstraction.Table;

/**
 * 测试数据库Schema信息的读取，Table schema形式： Table[table_name; ?; column_name data_type;
 * ...; PK(primary_key_columns); FK(foreign_key, referenced_table,
 * referenced_primary_key, ?); ...]
 * 上面第一个？处指数据表的大小，第二个？处指平均扩展系数，这两处的值都是从源数据库中获取的。 TP负载的扩展一般是指吞吐量的扩展，数据集的大小一般无需扩展。
 */
public class SchemaReader {

	private Logger logger = Logger.getLogger(SchemaReader.class);

	// for testing
	public static void main(String[] args) {
		PropertyConfigurator.configure(".//lib//log4j.properties");
		SchemaReader schemaReader = new SchemaReader();
		System.out.println(schemaReader.read(new File(".//testdata//tpcc-schema.txt")));
		schemaReader.read(new File(".//testdata//schemaTestFiles//test.txt"));
	}

	public List<Table> read(File schemaFile) {
		List<Table> tables = new ArrayList<>();
		try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(schemaFile), "utf-8"))) {
			String inputLine = null;
			while ((inputLine = br.readLine()) != null) {
				// skip the blank lines and comments
				if (inputLine.matches("[\\s]*") || inputLine.matches("[ ]*##[\\s\\S]*")) {
					continue;
				}

				if (inputLine.matches("[ \\t]*(Table|TABLE|table)[ \\t]*\\[[\\s\\S^\\]]+\\][ \\t]*")) {
					inputLine = inputLine.substring(inputLine.indexOf('[') + 1, inputLine.lastIndexOf(']')).trim();
					String[] schemaInfoArr = inputLine.split(";");
					String tableName = schemaInfoArr[0].trim();
					int tableSize = -1;
//					if (!schemaInfoArr[1].trim().equals("?")) {
//						tableSize = Integer.parseInt(schemaInfoArr[1].trim());
//						logger.info(tableSize + "，目前数据表的大小都是从源数据库中获取的，故这里设置的数据表大小没有实际用处，后面会被覆盖掉！");
//					}

					List<Column> columns = new ArrayList<>();
					String[] primaryKey = null;
					List<ForeignKey> foreignKeys = new ArrayList<>();

					for (int i = 1; i < schemaInfoArr.length; i++) {
						String schemaInfo = schemaInfoArr[i].trim();
						// table name is case-sensitive，外键约束中需要指出参照数据表
						if (!schemaInfo.matches("(fk|FK|Fk|fK)[\\s\\S]+")) {
							schemaInfo = schemaInfo.toLowerCase();
						}

						if (schemaInfo.matches("[a-z0-9_]+[ \\t]+(integer|real|decimal|datetime|varchar|boolean)")) {
							String columnName = schemaInfo.split("[ \\t]+")[0];
							String dataType = schemaInfo.split("[ \\t]+")[1];
							columns.add(new Column(columnName, dataType2Int(dataType)));
						} else if (schemaInfo.matches("pk\\([ \\t]*[a-z0-9_]+([ \\t]*,[ \\t]*[a-z0-9_]+)*[ \\t]*\\)")) { // 需要支持复合主键
							int beginIndex = schemaInfo.indexOf('(') + 1;
							int endIndex = schemaInfo.lastIndexOf(')');
							schemaInfo = schemaInfo.substring(beginIndex, endIndex).replaceAll("[ \\t]+", "");
							primaryKey = schemaInfo.split(",");
						} else if (schemaInfo.matches("(fk|FK|Fk|fK)\\([ \\t]*[a-zA-Z0-9_# ]+[ \\t]*,[ \\t]*" // 外键也可能是属性集合，用#隔开
//								+ "[a-zA-Z0-9_]+[ \\t]*,[ \\t]*[a-zA-Z0-9_# ]+[ \\t]*,[ \\t]*[0-9\\.?]+[ \\t]*\\)")) {
								+ "[a-zA-Z0-9_]+[ \\t]*,[ \\t]*[a-zA-Z0-9_# ]+[ \\t]*\\)")) {
							int beginIndex = schemaInfo.indexOf('(') + 1;
							int endIndex = schemaInfo.lastIndexOf(')');
							schemaInfo = schemaInfo.substring(beginIndex, endIndex).replaceAll("[ \\t]+", "");
							String[] tmp = schemaInfo.split(",");
							String[] localColumns = tmp[0].toLowerCase().split("#");
							String referencedTable = tmp[1];
							String[] referencedColumns = tmp[2].toLowerCase().split("#");
							double averageReferenceScale = -1;
//							if (!tmp[3].equals("?")) {
//								averageReferenceScale = Double.parseDouble(tmp[3]);
//								logger.info(schemaInfo + "，目前外键扩展因子是从源数据库（真实数据库）中分析得到的，所以这里的设置值没有实际用处！");
//							}
							foreignKeys.add(new ForeignKey(localColumns, referencedTable, referencedColumns,
									averageReferenceScale));
						} else {
							logger.error(schemaInfo + " cannot be parsed!");
						}
					}

					Column[] tmp1 = new Column[columns.size()];
					columns.toArray(tmp1);
					ForeignKey[] tmp2 = new ForeignKey[foreignKeys.size()];
					foreignKeys.toArray(tmp2);
					if (primaryKey == null) {
						primaryKey = new String[0];
					}
					tables.add(new Table(tableName, tableSize, tmp1, primaryKey, tmp2));
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		logger.info("All input table information:\n" + tables);
		return tables;
	}

	private int dataType2Int(String dataType) {
		switch (dataType) {
		case "integer":
			return 0;
		case "real":
			return 1;
		case "decimal":
			return 2;
		case "datetime":
			return 3;
		case "varchar":
			return 4;
		case "boolean":
			return 5;
		default:
			return -1;
		}
	}
}
