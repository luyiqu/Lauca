package input;

import java.io.File;
import java.sql.Timestamp;
import java.util.*;
import java.util.regex.Pattern;

import abstraction.*;
import accessdistribution.DistributionTypeInfo;
import config.Configurations;

public class SqlParser {

	private List<Table> tables = null;
	private List<StoredProcedure> storedProcedures = null;

	private static Pattern readSqlTmplPattern = Pattern.compile("(SELECT|Select|select)[\\s]+(@@)[\\s\\S]+");
	private static Pattern aggrTmplPattern = Pattern.compile("[ \\t]*select[ \\t]+(sum|count|avg|max|min)[\\s\\S]+");
	private static Pattern limitTmplPattern = Pattern.compile("[ \\t]*select[\\s\\S^(limit)]+limit[ \\t]+1[\\s\\S]*");
	private static Pattern commaPattern = Pattern.compile(",");
	private static Pattern dotPattern = Pattern.compile("\\.");
	private static Pattern predicatePattern = Pattern.compile("(=|>=|>|<|<=)");
	private static Pattern asPattern = Pattern.compile("(\\)as )|(\\) as )|[(|)| ]+");

	public SqlParser(List<Table> tables) {
		this.tables = tables;
	}
	public SqlParser(List<Table> tables,List<StoredProcedure> storedProcedures) {
		this.tables = tables;
		this.storedProcedures = storedProcedures;

	}

	public static void main(String[] args) {

		TableInfoSerializer serializer = new TableInfoSerializer();
		List<Table> tables = serializer.read(new File("E://dataCharacteristicSaveFile.obj"));
		// System.out.println(tables);
		SqlParser sqlParser = new SqlParser(tables);

		List<String> para = new ArrayList<String>();

//		String str = "SELECT MAX(ycsb_key) FROM `USERTABLE`";
//		ReadOperation rop1 = sqlParser.parseReadSqlStatement(str,0,
//				para);
//		ReadOperation rop1 = sqlParser.parseReadSqlStatement(
//				"SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM CUSTOMER WHERE C_W_ID = 325 AND C_D_ID = 23 AND C_ID = 46", 0,
//				new ArrayList<>());
//		ReadOperation rop2 = sqlParser.parseReadSqlStatement(
//				"SELECT SUM(OL_AMOUNT) AS OL_TOTAL##decimal FROM ORDER_LINE WHERE OL_O_ID = ? AND OL_D_ID = ? AND OL_W_ID = ?;",
//				0, para);
//		ReadOperation rop3 = sqlParser.parseReadSqlStatement(
//				"SELECT COUNT(DISTINCT (S_I_ID)) AS STOCK_COUNT##integer FROM ORDER_LINE, STOCK WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID < ? AND OL_O_ID >= ? AND S_W_ID = ? AND S_I_ID = OL_I_ID AND S_QUANTITY < ?;",
//				0, para);
//
//		System.out.println(rop1.sql);
//		System.out.println(rop2.sql);
//		System.out.println(rop3.sql);
//
//		WriteOperation wop1 = sqlParser.parseWriteSqlStatement(
//				"INSERT INTO OORDER (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL) VALUES (45, 45.4, 352, 90, 213, 8723 , 3);",
//				false, 0, new ArrayList<>());

//		List<String> para = new ArrayList<String>();
//		WriteOperation wop2 = sqlParser.parseWriteSqlStatement(
//				"replace INTO HISTORY (H_C_D_ID, H_C_W_ID, H_C_ID, H_D_ID, H_W_ID, H_DATE, H_AMOUNT, H_DATA) VALUES (45,3,435,2343,53.4,42,76,12);",
//				false, 0, para);

//		WriteOperation wop3 = sqlParser.parseWriteSqlStatement(
//				"UPDATE ORDER_LINE   SET OL_DELIVERY_D = '2020-04-27 17:11:00.733'  WHERE OL_O_ID = 2133    AND OL_D_ID = 1    AND OL_W_ID = 1 ",
//				false, 0, para);
//		System.out.println(para);
//
//		WriteOperation wop4 = sqlParser.parseWriteSqlStatement(
//				"UPDATE CUSTOMER SET C_BALANCE = 43, C_YTD_PAYMENT = 82, C_PAYMENT_CNT = 3546, C_DATA = 882 WHERE C_W_ID = 8982  AND C_D_ID = 74 AND C_ID = 66;",
//				false, 0, new ArrayList<>());
//		WriteOperation wop5 = sqlParser.parseWriteSqlStatement(
//				"DELETE FROM NEW_ORDER WHERE NO_O_ID = 22 AND NO_D_ID = 98 AND NO_W_ID = 0 AND NO_O_ID=NO_W_ID", false,
//				0, new ArrayList<>());

		WriteOperation wop1 = sqlParser.parseWriteSqlTemplate(
				"UPDATE STOCK   SET S_QUANTITY = ? ,        S_YTD = S_YTD + ?,        S_ORDER_CNT = S_ORDER_CNT + 1,        S_REMOTE_CNT = S_REMOTE_CNT + ?  WHERE S_I_ID = ?    AND S_W_ID = ? ",
				false, 0, para);

//		WriteOperation wop2 = sqlParser.parseWriteSqlStatement(
//				" UPDATE CUSTOMER SET C_BALANCE = C_BALANCE + ?, C_DELIVERY_CNT = C_DELIVERY_CNT + 1 WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?",
//				false, 0, para);

		System.out.println(para);



//		System.out.println(wop1.sql);
//		System.out.println(wop2.sql);
//		System.out.println(wop3.sql);
//		System.out.println(wop4.sql);
//		System.out.println(wop5.sql);
	}

	// modified by zsy:解析sql语句，parameters用来返回sql中的参数.如果是解析sql模板的话就不管这个参数了
	// 对select语句进行解析
	// 目前支持的select语句结构：select ... from ... where ... group by ... order by ... limit
	// ...
	@SuppressWarnings("static-access")
	public ReadOperation parseReadSqlStatement(String sql, int operationId, List<String> parameters) {
		String originalSql = sql;
		sql = sql.toLowerCase();
		int index1 = sql.indexOf("select ");
		int index2 = sql.indexOf(" from ");
		int index3 = sql.indexOf(" where ");
		int index4 = sql.indexOf(" group by ");
		int index5 = sql.indexOf(" order by ");
		int index6 = sql.indexOf(" limit ");
		// added by zsy:for update
		int index7 = sql.indexOf(" for ");

		if (index2 == -1 || sql.matches("(SELECT|Select|select)[\\s]+(@@)[\\s\\S]+")) {
			return null;
		}

		// 目前默认返回项都是简单的形式（仅属性），若不是简单形式需添加"##data_type"针对返回数据类型进行补充说明
		String[] returnItems = null;
		try {
			returnItems = sql.substring(index1 + 7, index2).replaceAll("[ \\t]+", " ").split(",");
		} catch (Exception e) {
			System.out.println(sql);
		}

//		System.out.println(sql);
		// 数据表名一般区分大小写，同时可能含有多数据表
		// TODO 没有谓词的情况没考虑  fixed by lyqu

		String[] tableNames = null;
		if(index3 == -1){
			tableNames = commaPattern.split(sql.substring(index2 + 6).replaceAll("[ \\t]+|`", ""));
		}
		else{
			tableNames = commaPattern.split(sql.substring(index2 + 6, index3).replaceAll("[ \\t]+|`", ""));
		}

//		try{
//
//		}catch (Exception exp){
//			System.out.println(sql);
//		}

		int[] returnDataTypes = new int[returnItems.length];
		Arrays.fill(returnDataTypes, -1);
		for (int i = 0; i < returnItems.length; i++) {
			returnItems[i] = returnItems[i].trim();
			if (returnItems[i].contains("##")) {
				returnDataTypes[i] = dataType2int(returnItems[i].split("##")[1].trim());
			} else {
				String[] itemExpr = asPattern.split(returnItems[i]);
				if (itemExpr.length == 1) {
					Column column = searchColumn(tableNames, returnItems[i]); // 返回项为单属性形式
					if (column != null) {
						returnDataTypes[i] = column.getDataType();
					} else {
						System.err.println("Unrecognized column: " + returnItems[i]);
					}
				}
				switch (itemExpr[0]) {
					case "sum":
					case "min":
					case "max":
						System.out.printf(itemExpr[1]);
						Column column = searchColumn(tableNames, itemExpr[1]); // 返回项为单属性形式

						if (column != null) {
							returnDataTypes[i] = column.getDataType();
						} else {
							System.err.println("Unrecognized column: " + returnItems[i]);
						}
						break;
					case "count":
						returnDataTypes[i] = 0;
						break;
					case "avg":
						// 取平均的都按decimal
						returnDataTypes[i] = 2;
						break;
				}

			}
		}

		// 利用minIdx来定位where后 谓词语句的末尾位置
		int minIdx = Integer.MAX_VALUE;
		if (index4 != -1 && minIdx > index4)
			minIdx = index4;
		if (index5 != -1 && minIdx > index5)
			minIdx = index5;
		if (index6 != -1 && minIdx > index6)
			minIdx = index6;
		if (index7 != -1 && minIdx > index7)
			minIdx = index7;

		// 目前默认条件谓词之间只有'and'运算符，并且一个谓词之中仅含有一个待输入参数（谓词中可能不含任何输入参数）
		// between and 需转化成两个谓词表示的形式，此时两个参数之间是有大小关系的，属于事务逻辑的一部分（暂不实现）。
		// modified by zsy:去掉分号
		if(index3 == -1){
			System.out.println("DEBUG****************");
			return new ReadOperation(operationId, originalSql.replaceAll("##[a-zA-Z]+", ""), returnItems ,returnDataTypes);
		}
		String[] predicates = sql.substring(index3 + 7, minIdx == Integer.MAX_VALUE ? sql.length() : minIdx)
				.split("(and)|;");

		// 有些谓词中可能不含输入参数
		// deleted by zsy
//		List<String> tmp = new ArrayList<>();
//		for (int i = 0; i < predicates.length; i++) {
//			if (predicates[i].contains("?")) {
//				tmp.add(predicates[i]);
//			}
//		}
//		predicates = new String[tmp.size()];
//		tmp.toArray(predicates);
//		int[] paraDataTypes = new int[predicates.length];
//		Arrays.fill(paraDataTypes, -1);
//		// sql参数的数据分布类型信息
//		DistributionTypeInfo[] paraDistTypeInfos = new DistributionTypeInfo[predicates.length];

		// added by zsy
		List<Integer> paraDataTypes = new ArrayList<Integer>();
		// sql参数的数据分布类型信息
		List<DistributionTypeInfo> paraDistTypeInfos = new ArrayList<DistributionTypeInfo>();
		List<String> paraSchemaInfos = new ArrayList<>();

		// 目前条件谓词中只支持条件运算符：=,>=,>,<,<=；不支持between and, like等运算符
		// 目前条件谓词中不支持任何算术运算，具体形式为：columnName op ?
		List<String> columnNames = new ArrayList<>(); // 用于后面检测是否是基于主键的查询

		// added by zsy
		// 判断是sql模板还是带参的sql语句
		boolean isTemplate = false;
		// 存储sql语句谓词的模板
		String predicateTemplate = new String();

		for (String predicate : predicates) {
			// modified by zsy:划分
			int opStartIndex = -1, opEndIndex = -1;// 关系运算符位置的开始和结束
			for (int j = 0; j < predicate.length(); ++j) {
				if (predicate.charAt(j) == '>' || predicate.charAt(j) == '<' || predicate.charAt(j) == '='
						|| predicate.charAt(j) == '!') {
					opStartIndex = j;
					if (predicate.charAt(j + 1) == '=' || predicate.charAt(j + 1) == '>') {
						opEndIndex = j + 1;
					} else {
						opEndIndex = j;
					}
					break;
				}
			}

			// added by zsy:分解谓词
			String[] predicateStrings = new String[3];
			predicateStrings[0] = predicate.substring(0, opStartIndex).trim();
			predicateStrings[1] = predicate.substring(opStartIndex, opEndIndex + 1).trim();
			predicateStrings[2] = predicate.substring(opEndIndex + 1).trim();
			// 谓词不包含输入参数的情况仅支持 column1 op column2
			if (searchColumn(tableNames, predicateStrings[2]) != null) {
				predicateTemplate += " " + predicateStrings[0] + " " + predicateStrings[1] + " " + predicateStrings[2]
						+ " and";
				continue;
			}

			String columnName = predicateStrings[0];
			columnNames.add(columnName);
			String tableName = searchTableName4Column(tableNames, columnName);
			Column column = searchColumn(tableName, columnName);
			if (column != null) {
				paraDataTypes.add(column.getDataType());
				paraDistTypeInfos.add(getParaDistTypeInfo(tableName, columnName));
				paraSchemaInfos.add(tableName + Partition.PARA_SCHEMA_SEPARATOR + columnName);
			} else {
				System.err.println("Unrecognized column: " + columnName);
			}

			// added by zsy:处理带参sql
			if (!isTemplate) {
				if (predicateStrings[2].equals("?")) {
					isTemplate = true;
					continue;
				}
				// 处理参数，去掉头尾单引号
				if (predicateStrings[2].charAt(0) == '\'' || predicateStrings[2].charAt(0) == '"') {
					predicateStrings[2] = predicateStrings[2].substring(1, predicateStrings[2].length() - 1);
				}
				parameters.add(predicateStrings[2]);
				// 将SQL中参数换成?
				predicateTemplate += " " + predicateStrings[0] + " " + predicateStrings[1] + " ? and";
			}
		}

		// added by zsy
		if (!isTemplate) {
			StringBuilder sqlBuffer = new StringBuilder(originalSql);
			// 结尾可能没有分号
			originalSql = new String(sqlBuffer.replace(index3 + 6, minIdx == Integer.MAX_VALUE ? sql.length() : minIdx,
					predicateTemplate.substring(0, predicateTemplate.length() - 4)));
		}
		// for test
//		System.out.println(parameters);


		boolean filterPrimaryKey = checkFilterPK(tableNames, columnNames);

		if (sql.matches("[ \\t]*select[ \\t]+(sum|count|avg|max|min)[\\s\\S]+")
				|| sql.matches("[ \\t]*select[\\s\\S^(limit)]+limit[ \\t]+1[\\s\\S]*")) {
			filterPrimaryKey = true;
		}

		int[] tmp = new int[paraDataTypes.size()];
		for (int i = 0; i < tmp.length; ++i) {
			tmp[i] = paraDataTypes.get(i);
		}
		transTimestamp(parameters, tmp);


		return new ReadOperation(operationId, originalSql.replaceAll("##[a-zA-Z]+", ""), tmp,
				paraDistTypeInfos.toArray(new DistributionTypeInfo[0]), paraSchemaInfos, returnItems,
				returnDataTypes, filterPrimaryKey);
	}

	private boolean checkFilterPK(String[] tableNames, List<String> columnNames) {
		boolean filterPrimaryKey = false;
		if (tableNames.length == 1) {
			Set<String> columnNameSet = new HashSet<>(columnNames);
			for (Table table : tables) {
				if (table.getName().equals(tableNames[0])) {
					if (columnNameSet.containsAll(Arrays.asList(table.getPrimaryKey()))) {
						filterPrimaryKey = true;
						break;
					}
				}
			}
		}
		return filterPrimaryKey;
	}

	// modified by zsy:解析sql语句，parameters用来返回sql中的参数.如果是解析sql模板的话就不管这个参数了
	// 对写操作语句进行解析，写操作语句有：insert、replace、update、delete
	// insert/replace 语句结构：insert/replace into table_name (columns...) values
	// (?,...,?);
	// update 语句结构：update table_name set ... where ...;
	// delete 语句结构：delete from table_name where ...;
	// 假设更新操作仅涉及单表
	@SuppressWarnings("static-access")
	public WriteOperation parseWriteSqlStatement(String sql, boolean batchExecute, int operationId,
			List<String> parameters) {
		String originalSql = sql.trim();
		sql = sql.trim().toLowerCase();

		if (sql.startsWith("insert") || sql.startsWith("replace")) {
			int index1 = sql.indexOf(" into ");
			int index2 = sql.indexOf("(");
			int index3 = sql.indexOf(")");

			String tableName = sql.substring(index1 + 6, index2).trim();

			String[] columnNames = sql.substring(index2 + 1, index3).replaceAll("[ \\t]+", "").split(",");
			int[] paraDataTypes = new int[columnNames.length];
			Arrays.fill(paraDataTypes, -1);
			DistributionTypeInfo[] paraDistTypeInfos = new DistributionTypeInfo[columnNames.length];

			// add by wsy
			List<String> paraSchemaInfos = new ArrayList<>();

			for (int i = 0; i < columnNames.length; i++) {
				Column column = searchColumn(tableName, columnNames[i]);
				if (column != null) {
					paraDataTypes[i] = column.getDataType();
					paraDistTypeInfos[i] = getParaDistTypeInfo(tableName, columnNames[i]);
					paraSchemaInfos.add(tableName + Partition.PARA_SCHEMA_SEPARATOR + columnNames[i]);
				} else {
					System.err.println("Unrecognized column: " + columnNames[i]);
				}
			}

			// added by zsy
			int index4 = sql.indexOf("values");
			int index5 = sql.indexOf('(', index4);
			int index6 = sql.indexOf(')', index4);
			String[] paraStrings = sql.substring(index5 + 1, index6).replaceAll("['\"]+", "").split(",");
			for (int i = 0; i < paraStrings.length; ++i) {
				paraStrings[i] = paraStrings[i].trim();
			}
			// 如果是带参sql
			if (!paraStrings[0].equals("?")) {
				parameters.addAll(Arrays.asList(paraStrings));
				String paraTemplate = new String();
				for (int i = 0; i < paraStrings.length; ++i) {
					paraTemplate += " ?,";
				}
				// 去掉最后一个逗号
				paraTemplate = paraTemplate.substring(0, paraTemplate.length() - 1);
				StringBuffer sqlBuffer = new StringBuffer(originalSql);
				// 结尾可能没有分号
				originalSql = new String(sqlBuffer.replace(index5 + 1, index6, paraTemplate));
			}
			// for test
//			System.out.println(parameters);

			// 将timestamp的类型转换
			transTimestamp(parameters, paraDataTypes);
			return new WriteOperation(operationId, originalSql, paraDataTypes, paraDistTypeInfos, paraSchemaInfos, batchExecute);
		} else if (sql.startsWith("update")) {
			int index1 = sql.indexOf("update ");
			int index2 = sql.indexOf(" set ");
			int index3 = sql.indexOf(" where ");

			String tableName = sql.substring(index1 + 7, index2).trim();

			// 对于set后面的表达式，这里假设形式都为：column = exp(?)，并且一个表达式中最多包含一个待输入参数
			// set表达式也可能不包含任何输入参数，如：c=c+1
			// modified by zsy:set后表达式的形式只支持 (column或参数) op (column或参数)
			// 即c=c+1中的1也当作参数，一个表达式内也可能有两个参数
			String[] setStatements = sql.substring(index2 + 5, index3).split(",");

			// deleted by zsy
//			List<String> tmp = new ArrayList<>();
//			for (int i = 0; i < setStatements.length; i++) {
//				if (setStatements[i].contains("?")) {
//					tmp.add(setStatements[i]);
//				}
//			}
//			setStatements = new String[tmp.size()];
//			tmp.toArray(setStatements);

			// 目前默认条件谓词之间只有'and'运算符，并且一个谓词之中仅含有一个待输入参数
			// modified by zsy
			String[] predicates = sql.substring(index3 + 7).split("(and)|;");
			// 有些谓词中可能不含输入参数
			// deleted by zsy
//			tmp.clear();
//			for (int i = 0; i < predicates.length; i++) {
//				if (predicates[i].contains("?")) {
//					tmp.add(predicates[i]);
//				}
//			}
//			predicates = new String[tmp.size()];
//			tmp.toArray(predicates);
//			int[] paraDataTypes = new int[setStatements.length + predicates.length];
//			Arrays.fill(paraDataTypes, -1);
//			DistributionTypeInfo[] paraDistTypeInfos = new DistributionTypeInfo[paraDataTypes.length];

			// added by zsy
			List<Integer> paraDataTypes = new ArrayList<Integer>();
			// sql参数的数据分布类型信息
			List<DistributionTypeInfo> paraDistTypeInfos = new ArrayList<DistributionTypeInfo>();

			// add by wsy
			List<String> paraSchemaInfos = new ArrayList<>();

			// added by zsy
			// 判断是sql模板还是带参的sql语句
			boolean isTemplate = false;
			String setExprTemplate = new String();

			for (String setStatement : setStatements) {
				// modified by zsy:划分
				// set后表达式的形式: column = (column或参数) op (column或参数)，目前一个set表达式只支持有最多1个参数
				// 或 column = column或参数
				int assignmentSignIndex = -1;// 赋值等号的位置
				int opIndex = -1;// 运算符的位置
				for (int j = 0; j < setStatement.length(); ++j) {
					if (setStatement.charAt(j) == '=') {
						assignmentSignIndex = j;
						break;
					}
				}
				int cursor = assignmentSignIndex + 1;
				while (setStatement.charAt(cursor) == ' ') {
					++cursor;
				}
				if (setStatement.charAt(cursor) == '"' || setStatement.charAt(cursor) == '\'') {
					// 去掉前后引号。这证明该表达式是一个字符串，此时后面必定没有运算符
					setStatement.replaceAll("[\"']", "");
				} else {
					// 第一个正负号是不能算的
					for (int j = cursor + 1; j < setStatement.length(); ++j) {
						if (setStatement.charAt(j) == '+' || setStatement.charAt(j) == '-'
								|| setStatement.charAt(j) == '*' || setStatement.charAt(j) == '/'
								|| setStatement.charAt(j) == '%') {
							opIndex = j;
							break;
						}
					}
				}
				// added by zsy:分解表达式
				String[] exprStrings = new String[3];
				exprStrings[0] = setStatement.substring(0, assignmentSignIndex).trim();
				if (opIndex != -1) {
					exprStrings[1] = setStatement.substring(assignmentSignIndex + 1, opIndex).trim();
					exprStrings[2] = setStatement.substring(opIndex + 1).trim();
				} else {
					exprStrings[1] = setStatement.substring(assignmentSignIndex + 1).trim();
					exprStrings[2] = null;
				}
				// 谓词不包含输入参数的情况仅支持 column1 op column2
				boolean leftExprIsColumn = (searchColumn(tableName, exprStrings[1]) != null);
				boolean rightExprIsColumn = (exprStrings[2] == null) || (searchColumn(tableName, exprStrings[2]) != null);
				if (leftExprIsColumn && rightExprIsColumn) {
					if (opIndex == -1) {
						setExprTemplate += " " + exprStrings[0] + " = " + exprStrings[1] + ",";
					} else {
						setExprTemplate += " " + exprStrings[0] + " = " + exprStrings[1] + " "
								+ setStatement.charAt(opIndex) + " " + exprStrings[2] + ",";
					}
					continue;
				}
//				String columnName = setStatements[i].split("=")[0].trim();
				String columnName = exprStrings[0];

				Column column = searchColumn(tableName, columnName);
				if (column != null) {
					paraDataTypes.add(column.getDataType());
					paraDistTypeInfos.add(getParaDistTypeInfo(tableName, columnName));
					paraSchemaInfos.add(tableName + Partition.PARA_SCHEMA_SEPARATOR + columnName);
				} else {
					System.err.println("Unrecognized column: " + columnName);
				}
				// added by zsy:处理带参sql
//				if (!isTemplate) {
//					if (exprStrings[1].equals("?") || (exprStrings[2] != null && exprStrings[2].equals("?"))) {
//						isTemplate = true;
//						continue;
//					}
				// 处理参数
				if (opIndex == -1) {
					if (!leftExprIsColumn) {
						if (exprStrings[1].charAt(0) == '\'' || exprStrings[1].charAt(0) == '"') {
							exprStrings[1] = exprStrings[1].substring(1, exprStrings[1].length() - 1);
						}
						parameters.add(exprStrings[1]);
						// 将SQL中参数换成?
						setExprTemplate += " " + exprStrings[0] + " = ?,";
					} else {
						setExprTemplate += " " + exprStrings[0] + " = " + exprStrings[1] + ",";
					}
				} else {
					if (!leftExprIsColumn) {
						if (exprStrings[1].charAt(0) == '\'' || exprStrings[1].charAt(0) == '"') {
							exprStrings[1] = exprStrings[1].substring(1, exprStrings[1].length() - 1);
						}
						parameters.add(exprStrings[1]);
						// 将SQL中参数换成?
						setExprTemplate += " " + exprStrings[0] + " = ? " + setStatement.charAt(opIndex);
					} else {
						setExprTemplate += " " + exprStrings[0] + " = " + exprStrings[1] + " "
								+ setStatement.charAt(opIndex);
					}
					if (!rightExprIsColumn) {
						if (exprStrings[2].charAt(0) == '\'' || exprStrings[2].charAt(0) == '"') {
							exprStrings[2] = exprStrings[2].substring(1, exprStrings[1].length() - 1);
						}
						parameters.add(exprStrings[2]);
						setExprTemplate += " ?,";
					} else {
						setExprTemplate += " " + exprStrings[2] + ",";
					}
				}
//				}
			}

			// added by zsy
			// 存储sql语句谓词的模板
			String predicateTemplate = new String();

			for (String predicate : predicates) {
				// modified by zsy:划分
				int opStartIndex = -1, opEndIndex = -1;// 关系运算符位置的开始和结束
				for (int j = 0; j < predicate.length(); ++j) {
					if (predicate.charAt(j) == '>' || predicate.charAt(j) == '<'
							|| predicate.charAt(j) == '=' || predicate.charAt(j) == '!') {
						opStartIndex = j;
						if (predicate.charAt(j + 1) == '=' || predicate.charAt(j + 1) == '>') {
							opEndIndex = j + 1;
						} else {
							opEndIndex = j;
						}
						break;
					}
				}
				// added by zsy:分解谓词
				String[] predicateStrings = new String[3];
				predicateStrings[0] = predicate.substring(0, opStartIndex).trim();
				predicateStrings[1] = predicate.substring(opStartIndex, opEndIndex + 1).trim();
				predicateStrings[2] = predicate.substring(opEndIndex + 1).trim();
				// 谓词不包含输入参数的情况仅支持 column1 op column2
				if (searchColumn(tableName, predicateStrings[2]) != null) {
					predicateTemplate += " " + predicateStrings[0] + " " + predicateStrings[1] + " "
							+ predicateStrings[2] + " and";
					continue;
				}
//				String columnName = predicates[i].split("(=|>=|>|<|<=)")[0].trim();
				String columnName = predicateStrings[0];

				Column column = searchColumn(tableName, columnName);
				if (column != null) {
					paraDataTypes.add(column.getDataType());
					paraDistTypeInfos.add(getParaDistTypeInfo(tableName, columnName));
					paraSchemaInfos.add(tableName + Partition.PARA_SCHEMA_SEPARATOR + columnName);
				} else {
					System.err.println("Unrecognized column: " + columnName);
				}
				// added by zsy:处理带参sql
				if (!isTemplate) {
					if (predicateStrings[2].equals("?")) {
						isTemplate = true;
						continue;
					}
					// 处理参数，去掉头尾单引号
					if (predicateStrings[2].charAt(0) == '\'' || predicateStrings[2].charAt(0) == '"') {
						predicateStrings[2] = predicateStrings[2].substring(1, predicateStrings[2].length() - 1);
					}
					parameters.add(predicateStrings[2]);
					// 将SQL中参数换成?
					predicateTemplate += " " + predicateStrings[0] + " " + predicateStrings[1] + " ? and";
				}

			}
			// added by zsy:替换成表达式模板和谓词模板
			if (!isTemplate) {
				StringBuilder sqlBuffer = new StringBuilder(originalSql);
				// 结尾可能没有分号
				sqlBuffer.replace(index3 + 6, sql.length(),
						predicateTemplate.substring(0, predicateTemplate.length() - 4));
				// added by zsy:去掉最后的逗号
				setExprTemplate = setExprTemplate.substring(0, setExprTemplate.length() - 1);
				sqlBuffer.replace(index2 + 4, index3, setExprTemplate);
				originalSql = new String(sqlBuffer);
			} else {// 替换表达式模板，针对+1
				StringBuilder sqlBuffer = new StringBuilder(originalSql);
				setExprTemplate = setExprTemplate.substring(0, setExprTemplate.length() - 1);
				sqlBuffer.replace(index2 + 4, index3, setExprTemplate);
				originalSql = new String(sqlBuffer);
			}

			int[] tmp = new int[paraDataTypes.size()];
			for (int i = 0; i < tmp.length; ++i) {
				// 将timestamp的类型转换
				if (paraDataTypes.get(i) == 3 && parameters.size() > 0 && parameters.get(i).contains("-")) {
					Timestamp ts = new Timestamp(0);
					parameters.set(i, "" + ts.valueOf(parameters.get(i)).getTime());
				}
				tmp[i] = paraDataTypes.get(i);
			}

//			System.out.println(paraDataTypes);

			return new WriteOperation(operationId, originalSql, tmp,
					paraDistTypeInfos.toArray(new DistributionTypeInfo[paraDistTypeInfos.size()]), paraSchemaInfos, batchExecute);
		} else if (sql.startsWith("delete")) {
			int index1 = sql.indexOf(" from ");
			int index2 = sql.indexOf(" where ");

			String tableName = originalSql.substring(index1 + 6, index2).trim();

			String[] predicates = sql.substring(index2 + 7).split("(and)|;");

			// 有些谓词中可能不含输入参数
			// deleted by zsy
//			List<String> tmp = new ArrayList<>();
//			for (int i = 0; i < predicates.length; i++) {
//				if (predicates[i].contains("?")) {
//					tmp.add(predicates[i]);
//				}
//			}
//			predicates = new String[tmp.size()];
//			tmp.toArray(predicates);
//			int[] paraDataTypes = new int[predicates.length];
//			Arrays.fill(paraDataTypes, -1);
//			DistributionTypeInfo[] paraDistTypeInfos = new DistributionTypeInfo[paraDataTypes.length];

			// added by zsy
			List<Integer> paraDataTypes = new ArrayList<Integer>();
			// sql参数的数据分布类型信息
			List<DistributionTypeInfo> paraDistTypeInfos = new ArrayList<DistributionTypeInfo>();
			// add by wsy
			List<String> paraSchemaInfos = new ArrayList<>();

			// added by zsy
			// 判断是sql模板还是带参的sql语句
			boolean isTemplate = false;
			// 存储sql语句谓词的模板
			String predicateTemplate = new String();

			for (String predicate : predicates) {
				// modified by zsy:划分
				int opStartIndex = -1, opEndIndex = -1;// 关系运算符位置的开始和结束
				for (int j = 0; j < predicate.length(); ++j) {
					if (predicate.charAt(j) == '>' || predicate.charAt(j) == '<'
							|| predicate.charAt(j) == '=' || predicate.charAt(j) == '!') {
						opStartIndex = j;
						if (predicate.charAt(j + 1) == '=' || predicate.charAt(j + 1) == '>') {
							opEndIndex = j + 1;
						} else {
							opEndIndex = j;
						}
						break;
					}
				}
				// added by zsy:分解谓词
				String[] predicateStrings = new String[3];
				predicateStrings[0] = predicate.substring(0, opStartIndex).trim();
				predicateStrings[1] = predicate.substring(opStartIndex, opEndIndex + 1).trim();
				predicateStrings[2] = predicate.substring(opEndIndex + 1).trim();
				// 谓词不包含输入参数的情况仅支持 column1 op column2
				if (searchColumn(tableName, predicateStrings[2]) != null) {
					predicateTemplate += " " + predicateStrings[0] + " " + predicateStrings[1] + " "
							+ predicateStrings[2] + " and";
					continue;
				}

				String columnName = predicateStrings[0];
				Column column = searchColumn(tableName, columnName);
				if (column != null) {
					paraDataTypes.add(column.getDataType());
					paraDistTypeInfos.add(getParaDistTypeInfo(tableName, columnName));
					paraSchemaInfos.add(tableName + Partition.PARA_SCHEMA_SEPARATOR + columnName);
				} else {
					System.out.println(sql);
					System.err.println("Unrecognized column: " + columnName);
				}
				// added by zsy:处理带参sql
				if (!isTemplate) {
					if (predicateStrings[2].equals("?")) {
						isTemplate = true;
						continue;
					}
					// 处理参数，去掉头尾单引号
					if (predicateStrings[2].charAt(0) == '\'' || predicateStrings[2].charAt(0) == '"') {
						predicateStrings[2] = predicateStrings[2].substring(1, predicateStrings[2].length() - 1);
					}
					parameters.add(predicateStrings[2]);
					// 将SQL中参数换成?
					predicateTemplate += " " + predicateStrings[0] + " " + predicateStrings[1] + " ? and";
				}
			}

			// added by zsy
			if (!isTemplate) {
				StringBuffer sqlBuffer = new StringBuffer(originalSql);
				// 结尾可能没有分号
				originalSql = new String(sqlBuffer.replace(index2 + 6, sql.length(),
						predicateTemplate.substring(0, predicateTemplate.length() - 4)));
			}
			// for test
//			System.out.println(parameters);

			// added by zsy
			int[] tmp = new int[paraDataTypes.size()];
			for (int i = 0; i < tmp.length; ++i) {
				// 将timestamp的类型转换
				if (paraDataTypes.get(i) == 3 && parameters.size() > 0 && parameters.get(i).contains("-")) {
					Timestamp ts = new Timestamp(0);
					parameters.set(i, "" + ts.valueOf(parameters.get(i)).getTime());
				}
				tmp[i] = paraDataTypes.get(i);
			}
			return new WriteOperation(operationId, originalSql, tmp,
					paraDistTypeInfos.toArray(new DistributionTypeInfo[paraDistTypeInfos.size()]),paraSchemaInfos, batchExecute);
		} else {
			System.err.println("Unrecognized write operation: " + originalSql);
			return null;
		}
	}

	/**
	 * 解析读sql模板
	 */
	@SuppressWarnings("static-access")
	public ReadOperation parseReadSqlTemplate(String sql, int operationId, List<String> parameters) {
		String originalSql = sql;
		sql = sql.toLowerCase();
		int index1 = sql.indexOf("select ");
		int index2 = sql.indexOf(" from ");
		int index3 = sql.indexOf(" where ");
		int index4 = sql.indexOf(" group by ");
		int index5 = sql.indexOf(" order by ");
		int index6 = sql.indexOf(" limit ");



		if (index2 == -1 || readSqlTmplPattern.matcher(sql).matches()) {
			return null;
		}

		// 数据表名一般区分大小写，同时可能含有多数据表
		String[] tableNames = null;
		if(index3 == -1){
			tableNames = commaPattern.split(sql.substring(index2 + 6).replaceAll("[ \\t]+|`", ""));
		}
		else{
			tableNames = commaPattern.split(sql.substring(index2 + 6, index3).replaceAll("[ \\t]+|`", ""));
		}
		// 目前默认返回项都是简单的形式（仅属性），若不是简单形式需添加"##data_type"针对返回数据类型进行补充说明
		String[] returnItems = commaPattern.split(originalSql.substring(index1 + 7, index2));
		ArrayList<String> returnItemsList = new ArrayList<String>();
		//added by ct —— select * from a; select * from a,b; select a.*, b.* from a,b;
		//但是原来没有考虑 表名.列名 的情况 TODO
		for (String returnItem : returnItems) {
			if (returnItem.contains("*")) {
				if (returnItem.contains(".")) {
					String tN = dotPattern.split(returnItem)[0].trim();
					//System.out.println("name "+tN);
					returnItemsList.addAll(allColumnNames(tN));
				} else {
					for (String tableName : tableNames) returnItemsList.addAll(allColumnNames(tableName));
				}
			} else {
				returnItemsList.add(returnItem);
			}
		}
		returnItems = returnItemsList.toArray(new String[returnItemsList.size()]);
//		System.out.println(originalSql);
//		System.out.println(returnItemsList + " "+returnItemsList.size());
//		System.out.println(returnItems.toString()+ " " + returnItems.length);

		int[] returnDataTypes = new int[returnItems.length];
		Arrays.fill(returnDataTypes, -1);
		for (int i = 0; i < returnItems.length; i++) {
			returnItems[i] = returnItems[i].toLowerCase().trim();
			if (returnItems[i].contains("##")) {
				returnDataTypes[i] = dataType2int(returnItems[i].split("##")[1].trim());
			} else {
				String[] itemExpr = asPattern.split(returnItems[i]);
				if (itemExpr.length == 1) {
					Column column = searchColumn(tableNames, returnItems[i]); // 返回项为单属性形式
					if (column != null) {
						returnDataTypes[i] = column.getDataType();
					} else {
						System.err.println("Unrecognized column: " + returnItems[i]);
					}
				}
				switch (itemExpr[0]) {
					case "sum":
					case "min":
					case "max":
						Column column = searchColumn(tableNames, itemExpr[1]); // 返回项为单属性形式

						if (column != null) {
							returnDataTypes[i] = column.getDataType();
						} else {
							System.err.println("Unrecognized column: " + returnItems[i]);
						}
						break;
					case "count":
						returnDataTypes[i] = 0;
						break;
					case "avg":
						// 取平均的都按decimal
						returnDataTypes[i] = 2;
						break;
				}

			}

		}

		// 利用minIdx来定位where后 谓词语句的末尾位置

		int minIdx = Integer.MAX_VALUE;

		if (index4 != -1 )
			minIdx = index4;

		if (index5 != -1 && minIdx > index5)
			minIdx = index5;

		if (index6 != -1 && minIdx > index6)
			minIdx = index6;

		// 目前默认条件谓词之间只有'and'运算符，并且一个谓词之中仅含有一个待输入参数（谓词中可能不含任何输入参数）

		// between and 需转化成两个谓词表示的形式，此时两个参数之间是有大小关系的，属于事务逻辑的一部分（暂不实现）。

		String[] predicates = sql.substring(index3 + 7, minIdx == Integer.MAX_VALUE ? sql.length() : minIdx)
				.replaceAll("[ \\t]+", "").split("and");

		// 有些谓词中可能不含输入参数

		List<String> tmp = new ArrayList<>();

		for (String predicate : predicates) {
			if (predicate.contains("?")) {
				tmp.add(predicate);
			}
		}

		predicates = new String[tmp.size()];

		tmp.toArray(predicates);

		int[] paraDataTypes = new int[predicates.length];
		List<String> paraSchemaInfos = new ArrayList<>();

		Arrays.fill(paraDataTypes, -1);

		// sql参数的数据分布类型信息

		DistributionTypeInfo[] paraDistTypeInfos = new DistributionTypeInfo[predicates.length];

		// 目前条件谓词中只支持条件运算符：=,>=,>,<,<=；不支持between and, like等运算符

		// 目前条件谓词中不支持任何算术运算，具体形式为：columnName op ?

		List<String> columnNames = new ArrayList<>(); // 用于后面检测是否是基于主键的查询

		for (int i = 0; i < predicates.length; i++) {

			String columnName = predicatePattern.split(predicates[i])[0];
			columnNames.add(columnName);
			String tableName = searchTableName4Column(tableNames, columnName);
			Column column = searchColumn(tableName, columnName);

			if (column != null) {
				paraDataTypes[i] = column.getDataType();
				paraDistTypeInfos[i] = getParaDistTypeInfo(tableName, columnName);
				paraSchemaInfos.add(tableName + Partition.PARA_SCHEMA_SEPARATOR + columnName);
			} else {
				System.err.println("Unrecognized column: " + columnName);
			}

		}

		boolean filterPrimaryKey = checkFilterPK(tableNames, columnNames);

		if (aggrTmplPattern.matcher(sql).matches()
				|| limitTmplPattern.matcher(sql).matches()) {
			filterPrimaryKey = true;
		}

		transTimestamp(parameters, paraDataTypes);

		return new ReadOperation(operationId, originalSql.replaceAll("##[a-zA-Z]+", ""), paraDataTypes,
				paraDistTypeInfos,paraSchemaInfos, returnItems, returnDataTypes, filterPrimaryKey);
	}

	/**
	 * 将timestamp的类型转换
 	 */
	private void transTimestamp(List<String> parameters, int[] paraDataTypes) {
		for (int i = 0; i < paraDataTypes.length; ++i) {
			// 将timestamp的类型转换
			if (paraDataTypes[i] == 3 && parameters.size() > 0 && parameters.get(i).contains("-")) {
				parameters.set(i, "" + Timestamp.valueOf(parameters.get(i)).getTime());
			}
		}
	}

	/**
	 * 解析写sql模板
	 * 
	 * @return
	 */
	@SuppressWarnings("static-access")
	public WriteOperation parseWriteSqlTemplate(String sql, boolean batchExecute, int operationId,
			List<String> parameters) {
		String originalSql = sql.trim();

		sql = sql.trim().toLowerCase();

		if (sql.startsWith("insert") || sql.startsWith("replace")) {

			int index1 = sql.indexOf(" into ");

			int index2 = sql.indexOf("(");

			int index3 = sql.indexOf(")");

			String tableName = sql.substring(index1 + 6, index2).trim();

			String[] columnNames = commaPattern.split(sql.substring(index2 + 1, index3).replaceAll("[ \\t]+", ""));

			if (columnNames[0].contains("?")){
				tableName = tableName.substring(0,tableName.length() -6).trim();
				int i = 0;
				for (String columns : allColumnNames(tableName)){
					columnNames[i] = columns;
					i++;
				}
			}

			int[] paraDataTypes = new int[columnNames.length];

			Arrays.fill(paraDataTypes, -1);

			DistributionTypeInfo[] paraDistTypeInfos = new DistributionTypeInfo[columnNames.length];
			// add by wsy
			List<String> paraSchemaInfos = new ArrayList<>();

			for (int i = 0; i < columnNames.length; i++) {

//				System.out.println(tableName+" "+columnNames[i]);
				Column column = searchColumn(tableName, columnNames[i]);

				if (column != null) {

					paraDataTypes[i] = column.getDataType();

					paraDistTypeInfos[i] = getParaDistTypeInfo(tableName, columnNames[i]);
					paraSchemaInfos.add(tableName + Partition.PARA_SCHEMA_SEPARATOR + columnNames[i]);

				} else {
					System.err.println("1Unrecognized column: " + columnNames[i]);

				}

			}

			transTimestamp(parameters, paraDataTypes);
			return new WriteOperation(operationId, originalSql, paraDataTypes, paraDistTypeInfos, paraSchemaInfos, batchExecute);

		} else if (sql.startsWith("update")) {

			int index1 = sql.indexOf("update ");

			int index2 = sql.indexOf(" set ");

			int index3 = sql.indexOf(" where ");

			String tableName = sql.substring(index1 + 7, index2).trim();

			// 对于set后面的表达式，这里假设形式都为：column = exp(?)，并且一个表达式中最多包含一个待输入参数

			// set表达式也可能不包含任何输入参数，如：c=c+1

			String[] setStatements = commaPattern.split(sql.substring(index2 + 5, index3));

			List<String> tmp = new ArrayList<>();

			for (String setStatement : setStatements) {

				if (setStatement.contains("?")) {

					tmp.add(setStatement);

				}

			}

			setStatements = new String[tmp.size()];

			tmp.toArray(setStatements);

			// 目前默认条件谓词之间只有'and'运算符，并且一个谓词之中仅含有一个待输入参数

			String[] predicates = sql.substring(index3 + 7).split("and");

			// 有些谓词中可能不含输入参数

			tmp.clear();

			for (String predicate : predicates) {

				if (predicate.contains("?")) {

					tmp.add(predicate);

				}

			}

			predicates = new String[tmp.size()];

			tmp.toArray(predicates);

			int[] paraDataTypes = new int[setStatements.length + predicates.length];

			Arrays.fill(paraDataTypes, -1);

			DistributionTypeInfo[] paraDistTypeInfos = new DistributionTypeInfo[paraDataTypes.length];
			// add by wsy
			List<String> paraSchemaInfos = new ArrayList<>();

			for (int i = 0; i < setStatements.length; i++) {

				String columnName = setStatements[i].split("=")[0].trim();

				Column column = searchColumn(tableName, columnName);

				if (column != null) {

					paraDataTypes[i] = column.getDataType();

					paraDistTypeInfos[i] = getParaDistTypeInfo(tableName, columnName);
					paraSchemaInfos.add(tableName + Partition.PARA_SCHEMA_SEPARATOR + columnName);
				} else {

					System.err.println("2Unrecognized column: " + columnName);

				}

			}

			for (int i = 0; i < predicates.length; i++) {

				String columnName = predicatePattern.split(predicates[i])[0].trim();

				Column column = searchColumn(tableName, columnName);

				if (column != null) {

					paraDataTypes[setStatements.length + i] = column.getDataType();

					paraDistTypeInfos[setStatements.length + i] = getParaDistTypeInfo(tableName, columnName);
					paraSchemaInfos.add(tableName + Partition.PARA_SCHEMA_SEPARATOR + columnName);

				} else {

					System.err.println("Unrecognized column: " + columnName);

				}

			}
			transTimestamp(parameters, paraDataTypes);
			return new WriteOperation(operationId, originalSql, paraDataTypes, paraDistTypeInfos, paraSchemaInfos, batchExecute);

		} else if (sql.startsWith("delete")) {

			int index1 = sql.indexOf(" from ");

			int index2 = sql.indexOf(" where ");

			String tableName = sql.substring(index1 + 6, index2).trim();

			String[] predicates = sql.substring(index2 + 7).split("and");

			// 有些谓词中可能不含输入参数

			List<String> tmp = new ArrayList<>();

			for (String predicate : predicates) {

				if (predicate.contains("?")) {

					tmp.add(predicate);

				}

			}

			predicates = new String[tmp.size()];

			tmp.toArray(predicates);

			int[] paraDataTypes = new int[predicates.length];

			Arrays.fill(paraDataTypes, -1);

			DistributionTypeInfo[] paraDistTypeInfos = new DistributionTypeInfo[paraDataTypes.length];
			// add by wsy
			List<String> paraSchemaInfos = new ArrayList<>();

			for (int i = 0; i < predicates.length; i++) {

				String columnName = predicatePattern.split(predicates[i])[0].trim();

				Column column = searchColumn(tableName, columnName);

				if (column != null) {

					paraDataTypes[i] = column.getDataType();

					paraDistTypeInfos[i] = getParaDistTypeInfo(tableName, columnName);
					paraSchemaInfos.add(tableName + Partition.PARA_SCHEMA_SEPARATOR + columnName);

				} else {

					System.err.println("Unrecognized column: " + columnName);

				}

			}
			transTimestamp(parameters, paraDataTypes);

			return new WriteOperation(operationId, originalSql, paraDataTypes, paraDistTypeInfos, paraSchemaInfos, batchExecute);

		}else if(sql.startsWith("call")){
			int index1 = sql.indexOf("(");
			String procedureName = sql.substring(5, index1).trim();
//			System.out.println("SqlParsr:"+procedureName);
			StoredProcedure storedProcedure = searchStoredProcedure(procedureName);
//			System.out.println("SqlParser: "+storedProcedure.getColumn()[0].getName());
			Column[] columns = storedProcedure.getColumn();
			Table[] tables4StoredProcedure = storedProcedure.getTable();
			int[] paraDataTypes = new int[columns.length];
			DistributionTypeInfo[] paraDistTypeInfos = new DistributionTypeInfo[columns.length];
			// add by wsy
			List<String> paraSchemaInfos = new ArrayList<>();
			for (int i = 0; i < columns.length; i++) {
					paraDataTypes[i] =columns[i].getDataType();
					paraDistTypeInfos[i] = getParaDistTypeInfo(tables4StoredProcedure[i],columns[i]);
					paraSchemaInfos.add(tables4StoredProcedure[i] + "_" + columns[i]);
			}

			for (int i = 0; i < columns.length; ++i) {
				// 将timestamp的类型转换
				if (columns[i].getDataType() == 3 && parameters.size() > 0 && parameters.get(i).contains("-")) {
					Timestamp ts = new Timestamp(0);
					parameters.set(i, "" + ts.valueOf(parameters.get(i)).getTime());
				}
			}
			return new WriteOperation(operationId, originalSql, paraDataTypes, paraDistTypeInfos,paraSchemaInfos, batchExecute);
		} 
		
		else {

			System.err.println("Unrecognized write operation: " + originalSql);

			return null;

		}
	}

	// 假设所有属性名都不带表名前缀 & 所有数据表的属性名都不一样
	// 主外键属性也具有相应的Column对象
	private String searchTableName4Column(String[] tableNames, String columnName){
		for (Table table : tables) {
			for (String tableName : tableNames) {
				if (table.getName().equalsIgnoreCase(tableName)) {
					Column[] columns = table.getColumns();
					for (Column column : columns) {
						if (column.getName().equalsIgnoreCase(columnName)) {
							return tableName;
						}
					}
				}
			}
		}
		return null;
	}
	private Column searchColumn(String[] tableNames, String columnName) {
		String tableName = searchTableName4Column(tableNames, columnName);
		return searchColumn(tableName, columnName);
	}

	private Column searchColumn(String tableName, String columnName) {
		for (Table table : tables) {
			if (table.getName().equalsIgnoreCase(tableName)) {
				Column[] columns = table.getColumns();
				for (Column column : columns) {
					if (column.getName().equalsIgnoreCase(columnName)) {
						return column;
					}
				}
			}
		}
		return null;
	}

	//added by ct —— 针对 select *
	private ArrayList<String> allColumnNames(String tableName) {
		ArrayList<String> columnNames = new ArrayList<String>();
		for (Table table : tables) {
			if (table.getName().equals(tableName)) {
				Column[] columns = table.getColumns();
				for (Column column : columns) {
					columnNames.add(column.getName());
				}
			}
		}
		return columnNames;
	}
	
	private StoredProcedure searchStoredProcedure(String storedProceduredName) {
		for (StoredProcedure storedProcedure : storedProcedures) {
			if (storedProcedure.getName().equals(storedProceduredName)) {
				return storedProcedure;
			}
		}
		return null;
	}

	
	private int dataType2int(String dataType) {
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

	private DistributionTypeInfo getParaDistTypeInfo(String[] tableNames, String columnName) {
		Table table = null;
		Column column = null;
		for (Table value : tables) {
			for (String tableName : tableNames) {
				if (value.getName().equals(tableName)) {
					Column[] columns = value.getColumns();
					for (Column item : columns) {
						if (item.getName().equals(columnName)) {
							table = value;
							column = item;
						}
					}
				}
			}
		}
		// table & column 必然不为Null（由该函数的调用位置决定的）
		return getParaDistTypeInfo(table, column);
	}

	private DistributionTypeInfo getParaDistTypeInfo(String tableName, String columnName) {
		Table table = null;
		Column column = null;
		for (Table value : tables) {
			if (value.getName().equalsIgnoreCase(tableName)) {
				Column[] columns = value.getColumns();
				for (Column item : columns) {
					if (item.getName().equalsIgnoreCase(columnName)) {
						table = value;
						column = item;
					}
				}
			}
		}
		return getParaDistTypeInfo(table, column);
	}

	//for storedProcedure added by qly
	private DistributionTypeInfo getParaDistTypeInfo(Column column) {
		int distCategory = Configurations.getDistributionCategory();  //2：基于连续时间窗口的数据访问分布；
		int dataType = column.getDataType();
		// 当前暂不考虑外键所在数据表size小于参照主键所在数据表size的情形
		if (distCategory == 0) {

		} else if (distCategory == 1) {
			// 1: real(double); 2: decimal(BigDecimal); 3: datetime(Date millisecond --
			// long);
			if (dataType == 1 || dataType == 2 || dataType == 3) {
				return new DistributionTypeInfo(0, dataType);
			} else if (dataType == 0) {// 整型
				return new DistributionTypeInfo(1, (long) column.getPara1(), (long) column.getPara2(),
						column.getCardinality(), column.getCoefficient());
			} else if (dataType == 4) {// varchar
				return new DistributionTypeInfo(2, column.getCardinality(), column.getMinLength(),
						column.getMaxLength(), column.getSeedStrings());
			}
		} else if (distCategory == 2) {

			if (dataType == 1 || dataType == 2 || dataType == 3) {
				return new DistributionTypeInfo(0, dataType);
			} else if (dataType == 0) {
				return new DistributionTypeInfo(4, (long) column.getPara1(), (long) column.getPara2(),
						column.getCardinality(), column.getCoefficient());
			} else if (dataType == 4) {
				return new DistributionTypeInfo(5, column.getCardinality(), column.getMinLength(),
						column.getMaxLength(), column.getSeedStrings());
			}
		} else if (distCategory == 3) {

		} else if (distCategory == -1) {

		}

		return null;
	}

	private DistributionTypeInfo getParaDistTypeInfo(Table table, Column column) {
		int dataType = column.getDataType();
		boolean isPk = false, isFk = false;
		for (int i = 0; i < table.getPrimaryKey().length; i++) {
			if (table.getPrimaryKey()[i].equals(column.getName())) {
				isPk = true;
				break;
			}
		}
		loop: for (int i = 0; i < table.getForeignKeys().length; i++) {
			String[] fkColumnNames = table.getForeignKeys()[i].getLocalColumns();
			for (int j = 0; j < fkColumnNames.length; j++) {
				if (fkColumnNames[j].equals(column.getName())) {
					isFk = true;
					break loop;
				}
			}
		}
		int distCategory = Configurations.getDistributionCategory();  //2：基于连续时间窗口的数据访问分布；

		// 当前暂不考虑外键所在数据表size小于参照主键所在数据表size的情形
		if (distCategory == 0) {

		} else if (distCategory == 1) {
			// 1: real(double); 2: decimal(BigDecimal); 3: datetime(Date millisecond --
			// long);
			if (isPk || isFk || dataType == 1 || dataType == 2 || dataType == 3) {
				return new DistributionTypeInfo(0, dataType);
			} else if (dataType == 0) {// 整型
				return new DistributionTypeInfo(1, (long) column.getPara1(), (long) column.getPara2(),
						column.getCardinality(), column.getCoefficient());
			} else if (dataType == 4) {// varchar
				return new DistributionTypeInfo(2, column.getCardinality(), column.getMinLength(),
						column.getMaxLength(), column.getSeedStrings());
			}
		} else if (distCategory == 2) {
			if (isPk || isFk) {
				return new DistributionTypeInfo(3, dataType);//主键或外键
			} else if (dataType == 1 || dataType == 2 || dataType == 3) {
				return new DistributionTypeInfo(0, dataType);
			} else if (dataType == 0) {
				return new DistributionTypeInfo(4, (long) column.getPara1(), (long) column.getPara2(),
						column.getCardinality(), column.getCoefficient());
			} else if (dataType == 4) {
				return new DistributionTypeInfo(5, column.getCardinality(), column.getMinLength(),
						column.getMaxLength(), column.getSeedStrings());
			}
		} else if (distCategory == 3) {

		} else if (distCategory == -1) {

		}

		return null;
	}
}
