package abstraction;

import java.io.File;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Random;

import config.Configurations;
import org.apache.log4j.PropertyConfigurator;

import input.TableInfoSerializer;

public class Table implements Serializable, Cloneable {

	private static final long serialVersionUID = 1L;

	private String name = null;
	private long size;
	private Column[] columns = null;
	private String[] primaryKey = null;
	private ForeignKey[] foreignKeys = null;

	// 下面这些类成员信息是为了高效生成测试数据的
	// 每个主键属性的阈值（or数值范围）
	private long[] pkColumnRanges = null;
	// 支持 按序生成 主键（目前主键属性是从1开始按序生成的）
	private long[] pkColumnGeneCounts = null;
	// 每个主键值（or值组合）的留存比例（针对情况：主键属性全是外键属性，并且当前数据表的大小小于被参照数据表的大小）
	private double pkPersistRatio;
	// 主键属性在所有属性中的位置
	private int[] pkColumnIndexes = null;

	// 所有非主键属性的外键属性的阈值 及其 在所有属性中的位置
	private long[][] allFkColumnRanges = null;
	private int[][] allFkColumnIndexes = null;

	// 所有的非键值属性  //这里没有进行处理
	private int[] nonKeyColumnIndexes = null;

	// 所有数据生成线程的总线程数
	private int threadNum;
	// 在没有主键的情况下，需利用'singleThreadTupleNum'控制当前线程的tuple生成数
	private long singleThreadTupleNum;

	// 这些信息是从输入的schema文件中读取的
	public Table(String name, long size, Column[] columns, String[] primaryKey, ForeignKey[] foreignKeys) {
		super();
		this.name = name;
		this.size = size;
		this.columns = columns;
		this.primaryKey = primaryKey;
		this.foreignKeys = foreignKeys;
	}

	public Table(String name, long size, Column[] columns, String[] primaryKey, ForeignKey[] foreignKeys,
			long[] pkColumnRanges, long[] pkColumnGeneCounts, double pkPersistRatio, int[] pkColumnIndexes,
			long[][] allFkColumnRanges, int[][] allFkColumnIndexes, int[] nonKeyColumnIndexes, int threadNum,
			long singleThreadTupleNum) {
		super();
		this.name = name;
		this.size = size;
		this.columns = columns;
		this.primaryKey = primaryKey;
		this.foreignKeys = foreignKeys;
		this.pkColumnRanges = pkColumnRanges;
		this.pkColumnGeneCounts = pkColumnGeneCounts;
		this.pkPersistRatio = pkPersistRatio;
		this.pkColumnIndexes = pkColumnIndexes;
		this.allFkColumnRanges = allFkColumnRanges;
		this.allFkColumnIndexes = allFkColumnIndexes;
		this.nonKeyColumnIndexes = nonKeyColumnIndexes;
		this.threadNum = threadNum;
		this.singleThreadTupleNum = singleThreadTupleNum;
	}

	@Override
	// Table对象的深拷贝（其实好像只有pkColumnGeneCounts、singleThreadTupleNum需要被深复制，其他成员在生成数据时都不会被更新）
	public Table clone() {
		String name = new String(this.name);
		long size = this.size;
		Column[] columns = new Column[this.columns.length];
		for (int i = 0; i < columns.length; i++) {
			columns[i] = this.columns[i].clone();
		}
		String[] primaryKey = Arrays.copyOf(this.primaryKey, this.primaryKey.length);
		ForeignKey[] foreignKeys = new ForeignKey[this.foreignKeys.length];
		for (int i = 0; i < foreignKeys.length; i++) {
			foreignKeys[i] = this.foreignKeys[i].clone();
		}

		long[] pkColumnRanges = Arrays.copyOf(this.pkColumnRanges, this.pkColumnRanges.length);
		long[] pkColumnGeneCounts = Arrays.copyOf(this.pkColumnGeneCounts, this.pkColumnGeneCounts.length);
		double pkPersistRatio = this.pkPersistRatio;
		int[] pkColumnIndexes = Arrays.copyOf(this.pkColumnIndexes, this.pkColumnIndexes.length);

		long[][] allFkColumnRanges = new long[this.allFkColumnRanges.length][];
		for (int i = 0; i < allFkColumnRanges.length; i++) {
			allFkColumnRanges[i] = Arrays.copyOf(this.allFkColumnRanges[i], this.allFkColumnRanges[i].length);
		}
		int[][] allFkColumnIndexes = new int[this.allFkColumnIndexes.length][];
		for (int i = 0; i < allFkColumnIndexes.length; i++) {
			allFkColumnIndexes[i] = Arrays.copyOf(this.allFkColumnIndexes[i], this.allFkColumnIndexes[i].length);
		}

		int[] nonKeyColumnIndexes = Arrays.copyOf(this.nonKeyColumnIndexes, this.nonKeyColumnIndexes.length);
		int threadNum = this.threadNum;
		long singleThreadTupleNum = this.singleThreadTupleNum;

		return new Table(name, size, columns, primaryKey, foreignKeys, pkColumnRanges, pkColumnGeneCounts, pkPersistRatio, 
				pkColumnIndexes, allFkColumnRanges, allFkColumnIndexes, nonKeyColumnIndexes, threadNum, singleThreadTupleNum);
	}





	// 初始化支持键值属性生成的相关数据结构
	public void init(List<Table> tables) {
		// 从参照数据表中获取外键属性的相关信息
		Map<String, long[]> fk2ColumnRanges = new HashMap<>();
		for (ForeignKey fk : foreignKeys) {
			for (Table table : tables) { // list中的数据表序需与数据表之间的参照序一致
				if (table.getName().equals(fk.getReferencedTable())) {
					// 被参照的属性（集合）必然是被参照数据表的主键
					if(table.pkColumnRanges == null) table.init(tables);
					//外键顺序 按照被参照表的主键顺序排列
					String[] localColumns = fk.getLocalColumns();
					String[] referencedColumns = fk.getReferencedColumns();
					String[] sortedLocalColumns = new String[localColumns.length];
					String[] sortedReferencedColumns = new String[referencedColumns.length];
					String[] pks = table.primaryKey;
					for(int i=0;i<referencedColumns.length;i++){
						for(int j=0;j<pks.length;j++){
							if(pks[j].equals(referencedColumns[i])){
								sortedLocalColumns[i] = localColumns[j];
								sortedReferencedColumns[i] = referencedColumns[j];
								break;
							}
						}
					}
					fk.setLocalColumns(sortedLocalColumns);
					fk.setReferencedColumns(sortedReferencedColumns);
					fk2ColumnRanges.put(Arrays.toString(fk.getLocalColumns()), table.pkColumnRanges);
					break;
				}
			}
		}

		pkColumnRanges = new long[primaryKey.length];
		pkColumnGeneCounts = new long[primaryKey.length];
		
		// 主键冲突bug fix！主键值从1开始~
		Arrays.fill(pkColumnGeneCounts, 1);
		
		// 对于是外键的主键属性，其范围需要从外键那里获得
		loop : for (int i = 0; i < primaryKey.length; i++) {
			for (int j = 0; j < foreignKeys.length; j++) {
				String[] localColumns = foreignKeys[j].getLocalColumns();
				for (int k = 0; k < localColumns.length; k++) {
					if (primaryKey[i].equals(localColumns[k])) {
						pkColumnRanges[i] = fk2ColumnRanges.get(Arrays.toString(localColumns))[k];
						continue loop;
					}
				}
			}
		}

		// 针对复合主键，寻找主键中第一个不是外键的属性（一般根据物理意义，复合主键中应该仅有一个这样的非外键的主键属性）
		boolean flag = false;
		int firstNoFkPkColumnIndex = -1;
		for (int i = 0; i < pkColumnRanges.length; i++) {
			if (pkColumnRanges[i] == 0) { //表明它不是外键
				flag = true;
				firstNoFkPkColumnIndex = i;
				break;
			}
		}

		// 根据数据表大小以及参照属性的范围，确定主键中该本地属性的范围
		if (flag) {
			long pkColumnRangesProduct = 1;
			for (int i = 0; i < pkColumnRanges.length; i++) {
				if (pkColumnRanges[i] != 0) {  //排除是复合主键中为本地属性的属性
					pkColumnRangesProduct *= pkColumnRanges[i];
				}
			}
			pkColumnRanges[firstNoFkPkColumnIndex] = Math.round(size / (double)pkColumnRangesProduct); //table_size / 外键组成主键的情况 得到该本地属性的range
			pkPersistRatio = 1;
		}

		// 补丁：处理 主键中含有多个不是外键的属性 的情况（直接设为1合适吗）
		for (int i = 0; i < pkColumnRanges.length; i++) {
			if (pkColumnRanges[i] == 0) {
				pkColumnRanges[i] = 1;
			}
		}

		// 存在主键 & 主键属性全为参照属性（属于某个外键） 不存在本地属性为复合主键中一员的情况
		if (primaryKey.length != 0 && !flag) {
			// 根据数据表大小和所有主键属性的范围，确定主键的存留比例
			long pkColumnRangesProduct = 1;
			for (int i = 0; i < pkColumnRanges.length; i++) {
				pkColumnRangesProduct *= pkColumnRanges[i];
			}
			pkPersistRatio = size / (double)pkColumnRangesProduct;
			// pkPersistRatio若小于1，这样的主键如果被参照，外键那边无法保证参照完全性 TODO
		}

		// 确定主键属性在columns中的位置
		pkColumnIndexes = new int[primaryKey.length];
		for (int i = 0; i < primaryKey.length; i++) {
			for (int j = 0; j < columns.length; j++) {
				if (primaryKey[i].equals(columns[j].getName())) {
					pkColumnIndexes[i] = j;
					break;
				}
			}
		}

		// 外键相关信息的初始化
		allFkColumnRanges = new long[foreignKeys.length][];
		allFkColumnIndexes = new int[foreignKeys.length][];

		// 需要剔除所有外键属性中的主键属性（因为主键属性上面考虑过了）
		for (int i = 0; i < foreignKeys.length; i++) {
			String[] localColumns = foreignKeys[i].getLocalColumns();
			List<String> noPkLocalColumns  = new ArrayList<>();
			List<Long> noPkLocalColumnRanges = new ArrayList<>();

			for (int j = 0; j < localColumns.length; j++) {
				boolean tmp = true;
				for (int k = 0; k < primaryKey.length; k++) {
					if (localColumns[j].equals(primaryKey[k])) {
						tmp = false;
						break;
					}
				}
				if (tmp) {
					noPkLocalColumns.add(localColumns[j]);
					noPkLocalColumnRanges.add(fk2ColumnRanges.get(Arrays.toString(localColumns))[j]);
				}
			}

			allFkColumnRanges[i] = new long[noPkLocalColumns.size()];
			for (int j = 0; j < noPkLocalColumns.size(); j++) {
				allFkColumnRanges[i][j] = noPkLocalColumnRanges.get(j);
			}

			allFkColumnIndexes[i] = new int[noPkLocalColumns.size()];
			for (int j = 0; j < noPkLocalColumns.size(); j++) {
				for (int k = 0; k < columns.length; k++) {
					if (noPkLocalColumns.get(j).equals(columns[k].getName())) {
						allFkColumnIndexes[i][j] = k;
						break;
					}
				}
			}
		}

		Set<Integer> keyColumnIndexSet = new HashSet<>();
		for (int i = 0; i < pkColumnIndexes.length; i++) {
			keyColumnIndexSet.add(pkColumnIndexes[i]);
		}
		for (int i = 0; i < allFkColumnIndexes.length; i++) {
			for (int j = 0; j < allFkColumnIndexes[i].length; j++) {
				keyColumnIndexSet.add(allFkColumnIndexes[i][j]);
			}
		}
		nonKeyColumnIndexes = new int[columns.length - keyColumnIndexSet.size()];
		int count = 0;
		for (int i = 0; i < columns.length; i++) {
			if (!keyColumnIndexSet.contains(i)) {
				nonKeyColumnIndexes[count++] = i;
			}
		}
	}

	public Object[] geneTuple(SimpleDateFormat sdf) {
		Object[] tuple = new Object[columns.length];

		if (pkColumnGeneCounts.length >= 1) {
			pkColumnGeneCounts[pkColumnGeneCounts.length - 1] += (long)(threadNum / pkPersistRatio);
		} else {
			if (--singleThreadTupleNum < 0) {
				return null;
			}
		}

		// 注意：主键值从1开始！
		for (int i = pkColumnGeneCounts.length - 1; i >= 0; i--) {
			if (pkColumnGeneCounts[i] > pkColumnRanges[i]) {
				if (i == 0) {
					return null;
				}
				long tmp1 = pkColumnGeneCounts[i] / pkColumnRanges[i];
				long tmp2 = pkColumnGeneCounts[i] % pkColumnRanges[i];
				pkColumnGeneCounts[i] = tmp2;
				pkColumnGeneCounts[i - 1] += tmp1;
			} else {
				// 防止数据生成完了后，此函数被再次调用
				if (pkColumnGeneCounts[0] > pkColumnRanges[0]) {
					return null;
				}
				break;
			}
		}

		for (int i = 0; i < pkColumnIndexes.length; i++) {
			tuple[pkColumnIndexes[i]] = pkColumnGeneCounts[i];
		}

		for (int i = 0; i < allFkColumnIndexes.length; i++) {
			for (int j = 0; j < allFkColumnIndexes[i].length; j++) {
				tuple[allFkColumnIndexes[i][j]] = (long)(Math.random() * allFkColumnRanges[i][j]);
			}
		}

		for (int i = 0; i < nonKeyColumnIndexes.length; i++) {
			tuple[nonKeyColumnIndexes[i]] = columns[nonKeyColumnIndexes[i]].geneData(sdf);
		}

		return tuple;
	}

	public String getName() {
		return name;
	}

	public long getSize() {
		return size;
	}

	public Column[] getColumns() {
		return columns;
	}

	public String[] getPrimaryKey() {
		return primaryKey;
	}

	public ForeignKey[] getForeignKeys() {
		return foreignKeys;
	}

	public void setName(String name) { this.name = name; }

	public void setColumns(Column[] columns) { this.columns = columns; }

	public void setPrimaryKey(String[] primaryKey) { this.primaryKey = primaryKey; }

	public void setForeignKeys(ForeignKey[] foreignKeys) { this.foreignKeys = foreignKeys; }

	public void setSize(long size) {
		this.size = size;
	}

	public void setThreadNum(int threadNum) {
		this.threadNum = threadNum;
		this.singleThreadTupleNum = (long)Math.ceil((double)size / threadNum);
	}

	// 这里的线程id是全局的id（多数据生成节点）
	public void setThreadId(int threadId) {
		if (pkColumnGeneCounts.length >= 1) {
			pkColumnGeneCounts[pkColumnGeneCounts.length - 1] = threadId - threadNum + 1; // 这个负值不会出现在主键中，每次生成前都会加上threadNum
		}
	}

	//add by qly， 为了模糊化columnSize，生成一些无实际含义的column，只会被执行一次
	public void modifyColumns(){
		List<Column> modifyColumns = new ArrayList<>();

		for(Column col:columns){   //真实的column在最前面
			modifyColumns.add(col);
		}

		double increaseRate = Configurations.getFakeColumnRate();
		int generateTotalTableSize = (int)(columns.length*(1+increaseRate));
		int increaseColumnSize = generateTotalTableSize - columns.length;
		for (int i = 0; i < increaseColumnSize; i++)
		{
			int generateColumnNameSize = (int)(Math.random()*10+3);    //3-13个字符组成column_name
			String columnName = getRandomString(generateColumnNameSize);
			String dataType;
//			double flagDataType = Math.random();
//			if(flagDataType <= 0.5){
//				dataType = "integer";
//			}
//			else{
//				dataType= "boolean";
//			}
			//目前只针对integer
			dataType = "integer";
			Column newColumn = new Column(columnName, dataType2Int(dataType));
			newColumn.setNullRatio(0.0);
			if(newColumn.getDataType()==5){   //boolean
				newColumn.setCardinality(2);
			}
			else{
				newColumn.setCardinality(800);
			}
			if(newColumn.getDataType()<=3){
				newColumn.setPara1(31);
				newColumn.setPara2(812);
			}
			else if(newColumn.getDataType()==5){
				newColumn.setPara1(0.5);
				newColumn.setPara2(0.5);
			}
			newColumn.init();   //信息都造完了
			modifyColumns.add(newColumn);

		}

		Column[] tmp1 = new Column[modifyColumns.size()];
		modifyColumns.toArray(tmp1);
		columns = tmp1;
//		System.out.println("Fake Column is finished");

	}

	@Override
	public String toString() {
		return "\n\tTable [name=" + name + ", size=" + size + ", columns=" + Arrays.toString(columns) + ", \n\t\tprimaryKey="
				+ Arrays.toString(primaryKey) + ", \n\t\tforeignKeys=" + Arrays.toString(foreignKeys) + ", \n\t\tpkColumnRanges=" 
				+ Arrays.toString(pkColumnRanges) + ", pkPersistRatio=" + pkPersistRatio + ", pkColumnIndexes="
				+ Arrays.toString(pkColumnIndexes) + ", \n\t\tallFkColumnRanges=" + Arrays.deepToString(allFkColumnRanges)
				+ ", allFkColumnIndexes=" + Arrays.deepToString(allFkColumnIndexes)+ ", \n\t\tnonKeyColumnIndexes=" 
				+ Arrays.toString(nonKeyColumnIndexes) + "]";
	}

	// 测试
	public static void main(String[] args) {
		PropertyConfigurator.configure(".//lib//log4j.properties");
		TableInfoSerializer serializer = new TableInfoSerializer();
		List<Table> tables = serializer.read(new File(".//testdata//tables.obj"));

		for (int i = 0; i < tables.size(); i++) {
			tables.get(i).init(tables);
//			tables.get(i).setThreadNum(3);
//			tables.get(i).setThreadId(2);
//			System.out.println(tables.get(i).getName());
//			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss");
//			for (int j = 0; j < 100; j++) {
//				System.out.println(Arrays.toString(tables.get(i).geneTuple(sdf)));
//			}
//			System.out.println();
		}
		serializer.write(tables, new File(".//testdata//tables2.obj"));
		System.out.println(tables);
	}


	//added by qly
	public static String getRandomString(int length){
		String str="abcdefghijklmnopqrstuvwxyz";
		Random random=new Random();
		StringBuffer sb=new StringBuffer();
		for(int i=0;i<length;i++){
			int number=random.nextInt(26);
			sb.append(str.charAt(number));
		}
		return sb.toString();
	}

	//added by qly
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
