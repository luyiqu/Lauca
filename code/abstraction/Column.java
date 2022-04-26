package abstraction;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;

import config.Configurations;

// Column中的所有成员在生成数据的过程中不会被修改，故Column对象是可以被多线程共享的
public class Column implements Serializable, Cloneable {

	private static final long serialVersionUID = 1L;

	private String name = null;
	// 0: integer(long); 1: real(double); 2: decimal(BigDecimal); 
	// 3: datetime(Date millisecond -- long); 4: varchar; 5: boolean
	private int dataType;
	private double nullRatio;
	private long cardinality;
	
	// integer, real, decimal, datetime: minValue; varchar: avgLength; boolean: trueRatio
	private double para1;
	// integer, real, decimal, datetime: maxValue; varchar: maxLength; boolean: falseRatio
	private double para2;

	private double coefficient;
	
	private int minLength, maxLength;
	private String[] seedStrings = null;


	public Column(String name, int dataType) {
		super();
		this.name = name;
		this.dataType = dataType;
	}

	public void init() {
		if (dataType <= 3) { // integer, real, decimal, datetime
			if (cardinality != 1) {
				coefficient = (para2 - para1) / (cardinality - 1);
			} else {
				coefficient = 0;
			}
		} else if (dataType == 4) { // varchar
			if (para2 / 2 > para1) {
				minLength = 0;
				maxLength = (int)Math.round(para1 * 2);
			} else {
				minLength = (int)Math.round(2 * para1 - para2);
				maxLength = (int)para2;
			}
			seedStrings = new String[Configurations.getSeedStringSize()];
			for (int i = 0; i < seedStrings.length; i++) {
				int randomLength = (int)Math.round(Math.random() * (maxLength - minLength)) + minLength;
				seedStrings[i] = geneString(randomLength);
			}
		}
	}

	private static final char[] chars = 
			("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz").toCharArray();

	private static String geneString(int length) {
		char[] buffer = new char[length];
		for (int i = 0; i < length; i++) {
			buffer[i] = chars[(int)(Math.random() * 62)];
		}
		return new String(buffer);
	}

	// SimpleDateFormat是非线程安全的，故这里的sdf需由数据生成线程传进来，针对日期类型的输出是格式化的字符串
	//这里是生成非主键和外键信息
	public Object geneData(SimpleDateFormat sdf) {
		if (Math.random() < nullRatio)  {
			return null;
		}

		if (dataType <= 4) {
			int randomIndex = (int)(Math.random() * cardinality);
			if (dataType == 0) {
				return (long)(randomIndex * coefficient + para1);
			} else if (dataType == 1 || dataType == 2) {
				return randomIndex * coefficient + para1;
			} else if (dataType == 3) {
				return sdf.format(new Date((long)(randomIndex * coefficient + para1)));
			} else {
				// 这里生成的随机字符串平均长度要大于指定值，多了前面的数字和‘#’号的长度 TODO
				//varchar
//				System.out.println("Column Name: "+name); //打印出哪个column有问题
				String tmp = randomIndex + "#" + seedStrings[randomIndex % seedStrings.length];
				if (tmp.length() > maxLength) {
					return tmp.substring(0, maxLength); // 可能这里生成出来的随机字符串不包含‘#’！
				} else {
					return tmp;
				}
				// return randomIndex + seedStrings[randomIndex % seedStrings.length];
			}
		} else if (dataType == 5) {
			if (Math.random() < para1) {
				return 1;
			} else {
				return 0;
			}
		} else {
			System.err.println("Unrecognized data type!");
			return null;
		}
	}

	public void setName(String name) { this.name = name; }

	public void setNullRatio(double nullRatio) {
		this.nullRatio = nullRatio;
	}

	public void setCardinality(long cardinality) {
		this.cardinality = cardinality;
	}

	public void setPara1(double para1) {
		this.para1 = para1;
	}

	public void setPara2(double para2) {
		this.para2 = para2;
	}

	public String getName() {
		return name;
	}

	public int getDataType() {
		return dataType;
	}

	public double getNullRatio() {
		return nullRatio;
	}

	public long getCardinality() {
		return cardinality;
	}

	public double getPara1() {
		return para1;
	}

	public double getPara2() {
		return para2;
	}

	public double getCoefficient() {
		return coefficient;
	}

	public int getMinLength() {
		return minLength;
	}

	public int getMaxLength() {
		return maxLength;
	}

	public String[] getSeedStrings() {
		return seedStrings;
	}

	@Override
	public String toString() {
		return "\n\t\tColumn [name=" + name + ", dataType=" + dataType + ", nullRatio=" + nullRatio + ", cardinality="
				+ cardinality + ", para1=" + para1 + ", para2=" + para2 + ", coefficient=" + coefficient
				+ ", minLength=" + minLength + ", maxLength=" + maxLength + "]";
	}

	@Override
	public Column clone() {
		try {
			return (Column)super.clone();
		} catch (CloneNotSupportedException e) {
			e.printStackTrace();
		}
		return null;
	}

	// test
	public static void main(String[] args) {
		Column column1 = new Column("age", 0);
		column1.setNullRatio(0.1f);
		column1.setCardinality(50);
		column1.setPara1(0);
		column1.setPara2(100);
		column1.init();
		for (int i = 0; i < 20; i++) {
			System.out.print(column1.geneData(null) + ", ");
		}
		System.out.println();

		Column column2 = new Column("salary", 2);
		column2.setNullRatio(0.05f);
		column2.setCardinality(10);
		column2.setPara1(2000);
		column2.setPara2(30000);
		column2.init();
		for (int i = 0; i < 20; i++) {
			System.out.print(column2.geneData(null) + ", ");
		}
		System.out.println();

		Column column3 = new Column("name", 4);
		column3.setNullRatio(0.2f);
		column3.setCardinality(5);
		column3.setPara1(10);
		column3.setPara2(50);
		column3.init();
		for (int i = 0; i < 20; i++) {
			System.out.print(column3.geneData(null) + ", ");
		}
		System.out.println();

		Column column4 = new Column("date", 3);
		column4.setNullRatio(0.15f);
		column4.setCardinality(20);
		column4.setPara1(10);
		column4.setPara2(2598522548d);
		column4.init();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss");
		for (int i = 0; i < 20; i++) {
			System.out.print(column4.geneData(sdf) + ", ");
		}
	}
}
