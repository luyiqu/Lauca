package config;

public class Configurations {

	private static String log4jConfigFile = ".//lib//log4j.properties";

	private static String databaseSchemaFile = null;
	private static boolean useTidbLog  = false;
	private static String tidbLogFile  = null;
	private static String laucaLogDir = "";
	private static String transactionTemplatesFile = null;
	private static String ddlFile = null;

	// 这两个都是文件夹，文件名格式为：lauca.log.num，序号（即num）越大时间越靠前
	private static String runningLogDir4TxLogic = ".//runningLog4TxLogic";
	private static String runningLogDir4Distribution = ".//runningLog4Distribution";

	// 数据库种类，目前支持MySQL，PostgreSQL，Oracle
	private static String databaseType = "MySQL";

	// 真实数据库的相关信息，用来获取数据特征信息
	private static String originalDatabaseIp = null;
	private static String originalDatabasePort = null;
	private static String originalDatabaseName = null;
	private static String originalDatabaseUserName = null;
	private static String originalDatabasePasswd = null;

	// 模拟数据库（即Lauca生成的数据库）的相关信息
	private static String laucaDatabaseIp = null;
	private static String laucaDatabasePort = null;
	private static String laucaDatabaseName = null;
	private static String laucaDatabaseUserName = null;
	private static String laucaDatabasePasswd = null;

	private static int seedStringSize = 1000;

	// 模拟数据库的原始数据（Lauca生成的）路径
	private static String laucaTablesDir = ".//syntheticDatabase";

	// 作为数据生成器，当前节点的id（从0开始计数）
	private static int machineId = 0;
	// 数据生成器总的部署节点数目
	private static int machineNum = 1;
	// 每个节点上的数据生成线程数（这里假设每个节点上的数据生成数都是一样的）
	private static int singleMachineThreadNum = 8;

	// 保存数据特征的中间状态文件
	private static String dataCharacteristicSaveFile = "D://dataCharacteristicSaveFile.obj";
	// 保存负载特征的中间状态文件
//	private static String workloadCharacteristicSaveFile  = ".//saveFiles//workloadCharacteristicSaveFile .obj";
	// 保存事务逻辑的中间状态文件
	private static String txLogicSaveFile = ".//saveFiles//txLogicSaveFile.obj";
	// 保存数据访问分布的中间状态文件
	private static String distributionSaveFile = ".//saveFiles//distributionSaveFile.obj";

	//保存存储过程文件
	private static String storedProcedureSaveFile = ".//storedProcedure.txt";
	// 0：整个负载周期的数据访问分布；1：基于时间窗口的数据访问分布；2：基于连续时间窗口的数据访问分布；
	// 3：多参数的关联数据访问分布；-1：没有数据分布，仅根据属性生成函数生成SQL参数
	private static int distributionCategory = 2; // 目前这里仅能设置1和2

	//是否获取skywalking捕获的负载（反之，直接从应用端打印）
	private static boolean useSkywalking = false;

	//是否自动从真实数据库中读取Schema，针对TIDB,目前还需要从ddl中读取，因为TIDB数据库中没有外键，需要再ddl中定死
	private static boolean useAutoSchemaReader = false;
	// 若在做实验时，想仅根据全负载周期的数据访问分布生成参数，可将下面的参数设置为true
	private static boolean expFullLifeCycleDist = false;

	// 完全随机生成参数，即生成的参数在阈值内符合均匀分布
	private static boolean expUniformPara = false;

	// 在统计全局数据分布时，采样数据的数据量大小
	private static int samplingSize = 1000000;

	// 统计数据访问分布时的时间窗口大小，单位为秒
	private static int timeWindowSize = 1;
	// 统计数据访问分布的线程数
	private static int statThreadNum = 8;

	private static int maxSizeOfTxDataList = 100000;
	private static int highFrequencyItemNum = 20;
	private static int intervalNum = 50; // 直方图的分段数

	// 事务逻辑控制参数，为真表示包含该类型事务逻辑，为假表示不包含该类型事务逻辑
	private static boolean equalRelationFlag = true;
	private static boolean includeRelationFlag = true;
	private static boolean linearRelationFlag = true;
	// 结构信息（循环平均执行次数和分支执行比例）必然会被统计，不受该配置项影响
	private static boolean multipleLogicFlag = true;

	// bug fix: 针对decimal和varchar数据类型，lauca的输入中没有指定约束参数（如p,s,n），故可能出现
	// 一个阈值小的decimal在一个极小的概率上与一个前面阈值较大的decimal满足等于关系或其他关系；而对于字符型参
	// 数来说可能就是长度不匹配的问题。这里我们通过对小概率关系的去除来规避这个问题~
	// 比如可规避错误 "Data truncation: Out of range value for column 'OL_QUANTITY'"
	// see Util.java
	private static double minProbability = 0.01;

	// 支持线性依赖关系分析的最小事务实例数据量，不能特别小，建议100以上
	private static int minTxDataSize4LinearRelation = 100;
	// 随机事务实例对数，因为一组线性系数的计算至少需要一对事务实例。建议设置得稍微大点，比如10000
	private static int randomPairs = 10000;

	// 评测时间，单位为秒（一般设置的值会小于运行日志的时长）
	private static int testTimeLength = 100;

	// 负载生成线程的个数（所有的测试线程数 以及 当前JVM中的测试线程数）
	private static int allTestThreadNum = 20;
	private static int localTestThreadNum = 20;

	// 负载加载类型。0：不控制吞吐，以最大吞吐压（while，无阻塞）；1：按照指定吞吐量加载负载（利用定时器控制）
	private static int loadingType = 0;
	// 吞吐扩展因子：测试负载吞吐与实际负载吞吐的比值
	private static double throughputScaleFactor = 1;
	// 以固定吞吐加载负载，下面值为-1时即忽视
	private static int fixedThroughput = 100;

	// 数据库隔离级别。0：冲突可串行化；1：可重复读；2：读已提交；3：读未提交
	private static int transactionIsolation = 0;

	// 性能统计时间窗口，一般建议设置为1s或者2s
	private static int statWindowSize = 1;

	// 启用rollbackProbabilities
	private static boolean enableRollbackProbability = false;

	//启用匿名化
	private static boolean enableAnonymity = false;

	//增加新列的比例
	private static double fakeColumnRate = 0.0;

	//扩增or缩小数据集的比例
	private static double dataIncreaseRate = 0.0;

	//保存匿名化对应关系的中间状态文件
	private static String anonymitySaveFile = "";

	//启用存储过程
	private static boolean useStoredProcedure = true;

	public static String getLog4jConfigFile() {
		return log4jConfigFile;
	}

	public static void setLog4jConfigFile(String log4jConfigFile) {
		Configurations.log4jConfigFile = log4jConfigFile;
	}

	public static String getDatabaseSchemaFile() {
		return databaseSchemaFile;
	}

	public static void setDatabaseSchemaFile(String databaseSchemaFile) {
		Configurations.databaseSchemaFile = databaseSchemaFile;
	}

	public static String getTransactionTemplatesFile() {
		return transactionTemplatesFile;
	}

	public static void setTransactionTemplatesFile(String transactionTemplatesFile) {
		Configurations.transactionTemplatesFile = transactionTemplatesFile;
	}

	public static String getRunningLogDir4TxLogic() {
		return runningLogDir4TxLogic;
	}

	public static void setRunningLogDir4TxLogic(String runningLogDir4TxLogic) {
		Configurations.runningLogDir4TxLogic = runningLogDir4TxLogic;
	}

	public static String getRunningLogDir4Distribution() {
		return runningLogDir4Distribution;
	}

	public static void setRunningLogDir4Distribution(String runningLogDir4Distribution) {
		Configurations.runningLogDir4Distribution = runningLogDir4Distribution;
	}

	public static String getDatabaseType() {
		return databaseType;
	}

	public static void setDatabaseType(String databaseType) {
		Configurations.databaseType = databaseType;
	}

	public static String getOriginalDatabaseIp() {
		return originalDatabaseIp;
	}

	public static void setOriginalDatabaseIp(String originalDatabaseIp) {
		Configurations.originalDatabaseIp = originalDatabaseIp;
	}

	public static String getOriginalDatabasePort() {
		return originalDatabasePort;
	}

	public static void setOriginalDatabasePort(String originalDatabasePort) {
		Configurations.originalDatabasePort = originalDatabasePort;
	}

	public static String getOriginalDatabaseName() {
		return originalDatabaseName;
	}

	public static void setOriginalDatabaseName(String originalDatabaseName) {
		Configurations.originalDatabaseName = originalDatabaseName;
	}

	public static String getOriginalDatabaseUserName() {
		return originalDatabaseUserName;
	}

	public static void setOriginalDatabaseUserName(String originalDatabaseUserName) {
		Configurations.originalDatabaseUserName = originalDatabaseUserName;
	}

	public static String getOriginalDatabasePasswd() {
		return originalDatabasePasswd;
	}

	public static void setOriginalDatabasePasswd(String originalDatabasePasswd) {
		Configurations.originalDatabasePasswd = originalDatabasePasswd;
	}

	public static String getLaucaDatabaseIp() {
		return laucaDatabaseIp;
	}

	public static void setLaucaDatabaseIp(String laucaDatabaseIp) {
		Configurations.laucaDatabaseIp = laucaDatabaseIp;
	}

	public static String getLaucaDatabasePort() {
		return laucaDatabasePort;
	}

	public static void setLaucaDatabasePort(String laucaDatabasePort) {
		Configurations.laucaDatabasePort = laucaDatabasePort;
	}

	public static String getLaucaDatabaseName() {
		return laucaDatabaseName;
	}

	public static void setLaucaDatabaseName(String laucaDatabaseName) {
		Configurations.laucaDatabaseName = laucaDatabaseName;
	}

	public static String getLaucaDatabaseUserName() {
		return laucaDatabaseUserName;
	}

	public static void setLaucaDatabaseUserName(String laucaDatabaseUserName) {
		Configurations.laucaDatabaseUserName = laucaDatabaseUserName;
	}

	public static String getLaucaDatabasePasswd() {
		return laucaDatabasePasswd;
	}

	public static void setLaucaDatabasePasswd(String laucaDatabasePasswd) {
		Configurations.laucaDatabasePasswd = laucaDatabasePasswd;
	}

	public static int getSeedStringSize() {
		return seedStringSize;
	}

	public static void setSeedStringSize(int seedStringSize) {
		Configurations.seedStringSize = seedStringSize;
	}

	public static String getLaucaTablesDir() {
		return laucaTablesDir;
	}

	public static void setLaucaTablesDir(String laucaTablesDir) {
		Configurations.laucaTablesDir = laucaTablesDir;
	}

	public static int getMachineId() {
		return machineId;
	}

	public static void setMachineId(int machineId) {
		Configurations.machineId = machineId;
	}

	public static int getMachineNum() {
		return machineNum;
	}

	public static void setMachineNum(int machineNum) {
		Configurations.machineNum = machineNum;
	}

	public static int getSingleMachineThreadNum() {
		return singleMachineThreadNum;
	}

	public static void setSingleMachineThreadNum(int singleMachineThreadNum) {
		Configurations.singleMachineThreadNum = singleMachineThreadNum;
	}

	public static String getDataCharacteristicSaveFile() {
		return dataCharacteristicSaveFile;
	}

	public static void setDataCharacteristicSaveFile(String dataCharacteristicSaveFile) {
		Configurations.dataCharacteristicSaveFile = dataCharacteristicSaveFile;
	}

	public static String getAnonymitySaveFile() { return anonymitySaveFile; }

	public static void setAnonymitySaveFile(String anonymitySaveFile) {
		Configurations.anonymitySaveFile = anonymitySaveFile;
	}

	public static String getTxLogicSaveFile() {
		return txLogicSaveFile;
	}

	public static void setTxLogicSaveFile(String txLogicSaveFile) {
		Configurations.txLogicSaveFile = txLogicSaveFile;
	}

	public static String getDistributionSaveFile() {
		return distributionSaveFile;
	}

	public static void setDistributionSaveFile(String distributionSaveFile) {
		Configurations.distributionSaveFile = distributionSaveFile;
	}

	public static void setStoredProcedureSaveFile(String storedProcedureSaveFile) {
		Configurations.storedProcedureSaveFile = storedProcedureSaveFile;
	}

	public static String getStoredProcedureSaveFile() {
		return storedProcedureSaveFile;
	}

	public static void setUseAutoSchemaReader(boolean useAutoSchemaReader) {
		Configurations.useAutoSchemaReader = useAutoSchemaReader;
	}



	public static boolean isUseAutoSchemaReader() {
		return useAutoSchemaReader;
	}

	public static void setUseSkywalking(boolean useSkywalking) {
		Configurations.useSkywalking = useSkywalking;
	}

	public static boolean isUseSkywalking() {
		return useSkywalking;
	}


	public static int getDistributionCategory() {
		return distributionCategory;
	}

//	public static String getWorkloadCharacteristicSaveFile() {
//		return workloadCharacteristicSaveFile;
//	}
//
//	public static void setWorkloadCharacteristicSaveFile(String workloadCharacteristicSaveFile) {
//		Configurations.workloadCharacteristicSaveFile = workloadCharacteristicSaveFile;
//	}

	public static void setDistributionCategory(int distributionCategory) {
		Configurations.distributionCategory = distributionCategory;
	}

	public static boolean isExpFullLifeCycleDist() {
		return expFullLifeCycleDist;
	}

	public static void setExpFullLifeCycleDist(boolean expFullLifeCycleDist) {
		Configurations.expFullLifeCycleDist = expFullLifeCycleDist;
	}

	public static boolean isExpUniformPara() {
		return expUniformPara;
	}

	public static void setExpUniformPara(boolean expUniformPara) {
		Configurations.expUniformPara = expUniformPara;
	}

	public static int getSamplingSize() {
		return samplingSize;
	}

	public static void setSamplingSize(int samplingSize) {
		Configurations.samplingSize = samplingSize;
	}

	public static int getTimeWindowSize() {
		return timeWindowSize;
	}

	public static void setTimeWindowSize(int timeWindowSize) {
		Configurations.timeWindowSize = timeWindowSize;
	}

	public static int getStatThreadNum() {
		return statThreadNum;
	}

	public static void setStatThreadNum(int statThreadNum) {
		Configurations.statThreadNum = statThreadNum;
	}

	public static int getMaxSizeOfTxDataList() {
		return maxSizeOfTxDataList;
	}

	public static void setMaxSizeOfTxDataList(int maxSizeOfTxDataList) {
		Configurations.maxSizeOfTxDataList = maxSizeOfTxDataList;
	}

	public static int getHighFrequencyItemNum() {
		return highFrequencyItemNum;
	}

	public static void setHighFrequencyItemNum(int highFrequencyItemNum) {
		Configurations.highFrequencyItemNum = highFrequencyItemNum;
	}

	public static int getIntervalNum() {
		return intervalNum;
	}

	public static void setIntervalNum(int intervalNum) {
		Configurations.intervalNum = intervalNum;
	}

	public static boolean isEqualRelationFlag() {
		return equalRelationFlag;
	}

	public static void setEqualRelationFlag(boolean equalRelationFlag) {
		Configurations.equalRelationFlag = equalRelationFlag;
	}

	public static boolean isIncludeRelationFlag() {
		return includeRelationFlag;
	}

	public static void setIncludeRelationFlag(boolean includeRelationFlag) {
		Configurations.includeRelationFlag = includeRelationFlag;
	}

	public static boolean isLinearRelationFlag() {
		return linearRelationFlag;
	}

	public static void setLinearRelationFlag(boolean linearRelationFlag) {
		Configurations.linearRelationFlag = linearRelationFlag;
	}

	public static boolean isMultipleLogicFlag() {
		return multipleLogicFlag;
	}

	public static void setMultipleLogicFlag(boolean multipleLogicFlag) {
		Configurations.multipleLogicFlag = multipleLogicFlag;
	}

	public static double getMinProbability() {
		return minProbability;
	}

	public static void setMinProbability(double minProbability) {
		Configurations.minProbability = minProbability;
	}

	public static int getMinTxDataSize4LinearRelation() {
		return minTxDataSize4LinearRelation;
	}

	public static void setMinTxDataSize4LinearRelation(int minTxDataSize4LinearRelation) {
		Configurations.minTxDataSize4LinearRelation = minTxDataSize4LinearRelation;
	}

	public static int getRandomPairs() {
		return randomPairs;
	}

	public static void setRandomPairs(int randomPairs) {
		Configurations.randomPairs = randomPairs;
	}

	public static int getTestTimeLength() {
		return testTimeLength;
	}

	public static void setTestTimeLength(int testTimeLength) {
		Configurations.testTimeLength = testTimeLength;
	}

	public static int getAllTestThreadNum() {
		return allTestThreadNum;
	}

	public static void setAllTestThreadNum(int allTestThreadNum) {
		Configurations.allTestThreadNum = allTestThreadNum;
	}

	public static int getLocalTestThreadNum() {
		return localTestThreadNum;
	}

	public static void setLocalTestThreadNum(int localTestThreadNum) {
		Configurations.localTestThreadNum = localTestThreadNum;
	}

	public static int getLoadingType() {
		return loadingType;
	}

	public static void setLoadingType(int loadingType) {
		Configurations.loadingType = loadingType;
	}

	public static double getThroughputScaleFactor() {
		return throughputScaleFactor;
	}

	public static void setThroughputScaleFactor(double throughputScaleFactor) {
		Configurations.throughputScaleFactor = throughputScaleFactor;
	}

	public static int getFixedThroughput() {
		return fixedThroughput;
	}

	public static void setFixedThroughput(int fixedThroughput) {
		Configurations.fixedThroughput = fixedThroughput;
	}

	public static int getTransactionIsolation() {
		return transactionIsolation;
	}

	public static void setTransactionIsolation(int transactionIsolation) {
		Configurations.transactionIsolation = transactionIsolation;
	}

	public static int getStatWindowSize() {
		return statWindowSize;
	}

	public static void setStatWindowSize(int statWindowSize) {
		Configurations.statWindowSize = statWindowSize;
	}

	public static boolean isEnableRollbackProbability() {
		return enableRollbackProbability;
	}

	public static void setEnableRollbackProbability(boolean enableRollbackProbability) {
		Configurations.enableRollbackProbability = enableRollbackProbability;
	}

	public static void setDdlFile(String ddlFile) {
		Configurations.ddlFile = ddlFile;
	}

	public static String getDdlFile() {
		return ddlFile;
	}

	public static boolean isUseTidbLog() {
		return useTidbLog;
	}

	public static void setUseTidbLog(boolean useTidbLog) {
		Configurations.useTidbLog = useTidbLog;
	}

	public static String getTidbLogFile() {
		return tidbLogFile;
	}

	public static void setTidbLogFile(String tidbLogFile) {
		Configurations.tidbLogFile = tidbLogFile;
	}

	public static String getLaucaLogDir() {
		return laucaLogDir;
	}

	public static void setLaucaLogDir(String laucaLogDir) {
		Configurations.laucaLogDir = laucaLogDir;
	}

	public static boolean isEnableAnonymity() {return enableAnonymity;}

	public static void setEnableAnonymity(boolean enableAnonymity){ Configurations.enableAnonymity = enableAnonymity;}

	public static boolean isUseStoredProcedure(){return useStoredProcedure;}

	public static void setUseStoredProcedure(boolean useStoredProcedure) {
		Configurations.useStoredProcedure = useStoredProcedure;
	}

	public static void setFakeColumnRate(Double fakeColumnRate){ Configurations.fakeColumnRate = fakeColumnRate;}

	public static Double getFakeColumnRate(){return fakeColumnRate;}

	public static void setDataIncreaseRate(Double dataIncreaseRate){ Configurations.dataIncreaseRate = dataIncreaseRate;}

	public static Double getDataIncreaseRate(){return dataIncreaseRate;}
}
