package config;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

public class ConfigurationsReader {

	public static void read(File configFile) {
		try (BufferedReader br = new BufferedReader(new InputStreamReader(Files.newInputStream(configFile.toPath()), StandardCharsets.UTF_8))) {
			String inputLine = null;
			while ((inputLine = br.readLine()) != null) {
				if (inputLine.matches("[\\s]*") || inputLine.matches("[ ]*##[\\s\\S]*")) {
					continue;
				}

				String[] arr = inputLine.split("=");
				switch (arr[0].trim()) {
				case "log4jConfigFile":
					Configurations.setLog4jConfigFile(arr[1].trim());
					break;
				case "useTidbLog":
					Configurations.setUseTidbLog(Boolean.parseBoolean(arr[1].trim()));
					break;
				case "tidbLogFile":
					Configurations.setTidbLogFile(arr[1].trim());
					break;
				case "laucaLogDir":
					Configurations.setLaucaLogDir(arr[1].trim());
					break;
				case "databaseSchemaFile":
					Configurations.setDatabaseSchemaFile(arr[1].trim());
					break;
				case "transactionTemplatesFile":
					Configurations.setTransactionTemplatesFile(arr[1].trim());
					break;
				case "ddlFile":
					Configurations.setDdlFile(arr[1].trim());
					break;
				case "runningLogDir4TxLogic":
					Configurations.setRunningLogDir4TxLogic(arr[1].trim());
					break;
				case "runningLogDir4Distribution":
					Configurations.setRunningLogDir4Distribution(arr[1].trim());
					break;
				case "databaseType":
					Configurations.setDatabaseType(arr[1].trim());
					break;
				case "originalDatabaseIp":
					Configurations.setOriginalDatabaseIp(arr[1].trim());
					break;
				case "originalDatabasePort":
					Configurations.setOriginalDatabasePort(arr[1].trim());
					break;
				case "originalDatabaseName":
					Configurations.setOriginalDatabaseName(arr[1].trim());
					break;
				case "originalDatabaseUserName":
					Configurations.setOriginalDatabaseUserName(arr[1].trim());
					break;
				case "originalDatabasePasswd":
					Configurations.setOriginalDatabasePasswd(arr[1].trim());
					break;
				case "laucaDatabaseIp":
					Configurations.setLaucaDatabaseIp(arr[1].trim());
					break;
				case "laucaDatabasePort":
					Configurations.setLaucaDatabasePort(arr[1].trim());
					break;
				case "laucaDatabaseName":
					Configurations.setLaucaDatabaseName(arr[1].trim());
					break;
				case "laucaDatabaseUserName":
					Configurations.setLaucaDatabaseUserName(arr[1].trim());
					break;
				case "laucaDatabasePasswd":
					Configurations.setLaucaDatabasePasswd(arr[1].trim());
					break;
				case "seedStringSize":
					Configurations.setSeedStringSize(Integer.parseInt(arr[1].trim()));
					break;
				case "laucaTablesDir":
					Configurations.setLaucaTablesDir(arr[1].trim());
					break;
				case "machineId":
					Configurations.setMachineId(Integer.parseInt(arr[1].trim()));
					break;
				case "machineNum":
					Configurations.setMachineNum(Integer.parseInt(arr[1].trim()));
					break;
				case "singleMachineThreadNum":
					Configurations.setSingleMachineThreadNum(Integer.parseInt(arr[1].trim()));
					break;
				case "dataCharacteristicSaveFile":
					Configurations.setDataCharacteristicSaveFile(arr[1].trim());
					break;
				case "txLogicSaveFile":
					Configurations.setTxLogicSaveFile(arr[1].trim());
					break;
				case "distributionSaveFile":
					Configurations.setDistributionSaveFile(arr[1].trim());
					break;
//				case "workloadCharacteristicSaveFile":
//					Configurations.setWorkloadCharacteristicSaveFile(arr[1].trim());
//					break;
				case "distributionCategory":
					Configurations.setDistributionCategory(Integer.parseInt(arr[1].trim()));
					break;
				case "expFullLifeCycleDist":
					Configurations.setExpFullLifeCycleDist(Boolean.parseBoolean(arr[1].trim()));
					break;
				case "expUniformPara":
					Configurations.setExpUniformPara(Boolean.parseBoolean(arr[1].trim()));
					break;
				case "samplingSize":
					Configurations.setSamplingSize(Integer.parseInt(arr[1].trim()));
					break;
				case "timeWindowSize":
					Configurations.setTimeWindowSize(Integer.parseInt(arr[1].trim()));
					break;
				case "statThreadNum":
					Configurations.setStatThreadNum(Integer.parseInt(arr[1].trim()));
					break;
				case "maxSizeOfTxDataList":
					Configurations.setMaxSizeOfTxDataList(Integer.parseInt(arr[1].trim()));
					break;
				case "highFrequencyItemNum":
					Configurations.setHighFrequencyItemNum(Integer.parseInt(arr[1].trim()));
					break;
				case "intervalNum":
					Configurations.setIntervalNum(Integer.parseInt(arr[1].trim()));
					break;
				case "equalRelationFlag":
					Configurations.setEqualRelationFlag(Boolean.parseBoolean(arr[1].trim()));
					break;
				case "includeRelationFlag":
					Configurations.setIncludeRelationFlag(Boolean.parseBoolean(arr[1].trim()));
					break;
				case "linearRelationFlag":
					Configurations.setLinearRelationFlag(Boolean.parseBoolean(arr[1].trim()));
					break;
				case "multipleLogicFlag":
					Configurations.setMultipleLogicFlag(Boolean.parseBoolean(arr[1].trim()));
					break;
				case "minProbability":
					Configurations.setMinProbability(Double.parseDouble(arr[1].trim()));
					break;
				case "minTxDataSize4LinearRelation":
					Configurations.setMinTxDataSize4LinearRelation(Integer.parseInt(arr[1].trim()));
					break;
				case "randomPairs":
					Configurations.setRandomPairs(Integer.parseInt(arr[1].trim()));
					break;
				case "testTimeLength":
					Configurations.setTestTimeLength(Integer.parseInt(arr[1].trim()));
					break;
				case "allTestThreadNum":
					Configurations.setAllTestThreadNum(Integer.parseInt(arr[1].trim()));
					break;
				case "localTestThreadNum":
					Configurations.setLocalTestThreadNum(Integer.parseInt(arr[1].trim()));
					break;
				case "loadingType":
					Configurations.setLoadingType(Integer.parseInt(arr[1].trim()));
					break;
				case "throughputScaleFactor":
					Configurations.setThroughputScaleFactor(Integer.parseInt(arr[1].trim()));
					break;
				case "fixedThroughput":
					Configurations.setFixedThroughput(Integer.parseInt(arr[1].trim()));
					break;
				case "transactionIsolation":
					Configurations.setTransactionIsolation(Integer.parseInt(arr[1].trim()));
					break;
				case "statWindowSize":
					Configurations.setStatWindowSize(Integer.parseInt(arr[1].trim()));
					break;
				case "enableRollbackProbability":
					Configurations.setEnableRollbackProbability(Boolean.parseBoolean(arr[1].trim()));
					break;
				case "anonymitySaveFile":
					Configurations.setAnonymitySaveFile(arr[1].trim());
					break;
				case "enableAnonymity":
					Configurations.setEnableAnonymity(Boolean.parseBoolean(arr[1].trim()));
					break;
				case "fakeColumnRate":
					Configurations.setFakeColumnRate(Double.parseDouble(arr[1].trim()));
					break;
				case "dataIncreaseRate":
					Configurations.setDataIncreaseRate(Double.parseDouble(arr[1].trim()));
					break;
				case "useStoredProcedure":
					Configurations.setUseStoredProcedure(Boolean.parseBoolean(arr[1].trim()));
					break;
				case "storedProcedureFile":
					Configurations.setStoredProcedureSaveFile(arr[1].trim());
					break;
				case "useAutoSchemaReader":
					Configurations.setUseAutoSchemaReader(Boolean.parseBoolean(arr[1].trim()));
					break;
				case "useSkywalking":
					Configurations.setUseSkywalking(Boolean.parseBoolean(arr[1].trim()));
					break;
				case "backwardLength":
					Configurations.setBackwardLength(Integer.parseInt(arr[1].trim()));
					break;
				case "quantileNum":
					Configurations.setQuantileNum(Integer.parseInt(arr[1].trim()));
					break;
				case "usePartitionRule":
					Configurations.setUsePartitionRule(Boolean.parseBoolean(arr[1].trim()));
					break;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		ConfigurationsReader.read(new File(".//testData//lauca.conf"));
		System.out.println(Configurations.isExpUniformPara());
	}
}
