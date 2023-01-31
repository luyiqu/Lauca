package transactionlogic;

import abstraction.Partition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PartitionEqualRelationAnalyzer {

    /**
     * @param txDataList: 一个事务（同一个事务模板）的所有运行数据（多个事务实例数据）
     * @return para2ParaOrResult2EqualCounter：等于依赖关系的计数器
     * @function: 统计事务中参数以及返回结果集元素之间的等于依赖关系。目前假设参数之间是相互独立的，暂不考虑多参数的组合逻辑关系。
     *     具体策略是依次统计事务中所有操作的输入参数与前面操作的输入参数和返回结果集元素以及当前操作前面的输入参数之间的等于关联关系。
     *     针对Multiple内部操作的多次执行数据，这里仅取其第一次执行的数据供统计之用，其多次执行输入参数之间的关系会进行独立分析。
     *     参数的等于关联（依赖）关系对于资源竞争强度，死锁发生的可能性和分布式事务的比例有很大的影响，是事务逻辑的重要组成部分。
     */
    @SuppressWarnings("unchecked")
    public Map<String, Map<String, Integer>> countPartitionEqualInfo(List<TransactionData> txDataList, Map<Integer, List<Partition>> opId2Partition, Map<Integer, List<String>> opId2paraSchema) {
        // 等于依赖关系的计数器
        // 数据结构解释：当前参数的标识符 -> Map(前一个参数或返回结果集元素的标识符 -> 相等的次数，即计数器)
        // operationId_"para"_paraIndex -> Map(operationId_"para"_paraIndex -> counter)
        Map<String, Map<String, Integer>> para2Para2PartitionEqualCounter = new HashMap<>();

        for (TransactionData txData : txDataList) {
            int[] operationTypes = txData.getOperationTypes();
            Object[] operationDatas = txData.getOperationDatas();

            for (int i = 0; i < operationDatas.length; i++) {

                OperationData operationData = null;
                if (operationTypes[i] == -1) {
                    continue;
                } else if (operationTypes[i] == 1) {
                    // 对于multiple中操作的多次运行数据，这里只取其第一次运行的数据作统计分析，同时假设multiple中操作至少运行一次
                    //TODO: 只取第一次运行的数据分析应该是没问题的，但是在生成中却不能如此，还是要讲究这是第几次循环。20210102
                    //TODO: 一般情况下，依赖只在本次循环中发生，这里不考虑第三次循环中某个参数依赖第二次循环中某个参数的情况，且这两个参数属于不同参数。
                    operationData = ((ArrayList<OperationData>)operationDatas[i]).get(0);
                } else if (operationTypes[i] == 0) {
                    operationData = (OperationData)operationDatas[i];
                }

                int operationId = operationData.getOperationId();
                int[] paraDataTypes = operationData.getParaDataTypes();
                Object[] parameters = operationData.getParameters();

                // 针对每个参数进行分析，统计其与 前面操作中的参数和返回结果集元素 以及 当前操作中前面的参数 之间的等于关联关系

                // ------ 统计当前参数与前面输入参数（位于不同SQL中）之间的等于关联关系 ------
                for (int j = 0; j < parameters.length; j++) {

                    // 当前操作的输入参数
                    Object parameter = parameters[j];
                    int dataType = paraDataTypes[j];
                    String paraIdentifier = operationId + "_para_" + j; // 当前参数的标识符
                    Partition partition = opId2Partition.get(operationId).get(j); // 当前参数的分区规则，可能为null
                    String paraSchema = opId2paraSchema.get(operationId).get(j); // 当前参数的schema信息，表示为 tableName"PARA_SCHEMA_SEPARATOR"columnName

                    if (!para2Para2PartitionEqualCounter.containsKey(paraIdentifier)) {
                        para2Para2PartitionEqualCounter.put(paraIdentifier, new HashMap<>());
                    }

                    if (partition == null){
                        continue;
                    }

                    String partitionKey = partition.getPartition((Number)parameter);

                    // 针对当前参数 前面操作的数据 依次进行分析
                    for (int k = 0; k < i; k++) {
                        OperationData frontOperationData = null;
                        if (operationTypes[k] == -1) {
                            continue;
                        } else if (operationTypes[k] == 1) {
                            frontOperationData = ((ArrayList<OperationData>)operationDatas[k]).get(0);
                        } else if (operationTypes[k] == 0) {
                            frontOperationData = (OperationData)operationDatas[k];
                        }
                        int frontOperationId = frontOperationData.getOperationId();

                        // 暂不考虑------ 统计与前面操作返回结果集元素之间的等于关联关系 ------
//                        // 只有当返回结果集仅有一个tuple时，才需判断Equal关系。当返回结果集是多个tuple时，需判断的是Include关系
//                        if (frontOperationData.isFilterPrimaryKey()) {
//                            Object[] returnItems = frontOperationData.getReturnItems();
//                            if (returnItems != null) {
//                                // 针对返回结果tuple中的每一个属性依次进行判断是否相等
//                                int[] frontReturnDataTypes = frontOperationData.getReturnDataTypes();
//                                for (int m = 0; m < returnItems.length; m++) {
//                                    String frontResultIdentifier = frontOperationId + "_result_" + m;
//
//                                    addParaIfEqual(para2Para2PartitionEqualCounter, parameter, dataType, paraIdentifier, returnItems[m], frontReturnDataTypes[m], frontResultIdentifier);
//                                } // for returnItems
//                            } // returnItems != null
//                        } // only one tuple

                        // ------ 统计与前面操作输入参数之间的等于关联关系 ------
                        int[] frontParaDataTypes = frontOperationData.getParaDataTypes();
                        Object[] frontParameters = frontOperationData.getParameters();
                        for (int m = 0; m < frontParameters.length; m++) {
                            String frontParaIdentifier = frontOperationId + "_para_" + m;

                            Partition frontPartition = opId2Partition.get(frontOperationId).get(m);
                            if (frontPartition == null) continue;
                            String frontPartitionKey = frontPartition.getPartition((Number)frontParameters[m]);

                            String frontParaSchema = opId2paraSchema.get(frontOperationId).get(m);
                            addParaIfPartitionEqual(para2Para2PartitionEqualCounter, partitionKey, paraSchema, paraIdentifier, frontPartitionKey, frontParaSchema, frontParaIdentifier);
                        }

                    } // 针对当前参数前面操作数据(不同SQL)的遍历

                    // ------ 统计当前参数与当前操作中前面输入参数（位于同一个SQL中）之间的等于关联关系 ------
                    for (int k = 0; k < j; k++) {
                        String frontParaIdentifier = operationId + "_para_" + k;

                        Partition frontPartition = opId2Partition.get(operationId).get(k);
                        if (frontPartition == null) continue;
                        String frontPartitionKey = frontPartition.getPartition((Number)parameters[k]);

                        String frontParaSchema = opId2paraSchema.get(operationId).get(k);
                        addParaIfPartitionEqual(para2Para2PartitionEqualCounter, partitionKey, paraSchema, paraIdentifier, frontPartitionKey, frontParaSchema, frontParaIdentifier);
                    }

                } // 针对当前操作中所有输入参数的遍历
            } // 针对当前事务中所有操作的遍历
        } // 针对一类事务中所有事务实例数据 的遍历

//		System.out.println("EqualRelationAnalyzer.countEqualInfo -> para2ParaOrResult2EqualCounter: \n\t"
//				+ para2ParaOrResult2EqualCounter);
        return para2Para2PartitionEqualCounter;
    }

    /**
     * 检查当前参数与之前的参数是否相等，如果是，更新对应的计数器
     *
     * @param para2Para2PartitionEqualCounter 参数相等的计数器
     * @param partitionKey                    分区
     * @param paraSchema                      参数的schema
     * @param paraIdentifier                  参数的标识符
     * @param frontPartitionKey               之前的参数的分区
     * @param frontParaSchema 之前的参数的schema
     * @param frontIdentifier                 之前参数的标识符
     */
    private void addParaIfPartitionEqual(Map<String, Map<String, Integer>> para2Para2PartitionEqualCounter, String partitionKey,
                                         String paraSchema, String paraIdentifier, String frontPartitionKey, String frontParaSchema, String frontIdentifier) {


        if (paraSchema.equals(frontParaSchema) && partitionKey.equals(frontPartitionKey)) {

            if (!para2Para2PartitionEqualCounter.get(paraIdentifier).containsKey(frontIdentifier)) {
                para2Para2PartitionEqualCounter.get(paraIdentifier).put(frontIdentifier, 1);
            } else {
                int tmp = para2Para2PartitionEqualCounter.get(paraIdentifier).get(frontIdentifier);
                para2Para2PartitionEqualCounter.get(paraIdentifier).put(frontIdentifier, tmp + 1);
            }
        }
    }
}
