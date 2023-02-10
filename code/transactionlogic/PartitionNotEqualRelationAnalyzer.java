package transactionlogic;

import abstraction.Partition;

import java.util.*;

public class PartitionNotEqualRelationAnalyzer {

    /**
     * @param txDataList: 一个事务（同一个事务模板）的所有运行数据（多个事务实例数据）
     * @return para2ParaOrResult2EqualCounter：等于依赖关系的计数器
     * @function: 统计事务中参数以及返回结果集元素之间的等于依赖关系。目前假设参数之间是相互独立的，暂不考虑多参数的组合逻辑关系。
     *     具体策略是依次统计事务中所有操作的输入参数与前面操作的输入参数和返回结果集元素以及当前操作前面的输入参数之间的等于关联关系。
     *     针对Multiple内部操作的多次执行数据，这里仅取其第一次执行的数据供统计之用，其多次执行输入参数之间的关系会进行独立分析。
     *     参数的等于关联（依赖）关系对于资源竞争强度，死锁发生的可能性和分布式事务的比例有很大的影响，是事务逻辑的重要组成部分。
     */
    @SuppressWarnings("unchecked")
    public Map<String, Map<String, Integer>> countPartitionNotEqualInfo(List<TransactionData> txDataList, Map<Integer, List<Partition>> opId2Partition, Map<Integer, List<String>> opId2paraSchema) {
        // 等于依赖关系的计数器
        // 数据结构解释：当前参数的标识符 -> Map(前一个参数或返回结果集元素的标识符 -> 相等的次数，即计数器)
        // operationId_"para"_paraIndex -> Map(operationId_"para"_paraIndex -> counter)
        Map<String, Map<String, Integer>> para2Para2PartitionEqualCounter = new HashMap<>();

        for (TransactionData txData : txDataList) {
            int[] operationTypes = txData.getOperationTypes();
            Object[] operationDatas = txData.getOperationDatas();

            for (int i = 0; i < operationDatas.length; i++) {
                if (operationTypes[i] == -1) {
                    continue;
                }
                ArrayList<OperationData> operationDataList = null;
                 if (operationTypes[i] == 1) {
                    // 对于multiple中操作的多次运行数据，这里只取其第一次运行的数据作统计分析，同时假设multiple中操作至少运行一次
                    //TODO: 只取第一次运行的数据分析应该是没问题的，但是在生成中却不能如此，还是要讲究这是第几次循环。20210102
                    //TODO: 一般情况下，依赖只在本次循环中发生，这里不考虑第三次循环中某个参数依赖第二次循环中某个参数的情况，且这两个参数属于不同参数。
                     operationDataList = new ArrayList<>((ArrayList<OperationData>)operationDatas[i]);
                } else if (operationTypes[i] == 0) {
                     operationDataList = new ArrayList<>();
                     operationDataList.add((OperationData)operationDatas[i]);
                }

                 int cnt = 0;
                 for (OperationData operationData : operationDataList){
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
                             if (operationTypes[k] == -1) {
                                 continue;
                             }
                             OperationData frontOperationData = null;


                             if (operationTypes[k] == 1) {
                                 frontOperationData = ((ArrayList<OperationData>)operationDatas[k]).get(0);
                             } else if (operationTypes[k] == 0) {
                                 frontOperationData = (OperationData)operationDatas[k];
                             }
                             int frontOperationId = frontOperationData.getOperationId();

                             // ------ 统计与前面操作输入参数之间的等于关联关系 ------
                             Object[] frontParameters = frontOperationData.getParameters();
                             for (int m = 0; m < frontParameters.length; m++) {
                                 String frontParaIdentifier = frontOperationId + "_para_" + m;

                                 Partition frontPartition = opId2Partition.get(frontOperationId).get(m);
                                 if (frontPartition == null) continue;
                                 String frontPartitionKey = frontPartition.getPartition((Number)frontParameters[m]);

                                 String frontParaSchema = opId2paraSchema.get(frontOperationId).get(m);
                                 addParaIfPartitionNotEqual(para2Para2PartitionEqualCounter, partitionKey, paraSchema, paraIdentifier, frontPartitionKey, frontParaSchema, frontParaIdentifier);
                             }

                         } // 针对当前参数前面操作数据(不同SQL)的遍历

                         // ----- 统计当前参数与自身操作（即之前循环的操作以及当前操作）的关联关系
                         // 记录之前的参数的关系是否已经记录，如果记录过了就略过
                         boolean[] isAdd = new boolean[parameters.length];
                         for (int frontIdx = 0; frontIdx <= cnt ; ++ frontIdx) {
                             OperationData frontOperationData = operationDataList.get(frontIdx);
                             Object[] frontParameter = frontOperationData.getParameters();
                             // 如果检查到当前操作，只探测和前面参数的关联，否则全部探测
                             int usedIdx = frontIdx == cnt ? j : frontParameter.length;

                             for (int k = 0; k < usedIdx; k++){
                                 String frontParaIdentifier = operationId + "_para_" + k;

                                 Partition frontPartition = opId2Partition.get(operationId).get(k);
                                 if (frontPartition == null) continue;
                                 String frontPartitionKey = frontPartition.getPartition((Number)frontParameter[k]);

                                 String frontParaSchema = opId2paraSchema.get(operationId).get(k);
                                 isAdd[k] |= addParaIfPartitionNotEqual(para2Para2PartitionEqualCounter, partitionKey, paraSchema, paraIdentifier, frontPartitionKey, frontParaSchema, frontParaIdentifier);
                             }
                         }
                 }
                cnt ++;
                } // 针对当前操作中所有输入参数的遍历
            } // 针对当前事务中所有操作的遍历
        } // 针对一类事务中所有事务实例数据 的遍历

//		System.out.println("EqualRelationAnalyzer.countEqualInfo -> para2ParaOrResult2EqualCounter: \n\t"
//				+ para2ParaOrResult2EqualCounter);
        return para2Para2PartitionEqualCounter;
    }

    /**
     * 检查当前参数与之前的参数是否不等，如果是，更新对应的计数器
     *
     * @param para2Para2PartitionNotEqualCounter 参数不等的计数器
     * @param partitionKey                    分区
     * @param paraSchema                      参数的schema
     * @param paraIdentifier                  参数的标识符
     * @param frontPartitionKey               之前的参数的分区
     * @param frontParaSchema 之前的参数的schema
     * @param frontIdentifier                 之前参数的标识符
     */
    private boolean addParaIfPartitionNotEqual(Map<String, Map<String, Integer>> para2Para2PartitionNotEqualCounter, String partitionKey,
                                               String paraSchema, String paraIdentifier, String frontPartitionKey, String frontParaSchema, String frontIdentifier) {
        if (paraSchema.equals(frontParaSchema) && !partitionKey.equals(frontPartitionKey)) {

            if (!para2Para2PartitionNotEqualCounter.get(paraIdentifier).containsKey(frontIdentifier)) {
                para2Para2PartitionNotEqualCounter.get(paraIdentifier).put(frontIdentifier, 1);
            } else {
                int tmp = para2Para2PartitionNotEqualCounter.get(paraIdentifier).get(frontIdentifier);
                para2Para2PartitionNotEqualCounter.get(paraIdentifier).put(frontIdentifier, tmp + 1);
            }
            return true;
        }

        return false;
    }

    /**
     * @param parameterNodeMap：此时等于依赖关系必须已维护完成（等于、包含和线性依赖关系都会维护在ParameterNode中）
     * @param formattedCounter：经格式化后的包含依赖关系的计数器（新计数器的特征：事务实例个数转化成相应比例 & 按照一定规则进行了排序）
     * @param identicalSets：数值大小完全相等的输入参数和返回结果集元素的集合，该输入与
     *     EqualRelationAnalyzer.constructDependency输入中的identicalSets完全相同。
     * @function：依据formatedCounter中的统计信息 以及 identicalSets，构建参数与 返回结果集 之间的包含依赖关系。
     */
    public void constructDependency(Map<String, ParameterNode> parameterNodeMap,
                                    List<Map.Entry<String, List<Map.Entry<String, Double>>>> formattedCounter, List<Set<String>> identicalSets) {
        // 若两个参数完全相同，当前一个参数构建完包含依赖关系后，后一个参数无需再构建包含依赖关系（其值已完全确定）
        Set<String> identicalItems = new HashSet<>();

        for (Map.Entry<String, List<Map.Entry<String, Double>>> paraPartitionEqualInfo : formattedCounter) {
            if (identicalItems.contains(paraPartitionEqualInfo.getKey())) {
                // 前面有个参数与当前参数的值完全相同，当前参数的值已确定~ 无需维护包含依赖关系
                continue;
            }

            // 每个参数的ParameterNode都必然已存在，在函数EqualRelationAnalyzer.constructDependency中构建的
            // 下面获取之前已维护的等于依赖关系
            // List<ParameterDependency> dependencies = parameterNodeMap.get(paraPartitionEqualInfo.getKey()).getDependencies();
            // bug fix: 添加了事务逻辑统计项控制参数之后，一个参数的ParameterNode可能不存在（没有统计等于关联关系）
            List<ParameterDependency> dependencies = null;
            if (parameterNodeMap.containsKey(paraPartitionEqualInfo.getKey())) {
                dependencies = parameterNodeMap.get(paraPartitionEqualInfo.getKey()).getDependencies();
            } else {
                List<String> identifiers = new ArrayList<String>();
                identifiers.add(paraPartitionEqualInfo.getKey());
                ParameterNode parameterNode = new ParameterNode(identifiers);
                parameterNodeMap.put(paraPartitionEqualInfo.getKey(), parameterNode);
                dependencies = new ArrayList<>();
                parameterNode.setDependencies(dependencies);
            }

            // paraDependencyInfo为当前参数上统计得到的分区等于依赖关系
            // 因为paraDependencyInfo上有更新操作，这样处理是不想破坏原有统计数据
            List<Map.Entry<String, Double>> paraDependencyInfo = new ArrayList<>(paraPartitionEqualInfo.getValue());

            double probabilitySum = 0; // 已维护依赖关系的概率之和
            for (ParameterDependency dependency : dependencies) {
                probabilitySum += dependency.getProbability();
            }

            List<ParameterDependency> appendedIncludeDependencies = new ArrayList<>();
            // 先遍历一遍，将可以直接添加的分区等于依赖关系找出来，对于这些包含依赖关系的添加是不需要替换掉原来的等于依赖关系的
            for (int j = 0; j < paraDependencyInfo.size(); j++) {
                Map.Entry<String, Double> includeDependency = paraDependencyInfo.get(j);
                if (probabilitySum + includeDependency.getValue() <= 1) {
                    appendedIncludeDependencies.add(
                            new ParameterDependency(includeDependency.getKey(), includeDependency.getValue(), ParameterDependency.DependencyType.PARTITION_NOT_EQUAL));
                    probabilitySum += includeDependency.getValue();
                    paraDependencyInfo.remove(j--);
                }
            }

            // 对于剩下的分区相等依赖关系，根据其关联关系强弱选择性地替换掉原来的等于依赖关系
            // 替换规则：关联概率probability大于原等于依赖关系的两倍，才可替换（前提是总依赖关系的概率和不超过1）
            // 选择这样的替换规则的原因是我们认为等于依赖关系相比包含依赖关系更重要~
            for (Map.Entry<String, Double> includeDependency : paraDependencyInfo) {
                // 我们更看重等于依赖关系，替换时也从关联概率最小的等于关系替换
                probabilitySum = getProbabilitySum(dependencies, probabilitySum, includeDependency);
            }

            dependencies.addAll(appendedIncludeDependencies); // 等于关联概率逆序，包含关联概率先升序后逆序（大致情况）

            // 在上面的两个for循环中不作过滤的原因是我们不认为有两个返回结果集一直是完全相同的！
            // 下面将所有和该参数完全相等的参数集合一并存储在identicalItems中，以便后续过滤之用
            for (Set<String> tmpSet : identicalSets) {
                if (tmpSet.contains(paraPartitionEqualInfo.getKey())) {
                    identicalItems.addAll(tmpSet);
                    break;
                }
            }
        }  //  for all paraPartitionEqualInfo in formattedCounter

//		System.out.println("IncludeRelationAnalyzer.constructDependency -> parameterNodeMap: \n\t" + parameterNodeMap);
    }

    static double getProbabilitySum(List<ParameterDependency> dependencies, double probabilitySum, Map.Entry<String, Double> includeDependency) {
        for (int k = dependencies.size() - 1; k >= 0; k--) {
            if (dependencies.get(k).getDependencyType() == ParameterDependency.DependencyType.EQUAL
                    && includeDependency.getValue() >= dependencies.get(k).getProbability() * 2
                    && probabilitySum - dependencies.get(k).getProbability() + includeDependency.getValue() <= 1.00000001) {
                dependencies.set(k, new ParameterDependency(includeDependency.getKey(), includeDependency.getValue(), ParameterDependency.DependencyType.PARTITION_NOT_EQUAL));
                probabilitySum = probabilitySum - dependencies.get(k).getProbability() + includeDependency.getValue();
                break;
            } else if (dependencies.get(k).getDependencyType() != ParameterDependency.DependencyType.EQUAL
                    && includeDependency.getValue() >= dependencies.get(k).getProbability()
                    && probabilitySum - dependencies.get(k).getProbability() + includeDependency.getValue() <= 1.00000001) {
                dependencies.set(k, new ParameterDependency(includeDependency.getKey(), includeDependency.getValue(), ParameterDependency.DependencyType.PARTITION_NOT_EQUAL));
                probabilitySum = probabilitySum - dependencies.get(k).getProbability() + includeDependency.getValue();
                break;
            }

        }
        return probabilitySum;
    }
}
