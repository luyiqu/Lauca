package accessdistribution;

import abstraction.Transaction;
import input.TraceInfo;
import transactionlogic.ParameterDependency;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 @@author : lyqu
 去掉原本已经在事务逻辑中统计过的参数，因为这些参数生成的时候大概率是根据事务逻辑要生成的，
 不希望统计访问分布的时候还用到这些参数。以下除了txnId2txnTraceAfterDelete是深拷贝的之外，其他都是浅拷贝，都是指向同一个对象（为了避免新建对象的开销）。

 ps: 感觉还是把txnId2txnTrace对象改掉了，还是浅拷贝
 debug: linearDependencies只考虑了integer

 */


public class DeleteLogicalTxnPara {

    /** 记录每个事务实例对应的事务模板号（相当于事务名）,模板号从1开始 ,传进来的*/
    private Map<Long,Integer> txnId2txnTemplateID = null;
    /** 得到每个事务模板号对应哪些事务id,要求跟txnId2txnTraceList中List<TraceInfo>的顺序一致 */
    private Map<Integer, List<Long>> txnTemplateID2txnIDList = new HashMap<>();

//    /** 原本的所有参数，由构造函数传进来*/
//    private Map<Long, List<TraceInfo>> txnId2txnTrace = new HashMap<>();

    /** 事务 ： 该事务所有的参数， 用于和已经生成的事务逻辑去匹配，去掉已经在事务逻辑中出现的参数*/
    private Map<Integer,List<List<TraceInfo>>> txnTemplateID2txnTraceList = new HashMap<>();

    /** 用作判断，做一个深拷贝 */
    private Map<Integer,List<List<TraceInfo>>> txnTemplateID2txnTraceListOrigin = new HashMap<>();
    /** 记录最终删除后参数值的结果，该结果用于去生成数据访问分布*/
    private Map<Long, List<TraceInfo>> txnId2txnTraceAfterDelete = new HashMap<>();


    private List<Transaction> transactions = null;

    public DeleteLogicalTxnPara(Map<Long, Integer> txnId2txnTemplateID,
                                Map<Long, List<TraceInfo>> txnId2txnTrace,List<Transaction> transactions) {
        this.txnId2txnTemplateID = txnId2txnTemplateID;
//        this.txnId2txnTrace = txnId2txnTrace;
        this.transactions = transactions;
        this.txnId2txnTraceAfterDelete.putAll(txnId2txnTrace); //实现深拷贝，代替txnId2txnTrace

    }

//    public static void main(String[] args) {
//        DeleteLogicalTxnPara deleteLogicalTxnPara = new DeleteLogicalTxnPara();
//    }


    /**
     * 将txnId2txnTemplateID转换成Map<Integer, List<Long>> txnTemplateID2txnIDList
     * 得到Map<Integer,List<List<TraceInfo>>>
     * @@param Map<Long,Integer> txnId2txnTemplateID,Map<Integer,List<List<TraceInfo>>> txnId2txnTraceList

     **/
    public void changeForm(){
        for(Long txnId : txnId2txnTemplateID.keySet()){
            int templateId = txnId2txnTemplateID.get(txnId);
            if(txnTemplateID2txnIDList.containsKey(templateId)){
                txnTemplateID2txnIDList.get(templateId).add(txnId);
                txnTemplateID2txnTraceList.get(templateId).add(txnId2txnTraceAfterDelete.get(txnId));
            }else{
                txnTemplateID2txnIDList.put(templateId, new ArrayList<>());
                txnTemplateID2txnTraceList.put(templateId,new ArrayList<>());
                txnTemplateID2txnIDList.get(templateId).add(txnId);
                txnTemplateID2txnTraceList.get(templateId).add(txnId2txnTraceAfterDelete.get(txnId));
            }
        }
        txnTemplateID2txnTraceListOrigin.putAll(txnTemplateID2txnTraceList);
    }

    /**
     * 处理txnId2txnTraceList ，得到删掉特定参数后的 txnId2txnTraceList
     * @@param txnId2txnTraceList
     */

    public void deleteTxnLogicPara(){
        for(Transaction transaction : transactions){
            String txnName = transaction.getName();
            int templateId = Integer.parseInt(txnName.substring(txnName.lastIndexOf("n")+1));
            List<List<TraceInfo>> txnTraceLists = txnTemplateID2txnTraceList.get(templateId);
//            System.out.println(txnTraceLists.size());
            List<List<TraceInfo>> txnTraceListsOrigin = txnTemplateID2txnTraceListOrigin.get(templateId);
            for(String parameter : transaction.getParameterNodeMap().keySet()){
                int parameterSqlId = Integer.parseInt(parameter.substring(0,parameter.indexOf('_')));
                int parameterparaId = Integer.parseInt(parameter.substring(parameter.lastIndexOf('_')+1));
                List<String> identifiers = transaction.getParameterNodeMap().get(parameter).getIdentifiers();
                // 判断该节点相同参数的size，如果多个参数共享同一个节点，则需要将其中参数值后面的都去掉，但如果对应的参数值不是最前面的哪个，则跳过
                if(identifiers.size() != 1 && parameter.equals(identifiers.get(0))){
                    for(String identifier : identifiers){
                        //除了第一个是parameter,其他在identifiers中的参数都需要由占位符取代。
                        if(!identifier.equals(parameter)){
                            int sqlId = Integer.parseInt(identifier.substring(0,identifier.indexOf('_')));
                            int paraId = Integer.parseInt(identifier.substring(identifier.lastIndexOf('_')+1));
                            for(List<TraceInfo> txnTraceList : txnTraceLists){
                                for (TraceInfo traceInfo : txnTraceList) {
                                    if (traceInfo.operationID == sqlId) {
                                        traceInfo.parameters.set(paraId, "#@#");
//                                        break; //针对multiple，只考虑multiple的第一次循环，但忽略了事务内的逻辑，还是不要break
                                    }
                                }
//                                System.out.println("!!identifiers!!");

                            }
                        }

                    }
                //这个para肯定会在其他的相同上考虑到，因此就直接跳过这一整个依赖 ~
                }else if(!parameter.equals(identifiers.get(0))){
                    continue;
                }

//                TODO:判断dependencies时，要结合paramenter以及它的依赖的，删掉的parameter ~ ER & IR\
//                //目前是只要在事务逻辑中参数的依赖关系，不管概率多大，都进行考虑了，以后可以把概率设为0.1以上再进行删除参数值操作
//                List<ParameterDependency> dependencies = transaction.getParameterNodeMap().get(parameter).getDependencies();
//                for(ParameterDependency dependency : dependencies){
//                    if(dependency.getProbability() <= 0.1){
//                        continue;
//                    }
//                    String dependencyIdentifier = dependency.getIdentifier();
//                    int sqlId = Integer.parseInt(dependencyIdentifier.substring(0,dependencyIdentifier.indexOf('_')));
//                    int paraId = Integer.parseInt(dependencyIdentifier.substring(dependencyIdentifier.lastIndexOf('_')+1));
//                    String paraOrResult = dependencyIdentifier.substring(dependencyIdentifier.indexOf('_')+1,dependencyIdentifier.lastIndexOf('_'));
//                    for(int j = 0;j < txnTraceLists.size();j++){
//                        List<TraceInfo> txnTraceList = txnTraceLists.get(j);
//                        List<TraceInfo> txnTraceParameters = new ArrayList<>(),txnTraceDependencies = new ArrayList<>();  //可能返回多个,multiple中也有逻辑~
//                        for(int traceId = 0;traceId<txnTraceList.size();traceId++){
//                            if(txnTraceList.get(traceId).operationID == parameterSqlId){
//                                txnTraceParameters.add(txnTraceList.get(traceId));
////                                break;
//                            }
//                        }
//                        for(int traceId = 0;traceId<txnTraceList.size();traceId++){
//                            if(txnTraceList.get(traceId).operationID == sqlId){
//                                txnTraceDependencies.add(txnTraceListsOrigin.get(j).get(traceId));
////                                break;
//                            }
//                        }
//
//                        /** ER */
//                        if(dependency.getDependencyType() == 0){
//                            /** parameter */
//                            if(paraOrResult.equals("para")){
//
//                                for(int multipleId = 0;multipleId < txnTraceParameters.size();multipleId++){
//                                    TraceInfo txnTraceParameter = txnTraceParameters.get(multipleId);
//                                    TraceInfo txnTraceDependency = null;
//                                    if(txnTraceDependencies.size() == 1){
//                                        txnTraceDependency = txnTraceDependencies.get(0);
//                                    }else{
//                                        txnTraceDependency = txnTraceDependencies.get(multipleId);
//                                    }
//
//                                    if(txnTraceParameter.parameters.get(parameterparaId).equals(txnTraceDependency.parameters.get(paraId))){
////                                    System.out.println("!!parameter ER!!");
//                                        txnTraceParameter.parameters.set(parameterparaId,"#@#");
//                                    }
//                                }
//
//                                /** results */
//                            }
////                            else if(paraOrResult.equals("result")){
////                                //只有返回一个参数的时候才有ER，不然是IR
////                                for(int multipleId = 0;multipleId < txnTraceParameters.size();multipleId++){
////                                    TraceInfo txnTraceParameter = txnTraceParameters.get(multipleId);
//////                                    TraceInfo txnTraceDependency = txnTraceDependencies.get(multipleId);
////                                    TraceInfo txnTraceDependency = null;
////                                    if(txnTraceDependencies.size() == 1){
////                                        txnTraceDependency = txnTraceDependencies.get(0);
////                                    }else{
////                                        txnTraceDependency = txnTraceDependencies.get(multipleId);
////                                    }
////                                    if(txnTraceDependency.results.size() == 1 &&
////                                            txnTraceParameter.parameters.get(parameterparaId).equals(txnTraceDependency.results.get(0).get(paraId))){
////                                        txnTraceParameter.parameters.set(parameterparaId,"#@#");
//////                                    System.out.println("!!results ER!!");
////                                    }
////                                }
////
////                            }
//                        } /** IR 只能是result*/
//                        //TODO 终于找到问题了，这段有问题 呜呜呜。20201229 但还是不知道为啥，对性能应该影响不大
////                        else if(dependency.getDependencyType() == 1){
////                            for(int multipleId = 0;multipleId < txnTraceParameters.size();multipleId++) {
////                                TraceInfo txnTraceParameter = txnTraceParameters.get(multipleId);
//////                                TraceInfo txnTraceDependency = txnTraceDependencies.get(multipleId);
////                                TraceInfo txnTraceDependency = null;
////                                if(txnTraceDependencies.size() == 1){
////                                    txnTraceDependency = txnTraceDependencies.get(0);
////                                }else{
////                                    txnTraceDependency = txnTraceDependencies.get(multipleId);
////                                }
////                                for(List<String> result : txnTraceDependency.results){
////                                    if(txnTraceParameter.parameters.get(parameterparaId).equals(result.get(paraId))){
////                                        txnTraceParameter.parameters.set(parameterparaId,"#@#");
////                                    System.out.println("!!results IR!!");
////                                        break;
////                                    }
////                                }
////                            }
////
////
////                        }
//
//                    }
//
//                }
//
////                TODO: 判断linearDependencies ！= null ax+b = y （y是参数，x是依赖）
//
//                List<ParameterDependency> linearDependencies = transaction.getParameterNodeMap().get(parameter).getLinearDependencies();
//                if(linearDependencies != null){
//                    for(ParameterDependency linearDependency : linearDependencies){
//                        String linearDependencyIdentifier = linearDependency.getIdentifier();
//                        double a = linearDependency.getCoefficientA();
//                        double b = linearDependency.getCoefficientB();
//                        int sqlId = Integer.parseInt(linearDependencyIdentifier.substring(0,linearDependencyIdentifier.indexOf('_')));
//                        int paraId = Integer.parseInt(linearDependencyIdentifier.substring(linearDependencyIdentifier.lastIndexOf('_')+1));
//                        String paraOrResult = linearDependencyIdentifier.substring(linearDependencyIdentifier.indexOf('_')+1,linearDependencyIdentifier.lastIndexOf('_'));
//
//
//                        for(int j = 0;j < txnTraceLists.size();j++){
//                            List<TraceInfo> txnTraceList = txnTraceLists.get(j);
//                            List<TraceInfo> txnTraceParameters = new ArrayList<>(),txnTraceDependencies = new ArrayList<>();
//                            for(int traceId = 0; traceId<txnTraceList.size();traceId++){
//                                if(txnTraceList.get(traceId).operationID == parameterSqlId){
//                                    txnTraceParameters.add(txnTraceList.get(traceId));
////                                    break; //针对multiple
//                                }
//                            }
//                            for(int traceId = 0;traceId<txnTraceList.size();traceId++){
//                                if(txnTraceList.get(traceId).operationID == sqlId){
//                                    txnTraceDependencies.add(txnTraceListsOrigin.get(j).get(traceId));
////                                    break; //针对multiple
//                                }
//                            }
//
//
//                            /** parameter */
//                            if(paraOrResult.equals("para")){
//                                for(int multipleId = 0;multipleId < txnTraceParameters.size();multipleId++) {
//                                    TraceInfo txnTraceParameter = txnTraceParameters.get(multipleId);
//                                    TraceInfo txnTraceDependency = null;
//                                    if(txnTraceDependencies.size() == 1){
//                                        txnTraceDependency = txnTraceDependencies.get(0);
//                                    }else{
//                                        txnTraceDependency = txnTraceDependencies.get(multipleId);
//                                    }
////                                    TraceInfo txnTraceDependency = txnTraceDependencies.get(multipleId);
//                                    if(txnTraceParameter.parameters.get(parameterparaId).equals("#@#")){
//                                        continue;
//                                    }
//                                    if(!txnTraceDependency.parameters.get(paraId).contains(".")&&!txnTraceParameter.parameters.get(parameterparaId).contains("."))
//                                    {
//                                        if(txnTraceParameter.parameters.get(parameterparaId).equals("#@#")||txnTraceDependency.parameters.get(paraId).equals("#@#")){
//                                            continue;
//                                        }
//                                        //TODO: debug: 这里需要结合paramters的参数类型，但是一开始就没考虑，这里都先假定是integer型的（对tpcc负载是没问题的）
//                                        if(Integer.parseInt(txnTraceDependency.parameters.get(paraId)) * a + b == Integer.parseInt(txnTraceParameter.parameters.get(parameterparaId))){
//                                            txnTraceParameter.parameters.set(parameterparaId,"#@#");
////                                    System.out.println("!!parameter IR!!");
//                                        }
//                                    }
//
//                                }
//
//                                /** result */
//                            }else if(paraOrResult.equals("result")){
//                                for(int multipleId = 0;multipleId < txnTraceParameters.size();multipleId++) {
//                                    TraceInfo txnTraceParameter = txnTraceParameters.get(multipleId);
////                                    TraceInfo txnTraceDependency = txnTraceDependencies.get(multipleId);
//                                    TraceInfo txnTraceDependency = null;
//                                    if(txnTraceDependencies.size() == 1){
//                                        txnTraceDependency = txnTraceDependencies.get(0);
//                                    }else{
//                                        txnTraceDependency = txnTraceDependencies.get(multipleId);
//                                    }
//
//                                    if(txnTraceParameter.parameters.get(parameterparaId).equals("#@#")){
//                                            continue;
//                                    }
//
//                                    for(List<String> result : txnTraceDependency.results){
//                                        try{
//                                            if(Integer.parseInt(result.get(paraId)) * a + b == Integer.parseInt(txnTraceParameter.parameters.get(parameterparaId))) {
//                                                txnTraceParameter.parameters.set(parameterparaId, "#@#");
////                                            System.out.println("!!results IR!!");
//                                                break;
//                                            }
//                                        }catch (Exception e){
//                                            System.out.println("sqlId: "+parameterSqlId);
//                                            System.out.println("parameterId: "+parameterparaId);
//                                            System.out.println("DsqlId: "+sqlId);
//                                            System.out.println("DparameterId: "+paraId);
//                                            System.out.println(linearDependency);
////                                            System.out.println("dependency  "+ result.get(par));
////                                            System.out.println("parameters  "+txnTraceParameter.parameters.get(parameterparaId));
////
//                                            System.exit(1);
//                                        }
//
//                                    }
//                                }
//
//                            }
//                        }
//
//                    }
//                }
//
            }

            //TODO : FOR PARA in MutipleLogicMap，都删掉
            for(String multiParameter : transaction.getMultipleLogicMap().keySet()){
                String multiParameterAfter = multiParameter.substring(9);
                int multiParameterSqlId = Integer.parseInt(multiParameterAfter.substring(0,multiParameterAfter.indexOf('_')));
                int multiParameterparaId = Integer.parseInt(multiParameterAfter.substring(multiParameterAfter.lastIndexOf('_')+1));
                for(List<TraceInfo> txnTraceList : txnTraceLists){
                    int multipleCount = 0;
                    for(int traceId = 0; traceId < txnTraceList.size();traceId++){
                        if(txnTraceList.get(traceId).operationID==multiParameterSqlId){
                            multipleCount+=1;
                            if(multipleCount != 1){
                                txnTraceList.get(traceId).parameters.set(multiParameterparaId,"#@#");
                            }
                        }
                    }
//                    System.out.println("!!Mutiple!!");
                }
            }
        }
    }


    /**
     * 得到最终的txnId2txnTraceAfterDelete
     * @@param Map<Integer,List<List<TraceInfo>>> txnTemplateID2txnTraceList
     * @@param Map<Integer,List<Long>> txnTemplateID2txnIDList
     * @return Map<Long, List<TraceInfo>> txnId2txnTraceAfterDelete
     * ps: 感觉已经改了txnId2txnTraceAfterDelete中的List<TraceInfo>对象，试试看 ~
     */
//
//    private void changetxnTraceList2OriginForm(){
//        for(Integer txnTemplateID: txnTemplateID2txnIDList.keySet()){
//            List<List<TraceInfo>> txnTraceLists = txnTemplateID2txnTraceList.get(txnTemplateID);
//            List<Long> txnIDLists = txnTemplateID2txnIDList.get(txnTemplateID);
//            for(int i = 0; i < txnIDLists.size();i++){
//
//            }
//        }
//
//    }



    public List<Transaction> getTransactions() {
        return transactions;
    }

    public Map<Integer, List<List<TraceInfo>>> getTxnTemplateID2txnTraceList() {
        return txnTemplateID2txnTraceList;
    }

    public Map<Integer, List<Long>> getTxnTemplateID2txnIDList() {
        return txnTemplateID2txnIDList;
    }

    public Map<Long, Integer> getTxnId2txnTemplateID() {
        return txnId2txnTemplateID;
    }

    public Map<Long, List<TraceInfo>> getTxnId2txnTraceAfterDelete() {
        return txnId2txnTraceAfterDelete;
    }
}
