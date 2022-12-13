package accessdistribution;

import abstraction.*;
import accessdistribution.DataAccessDistribution;


import javax.swing.plaf.LabelUI;
import java.util.*;

/**
 * 增加新增column的全部访问分布
 * @author Luyi Qu
 **/

public class FakeAccessDistribution {
    private List<Transaction> transactions = null;
    private Map<String, Map<String, Vector<DataAccessDistribution>>> txName2ParaId2DistributionList = null;
    private Map<String, Map<String, DataAccessDistribution>> txName2ParaId2FullLifeCycleDistribution = null;

    public FakeAccessDistribution(List<Transaction> transactions,Map<String,Map<String,Vector<DataAccessDistribution>>> txName2ParaId2DistributionList
    ,Map<String, Map<String, DataAccessDistribution>> txName2ParaId2FullLifeCycleDistribution){
        this.transactions = transactions;
        this.txName2ParaId2DistributionList = txName2ParaId2DistributionList;
        this.txName2ParaId2FullLifeCycleDistribution = txName2ParaId2FullLifeCycleDistribution;
    }
    //先把insert操作对应的txn 和 sql找出来 ok
    //再找出index = 0 的 window_time List ok
    //这边最好能传进来多增加column的index，是在第几个
    //明确是否需要new vector 为operation_id_index

    public void addTxname2ParaIdDistributionList(){
        for (Transaction trans : transactions) {
            for (TransactionBlock transBlock : trans.getTransactionBlocks()) {
                String className = transBlock.getClass().getName();
                if (className.equals("abstraction.WriteOperation")) {
                    WriteOperation op = (WriteOperation) transBlock;
                    String sql = op.sql; //已经是修改好之后的sql了
                    if (sql.trim().toLowerCase().startsWith("insert")) {
                        int operationId = op.getOperationId();
                        String operationIDIndex = null; //遍历新增加的column
                        DataAccessDistribution[] fullLifeCycleParaGenerators = op.getFullLifeCycleParaGenerators();
                        int number = fullLifeCycleParaGenerators.length;

                        for (int n = 0; n < number; n++) {
                            operationIDIndex = operationId + "_" + n;
                                //无需生成window分布
//                            if (txName2ParaId2DistributionList.get(trans.getName()).get(operationIDIndex) == null) {
//                                txName2ParaId2DistributionList.get(trans.getName()).put(operationIDIndex, new Vector<>());
//
//                                Vector<Long> windowTimeList = getWindowTime(trans.getName(), operationId);
//                                for (Long time : windowTimeList) {
//                                    DataAccessDistribution dataAccessDistribution = windowParaGenerators[n];
//                                    dataAccessDistribution.setTime(time);
//                                    txName2ParaId2DistributionList.get(trans.getName()).get(operationIDIndex).add(dataAccessDistribution);
//                                }
//                            }
                            //生成fullLife的访问分布
                            if (!txName2ParaId2FullLifeCycleDistribution.get(trans.getName()).containsKey(operationIDIndex)) {
                                txName2ParaId2FullLifeCycleDistribution.get(trans.getName()).put(operationIDIndex, fullLifeCycleParaGenerators[n]);
                            }

                        }
                    }


                } else if (className.equals("abstraction.Multiple")) {
                    Multiple multiple = (Multiple) transBlock;
                    List<SqlStatement> sqls = multiple.getSqls();
                    for (int k = 0; k < sqls.size(); k++) {
                        String className2 = sqls.get(k).getClass().getName();
                        if (className2.equals("abstraction.WriteOperation")) {
                            WriteOperation op = (WriteOperation) sqls.get(k);
                            String sql = op.sql;
                            if (sql.trim().toLowerCase().startsWith("insert")) {
                                int operationId = op.getOperationId();
                                String operationIDIndex = null; //遍历新增加的column
                                DataAccessDistribution[] fullLifeCycleParaGenerators = op.getFullLifeCycleParaGenerators();
                                int number = fullLifeCycleParaGenerators.length;
                                for (int n = 0; n < number; n++) {
                                    operationIDIndex = operationId + "_" + n;
                                        //无需生成window分布
//                                    if (txName2ParaId2DistributionList.get(trans.getName()).get(operationIDIndex) == null) {
//                                        txName2ParaId2DistributionList.get(trans.getName()).put(operationIDIndex, new Vector<>());
//
//                                        Vector<Long> windowTimeList = getWindowTime(trans.getName(), operationId);
//                                        for (Long time : windowTimeList) {
//                                            DataAccessDistribution dataAccessDistribution = windowParaGenerators[n];
//                                            dataAccessDistribution.setTime(time);
//                                            txName2ParaId2DistributionList.get(trans.getName()).get(operationIDIndex).add(dataAccessDistribution);
//                                        }
//                                    }
                                    //生成fullLife的访问分布
                                    if (txName2ParaId2FullLifeCycleDistribution.get(trans.getName()).get(operationIDIndex) == null) {
                                        txName2ParaId2FullLifeCycleDistribution.get(trans.getName()).put(operationIDIndex, fullLifeCycleParaGenerators[n]);
                                    }

                                }
                            }
                        }
                    }
                } else if (className.equals("abstraction.Branch")) {
                    Branch branch = (Branch) transBlock;
                    List<List<SqlStatement>> branches = branch.getBranches();
                    for (int k = 0; k < branches.size(); k++) {
                        List<SqlStatement> sqls = branches.get(k);
                        for (int m = 0; m < sqls.size(); m++) {
                            String className2 = sqls.get(m).getClass().getName();
                            if (className2.equals("abstraction.WriteOperation")) {
                                WriteOperation op = (WriteOperation) sqls.get(m);
                                String sql = op.sql;
                                if (sql.trim().toLowerCase().startsWith("insert")) {
                                    int operationId = op.getOperationId();
                                    String operationIDIndex = null; //遍历新增加的column
                                    //无需生成window分布
                                    DataAccessDistribution[] fullLifeCycleParaGenerators = op.getFullLifeCycleParaGenerators();
                                    int number = fullLifeCycleParaGenerators.length;
                                    for (int n = 0; n < number; n++) {
                                        operationIDIndex = operationId + "_" + n;
//                                        if (txName2ParaId2DistributionList.get(trans.getName()).get(operationIDIndex) == null) {
//                                            txName2ParaId2DistributionList.get(trans.getName()).put(operationIDIndex, new Vector<>());
//                                            //说明这参数还没有进去~
//                                            Vector<Long> windowTimeList = getWindowTime(trans.getName(), operationId);
//                                            for (Long time : windowTimeList) {
//                                                DataAccessDistribution dataAccessDistribution = windowParaGenerators[n];
//                                                dataAccessDistribution.setTime(time);
//                                                txName2ParaId2DistributionList.get(trans.getName()).get(operationIDIndex).add(dataAccessDistribution);
//                                            }
//                                        }
                                        //生成fullLife的访问分布
                                        if (txName2ParaId2FullLifeCycleDistribution.get(trans.getName()).get(operationIDIndex) == null) {
                                            txName2ParaId2FullLifeCycleDistribution.get(trans.getName()).put(operationIDIndex, fullLifeCycleParaGenerators[n]);
                                        }

                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

    }


    public Map<String, Map<String, Vector<DataAccessDistribution>>> getTxName2ParaId2DistributionList(){
        return txName2ParaId2DistributionList;
    }

    public Map<String, Map<String, DataAccessDistribution>> getTxName2ParaId2FullLifeCycleDistribution(){
        return txName2ParaId2FullLifeCycleDistribution;
    }


}
