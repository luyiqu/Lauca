package accessdistribution;

import abstraction.*;
import config.Configurations;

import java.io.File;
import java.util.List;

/**
 * 新增column后，在transaction中加入新增column的相关信息
 * @author Luyi Qu
 **/
public class FakeTransactionDistribution {

    private List<Transaction> transactions = null;
    private List<Table> originTables = null;
    private List<Table> tables = null;

    public FakeTransactionDistribution(List<Transaction> transactions,List<Table> originTables,List<Table> tables){
        this.transactions = transactions;
        this.originTables = originTables;
        this.tables = tables;
    }

    public List<Transaction> getTransactions() {
        return transactions;
    }

    //增加fake column的分布情况
    public void addFakeColumnDistributionAccess()
    {

        for (Transaction trans : transactions) {
            for (TransactionBlock transBlock : trans.getTransactionBlocks()) {
                String className = transBlock.getClass().getName();
                if (className.equals("abstraction.WriteOperation")) {
                    WriteOperation op = (WriteOperation) transBlock;
                    String sql = op.sql;
                    if(sql.trim().toLowerCase().startsWith("insert")){
                        Column[]fakeColumn = getFakeColumn(sql,originTables,tables);
                        if(fakeColumn.length==0){
                            op.sql = sql;
                       }
                        else{
                            String modifiedSql = modifyInsertTemplate(sql,fakeColumn);
                            op.sql = modifiedSql;
                        }
                        //现在fake column都是int类型的~
                        int []paraDataTypes = op.getParaDataTypes();
                        DistributionTypeInfo[] paraDistTypeInfos = op.getParaDistTypeInfos();
                        int []paraDataTypesModified = new int[paraDataTypes.length+fakeColumn.length];
                        DistributionTypeInfo[] paraDistTypeInfosModified = new DistributionTypeInfo[paraDataTypes.length+fakeColumn.length];
                        int i= 0;
                        for(;i < paraDataTypes.length;i++){
                            paraDataTypesModified[i] = paraDataTypes[i];
                            paraDistTypeInfosModified[i] = paraDistTypeInfos[i];
                        }
                        for(int j = 0;j < fakeColumn.length;j++){
                            paraDataTypesModified[i] = fakeColumn[j].getDataType();
                            paraDistTypeInfosModified[i] = new DistributionTypeInfo
                                    (1,(long)fakeColumn[j].getPara1(),(long)fakeColumn[j].getPara2(),fakeColumn[j].getCardinality(),fakeColumn[j].getCoefficient());
                            i++;
                        }

                        op.setParaDataTypes(paraDataTypesModified);
                        op.setParaDistTypeInfos(paraDistTypeInfosModified);
                        op.setWindowParaGenerators(fakeColumn);
                        op.setFullLifeCycleParaGenerators(fakeColumn);
//                        System.out.println("modify sql :"+op.sql);
//                        System.out.println("After windowParaGenerators Size: "+op.getWindowParaGenerators().length);
//                        System.out.println("After FullLifeCycleParaGenerators Size: "+op.getFullLifeCycleParaGenerators().length);

                    }


                } else if (className.equals("abstraction.Multiple")) {
                    Multiple multiple = (Multiple) transBlock;
                    List<SqlStatement> sqls = multiple.getSqls();
                    for (int k = 0; k < sqls.size(); k++) {
                        String className2 = sqls.get(k).getClass().getName();
                        if (className2.equals("abstraction.WriteOperation")) {
                            WriteOperation op = (WriteOperation) sqls.get(k);
                            String sql = op.sql;
                            if(sql.trim().toLowerCase().startsWith("insert")){
                                Column []fakeColumn = getFakeColumn(sql,originTables,tables);
                                if(fakeColumn.length==0){
                                    //System.out.println("No increase column");
                                    op.sql = sql;
                                }
                                else{
                                    String modifiedSql = modifyInsertTemplate(sql,fakeColumn);
                                    op.sql = modifiedSql;
                                }
                                //现在fake column都是int类型的 ~
                                int []paraDataTypes = op.getParaDataTypes();
                                DistributionTypeInfo[] paraDistTypeInfos = op.getParaDistTypeInfos();
                                int []paraDataTypesModified = new int[paraDataTypes.length+fakeColumn.length];
                                DistributionTypeInfo[] paraDistTypeInfosModified = new DistributionTypeInfo[paraDataTypes.length+fakeColumn.length];
                                int i= 0;
                                for(;i < paraDataTypes.length;i++){
                                    paraDataTypesModified[i] = paraDataTypes[i];
                                    paraDistTypeInfosModified[i] = paraDistTypeInfos[i];
                                }
                                for(int j = 0;j < fakeColumn.length;j++){
                                    paraDataTypesModified[i] = fakeColumn[j].getDataType();
                                    paraDistTypeInfosModified[i] = new DistributionTypeInfo
                                            (1,(long)fakeColumn[j].getPara1(),(long)fakeColumn[j].getPara2(),fakeColumn[j].getCardinality(),fakeColumn[j].getCoefficient());
                                    i++;
                                }

                                op.setParaDataTypes(paraDataTypesModified);
                                op.setParaDistTypeInfos(paraDistTypeInfosModified);
                                op.setWindowParaGenerators(fakeColumn);
                                op.setFullLifeCycleParaGenerators(fakeColumn);
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
                                if(sql.trim().toLowerCase().startsWith("insert")){
                                    Column []fakeColumn = getFakeColumn(sql,originTables,tables);
                                    if(fakeColumn.length==0){
                                        op.sql = sql;
                                    }
                                    else{
                                        String modifiedSql = modifyInsertTemplate(sql,fakeColumn);
                                        op.sql = modifiedSql;
                                    }
                                    //现在fake column都是int类型的 ~
                                    int []paraDataTypes = op.getParaDataTypes();
                                    DistributionTypeInfo[] paraDistTypeInfos = op.getParaDistTypeInfos();
                                    int []paraDataTypesModified = new int[paraDataTypes.length+fakeColumn.length];
                                    DistributionTypeInfo[] paraDistTypeInfosModified = new DistributionTypeInfo[paraDataTypes.length+fakeColumn.length];
                                    int i= 0;
                                    for(;i < paraDataTypes.length;i++){
                                        paraDataTypesModified[i] = paraDataTypes[i];
                                        paraDistTypeInfosModified[i] = paraDistTypeInfos[i];
                                    }
                                    for(int j = 0;j < fakeColumn.length;j++){
                                        paraDataTypesModified[i] = fakeColumn[j].getDataType();
                                        paraDistTypeInfosModified[i] = new DistributionTypeInfo
                                                (1,(long)fakeColumn[j].getPara1(),(long)fakeColumn[j].getPara2(),fakeColumn[j].getCardinality(),fakeColumn[j].getCoefficient());
                                        i++;
                                    }

                                    op.setParaDataTypes(paraDataTypesModified);
                                    op.setParaDistTypeInfos(paraDistTypeInfosModified);
                                    op.setWindowParaGenerators(fakeColumn);
                                    op.setFullLifeCycleParaGenerators(fakeColumn);
//                                    System.out.println("modify sql :"+op.sql);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private Column[] searchColumn(String tableName, List<Table> tables) {
        for (int i = 0; i < tables.size(); i++) {
            if (tables.get(i).getName().equals(tableName)) {
                Column[] columns = tables.get(i).getColumns();
                return columns;
            }
        }
        return null;
    }

    //返回新增加的column
    private Column[] getFakeColumn(String sql,List<Table> originTables,List<Table> tables) {
        String originalSql = sql.trim();
        sql = sql.trim().toLowerCase();
        int index1 = sql.indexOf(" into ");
        int index2 = sql.indexOf("(");
        int index3 = sql.indexOf(")");
        String tableName = originalSql.substring(index1 + 6, index2).trim();

        Column[] columnWithFakeColumn = searchColumn(tableName,tables);
        Column[] columnWithoutFakeColumn = searchColumn(tableName,originTables);
        Column[] returnColumn = new Column[columnWithFakeColumn.length-columnWithoutFakeColumn.length];
        int count = 0;
        for (Column colWithFakeCol : columnWithFakeColumn) {
            int flag = 0;
            for (Column colWithoutFakeCol : columnWithoutFakeColumn) {
                if (colWithoutFakeCol.getName().equals(colWithFakeCol.getName())) {
                    flag = 1;
                    break;
                }
            }
            if(flag == 0){
                returnColumn[count++] = colWithFakeCol;
            }
        }
        return returnColumn;
    }


    //根据新增的column修改负载中Insert模板
    private String modifyInsertTemplate(String inputline,Column[] fakeColumn) {
        String input = inputline.trim();
        inputline = inputline.trim().toLowerCase();
        int index1 = inputline.indexOf(" into ");
        int index2 = inputline.indexOf("(");
        int index3 = inputline.indexOf(")");
        String sql = null;
        String tableName = input.substring(index1 + 6, index2).trim();
        String[] columnNames = inputline.substring(index2 + 1, index3).replaceAll("[ \\t]+", "").split(",");


        sql = "INSERT INTO " + tableName + " (";
        for (String columnName : columnNames) {
            sql = sql + columnName;
            if (fakeColumn.length!=0) {
                sql = sql + ", ";
            }
        }

        int count = 0;
        for (Column column : fakeColumn) {
            sql = sql + column.getName();
            if (count != fakeColumn.length-1) {
                sql = sql + ", ";
            }
            count++;
        }

        sql = sql + ") VALUES(";
        for (int k = 0; k < columnNames.length+fakeColumn.length; k++) {
            if (k != columnNames.length+fakeColumn.length- 1) {
                sql = sql + "?, ";
            } else {
                sql = sql + "?)";
            }
        }
        return sql;

    }



}
