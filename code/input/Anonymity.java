package input;

import abstraction.Column;
import abstraction.ForeignKey;
import abstraction.Table;

import java.awt.*;
import java.io.Serializable;
import java.util.*;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 匿名化：表schema匿名，对应的事务模板也匿名化
 * @author Ting Chen
 */
public class Anonymity implements Serializable {

    private static final long serialVersionUID = 1L;

    private Map<String,String> tables2Anonymity = null;
    private Map<String,String> Anonymity2tables = null;
    private Map<String, Map<String,String>> table2Columns2Anonymity =null;
    private Set usedName = null;

    public List<Table> Schema2Anonymity (List<Table> tables){
        tables2Anonymity = new HashMap<>();
        table2Columns2Anonymity = new HashMap<>();
        Anonymity2tables = new HashMap<>();
        usedName = new HashSet();

        String OriginalTableName = null;
        String AnonymousTableName = null;
        //表名匿名化：生成唯一的随机串代替原表名
        for( Table table : tables) {
            OriginalTableName = table.getName();
            AnonymousTableName = getRandomTableName(OriginalTableName.length());
            while (usedName.contains(AnonymousTableName)) {
                AnonymousTableName = getRandomTableName(OriginalTableName.length());
            }
            table.setName(AnonymousTableName);
            usedName.add(AnonymousTableName);
            tables2Anonymity.put(OriginalTableName, AnonymousTableName);
            Anonymity2tables.put(AnonymousTableName, OriginalTableName);
            table2Columns2Anonymity.put(OriginalTableName, new HashMap<>());
            //System.out.println(OriginalTableName + "  " +AnonymousTableName);
        }
        //将表中的所有列名匿名化
        for( Table table : tables) {
            AnonymousTableName = table.getName();
            OriginalTableName = Anonymity2tables.get(AnonymousTableName);
            Column[] columns = table.getColumns();
            for (Column column : columns) {
                String OriginalColumnName = column.getName();
                String AnonymousColumnName = getRandomColumnName(OriginalColumnName.length());
                while (usedName.contains(AnonymousColumnName)) {
                    AnonymousColumnName = getRandomColumnName(OriginalColumnName.length());
                }
                column.setName(AnonymousColumnName);
                usedName.add(AnonymousColumnName);
                table2Columns2Anonymity.get(OriginalTableName).put(OriginalColumnName, AnonymousColumnName);
                //System.out.println(OriginalColumnName + "  " +AnonymousColumnName);
            }
            table.setColumns(columns);
        }
        //主键名转换成相应的匿名
        for( Table table : tables){
            AnonymousTableName = table.getName();
            OriginalTableName = Anonymity2tables.get(AnonymousTableName);
            String[] OriginalPKs = table.getPrimaryKey();
            String[] AnonymousPKs = new String[OriginalPKs.length];
            for( int i=0; i<OriginalPKs.length; i++){
                AnonymousPKs[i] = table2Columns2Anonymity.get(OriginalTableName).get(OriginalPKs[i]);
            }
            table.setPrimaryKey(AnonymousPKs);
            //外键名转换成相应的匿名
            ForeignKey[] foreignKeys = table.getForeignKeys();
            for( ForeignKey foreignKey : foreignKeys) {
                String[] OriginalLocalColumns = foreignKey.getLocalColumns();
                String OriginalReferencedTable = foreignKey.getReferencedTable();
                String[] OriginalReferencedColumns = foreignKey.getReferencedColumns();
                String[] AnonymousLocalColumns = new String[OriginalLocalColumns.length];
                String[] AnonymousReferencedColumns = new String[OriginalReferencedColumns.length];
                for(int i=0; i<OriginalLocalColumns.length; i++){
                    AnonymousLocalColumns[i] = table2Columns2Anonymity.get(OriginalTableName).get(OriginalLocalColumns[i]);
                }
                String AnonymousReferencedTable = tables2Anonymity.get(OriginalReferencedTable);
                for(int i=0; i<OriginalReferencedColumns.length; i++){
                    AnonymousReferencedColumns[i] = table2Columns2Anonymity.get(OriginalReferencedTable).get(OriginalReferencedColumns[i]);
                }
                foreignKey.setLocalColumns(AnonymousLocalColumns);
                foreignKey.setReferencedTable(AnonymousReferencedTable);
                foreignKey.setReferencedColumns(AnonymousReferencedColumns);
            }
            table.setForeignKeys(foreignKeys);
        }
        return tables;
    }

    public String sql2Anonymity(String originalSql) {
        originalSql = originalSql.replaceAll("\\s+"," ");
        String sql = originalSql;
        sql = sql.toLowerCase();
        String[] originalTableNames = null;
        String[] anonymousTableNames = null;
        if(sql.matches("select[\\s\\S]+")){
            int index1 = sql.indexOf(" from ");
            int index2 = sql.indexOf(" where ");
            originalTableNames = originalSql.substring(index1 + 6, index2).replaceAll("[ \\t]+", "").split(",");
            anonymousTableNames = new String[originalTableNames.length];
            anonymousTableNames[0] = tables2Anonymity.get(originalTableNames[0]);
            String tmpsql = sql.substring(0,index1+6) + anonymousTableNames[0];
            for(int i=1;i<originalTableNames.length;i++) {
                anonymousTableNames[i] = tables2Anonymity.get(originalTableNames[i]);
                tmpsql += ", " + anonymousTableNames[i];
            }
            tmpsql += sql.substring(index2);
            sql = tmpsql;
//            String tmp[] = sql.split("\\W+");
//            for(int i=0;i<tmp.length;i++){
//                tmp[i] = tmp[i].trim();
//                if(tmp[i].equals("select")||tmp[i].equals("from")||tmp[i].equals("where")||tmp[i].equals("order")
//                ||tmp[i].equals("by")||tmp[i].equals("group")||tmp[i].equals("having")||tmp[i].equals("and")){
//                    resultSQL += tmp[i] + " ";
//                    continue;
//                }
//                for(String tableName : originalTableNames) {
//                    if(table2Columns2Anonymity.get(tableName).containsKey(tmp[i])) {
//                        tmp[i] = table2Columns2Anonymity.get(tableName).get(tmp[i]);
//                        resultSQL += tmp[i] + " ";
//                        break;
//                    }
//                }
//            }
            sql = sql + " ";//sql末尾增加一个空格，便于处理末尾单词
            Pattern pattern= Pattern.compile("\\W+");
            Matcher matcher = pattern.matcher(sql);
            String tmp;
            int indexStart = 0,indexEnd = -1;
            while(matcher.find()){
                indexEnd = matcher.start();
                tmp = sql.substring(indexStart,indexEnd).trim();
                for(String tableName : originalTableNames) {
                    if(table2Columns2Anonymity.get(tableName).containsKey(tmp)) {
                        tmp = table2Columns2Anonymity.get(tableName).get(tmp);
                        sql = sql.substring(0, indexStart) + tmp + sql.substring(indexEnd);
                        break;
                    }
                }
                indexStart = matcher.end();
            }
        }
        else if(sql.matches("(update|insert|replace|delete)[\\s\\S]+")){
            String originalTableName = null;
            String anonymousTableName = null;

            if(sql.matches("update[\\s\\S]+")) {
                int index = sql.indexOf(" set ");
                originalTableName = originalSql.substring(7, index).trim();
                anonymousTableName = tables2Anonymity.get(originalTableName);
                sql = sql.substring(0, 7) + anonymousTableName + sql.substring(index);
            }
            else if(sql.matches("(insert|replace)[\\s\\S]+")) {
                int index1 = sql.indexOf(" into ");
                int index2 = sql.indexOf("(");
                originalTableName = originalSql.substring(index1 + 6, index2).trim();
                anonymousTableName = tables2Anonymity.get(originalTableName);
                sql = sql.substring(0, index1 + 6) + anonymousTableName + sql.substring(index2);
            }
            else if(sql.matches("delete[\\s\\S]+")) {
                int index1 = sql.indexOf(" from ");
                int index2 = sql.indexOf(" where ");
                originalTableName = originalSql.substring(index1 + 6, index2).trim();
                anonymousTableName = tables2Anonymity.get(originalTableName);
                sql = sql.substring(0, index1 + 6) + anonymousTableName + sql.substring(index2);
            }

            sql = sql + " ";//sql末尾增加一个空格，便于处理末尾单词
            Pattern pattern= Pattern.compile("\\W+");
            Matcher matcher = pattern.matcher(sql);
            String tmp;
            int indexStart = 0,indexEnd = -1;
            while(matcher.find()){
                indexEnd = matcher.start();
                tmp = sql.substring(indexStart,indexEnd).trim();
                if(table2Columns2Anonymity.get(originalTableName).containsKey(tmp)) {
                    tmp = table2Columns2Anonymity.get(originalTableName).get(tmp);
                    sql = sql.substring(0, indexStart) + tmp + sql.substring(indexEnd);
                }
                indexStart = matcher.end();
            }

        }

        //System.out.println(sql.trim());
        return sql.trim();
    }

    public static String getRandomTableName(int length) {
        String str = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        Random random = new Random();
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < length; i++) {
            int number = random.nextInt(26);
            sb.append(str.charAt(number));
        }
        return sb.toString();
    }

    public static String getRandomColumnName(int length) {
        String str = "abcdefghijklmnopqrstuvwxyz";
        Random random = new Random();
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < length; i++) {
            int number = random.nextInt(26);
            sb.append(str.charAt(number));
        }
        return sb.toString();
    }



    public Map<String,String> getTables2Anonymity(){ return this.tables2Anonymity; };

    public Map<String,Map<String,String>> getTable2Columns2Anonymity(){ return this.table2Columns2Anonymity; }

    @Override
    public String toString() {
        String tmp = "";
        for(Map.Entry<String, String> entry1 : tables2Anonymity.entrySet()) {
            String originalTableName = entry1.getKey();
            String anonymousTableName = entry1.getValue();
            tmp = tmp + "\n\tTable [originalName=" + originalTableName +", anonymousName=" + anonymousTableName+", columns=[";
            Map<String, String> columns = table2Columns2Anonymity.get(originalTableName);
            for(Map.Entry<String, String> entry2 : columns.entrySet()) {
                String originalColumnName = entry2.getKey();
                String anonymousColumnName = entry2.getValue();
                tmp = tmp + "\n\t\tColumn [originalName="+originalColumnName+", anonymousName="+anonymousColumnName+"]";
            }
            tmp += "]";
        }
        return tmp;
    }
}
