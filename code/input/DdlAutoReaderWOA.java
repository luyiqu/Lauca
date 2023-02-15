package input;

import abstraction.Column;
import abstraction.ForeignKey;
import abstraction.Table;
import config.Configurations;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DdlAutoReaderWOA {
    private String username = null;
    private Connection oriConn = null;
    private Connection laucaConn = null;

    public DdlAutoReaderWOA (String username, Connection oriConn, Connection laucaConn){
        super();
        this.username = username;
        this.laucaConn = laucaConn;
        this.oriConn = oriConn;
    }

    public ArrayList<String> createTables4Oracle() {
        //读取匿名化的中间状态文件，得到原表名、列名与匿名的对应
//        AnonymityInfoSerializer ais = new AnonymityInfoSerializer();
//        File infile = new File(Configurations.getAnonymitySaveFile());
//        Anonymity anonymity = new Anonymity();
//        anonymity = ais.read(infile);
//        Map<String, String > tables2Anonymity = anonymity.getTables2Anonymity();
//        Map<String, Map<String, String>> table2Columns2Anonymity = anonymity.getTable2Columns2Anonymity();

        TableInfoSerializer serializer = new TableInfoSerializer();
        List<Table> tables = serializer.read(new File(Configurations.getDataCharacteristicSaveFile()));

        //用于返回外键与索引，在导入数据后再建外键与索引
        ArrayList<String> FKs_Indexes = new ArrayList<String>();
        try {
            // 创建Statement
            Statement oriStmt = oriConn.createStatement();
            Statement laucaStmt = laucaConn.createStatement();
            //获取所有表的ddl
            String ddl = null;
            for (Table value : tables) {
                Set<String> cols = new HashSet<>();
                Table table = value;
                String originalTableName = table.getName();
                //String anonymousTableName = entry.getValue();

                String sql = "SELECT DBMS_METADATA.GET_DDL('TABLE','" + originalTableName + "') FROM DUAL";
                // 发送sql语句，执行sql语句,得到返回结果
                ResultSet rs = oriStmt.executeQuery(sql);
                if (rs.next())
                    ddl = rs.getString(1);
                ddl = ddl.replaceAll("[ \\t]+", " ");

                int index = ddl.lastIndexOf(") SEGMENT CREATION");
                ddl = ddl.substring(0, index + 1);
                //String username = "test";
                String tmp = "\"" + username.toUpperCase() + "\".";
                //CREATE TABLE “USER_NAME”."TABLE_NAME"——去除用户名
                ddl = ddl.replaceAll(tmp, "");
                //去除为主键自动创建的索引
                ddl = ddl.replaceAll("\\)\\s*USING INDEX[^,]*,", "\\),");
                ddl = ddl.replaceAll("\\)\\s*USING INDEX[^,]*\\)", "\\)\\)");
                //一行一行处理ddl语句,删除外键约束
                BufferedReader br = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(ddl.getBytes(Charset.forName("utf8"))), Charset.forName("utf8")));
                String line;
                StringBuffer strbuf = new StringBuffer();
                try {
                    while ((line = br.readLine()) != null) {
                        line = line.trim();
                        if (line.equals("")) continue;
                        else if (line.startsWith("FOREIGN KEY ") || line.startsWith("REFERENCES "))
                            continue;
                        else
                            strbuf.append(line + "\r\n");

                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                ddl = strbuf.toString();
                //列名匿名
                int index1 = 0, index2 = 0;
                while ((index1 = ddl.indexOf("\"", index2 + 1)) != -1) {
                    if ((index2 = ddl.indexOf("\"", index1 + 1)) == -1) break;
                    String originalColumnName = ddl.substring(index1 + 1, index2).toLowerCase();
                    cols.add(originalColumnName.trim());
                    //                   String anonymousColumnName = null;
//                    if(table2Columns2Anonymity.get(originalTableName).containsKey(originalColumnName))
//                        anonymousColumnName = table2Columns2Anonymity.get(originalTableName).get(originalColumnName);
//                    else anonymousColumnName = geneRandomName(originalColumnName.length());
//                    //System.out.println(originalColumnName + " " + anonymousColumnName);
//                    ddl = ddl.substring(0, index1) + anonymousColumnName + ddl.substring(index2 + 1);
                }
                //去除末尾多余的逗号
                index1 = ddl.lastIndexOf(",");
                index2 = ddl.lastIndexOf(")");
                Pattern p = Pattern.compile("\\w+");
                Matcher m = p.matcher(ddl.substring(index1, index2));
                if (!m.find()) ddl = ddl.substring(0, index1) + ')';
                //连接测试数据库，通过读取并修改的ddl语句建表
                laucaStmt.addBatch("BEGIN EXECUTE IMMEDIATE 'DROP TABLE " + originalTableName
                        + " CASCADE CONSTRAINTS PURGE'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;");
                laucaStmt.addBatch(ddl);
                //System.out.println(ddl);

                //增加字段
                Column[] columns = table.getColumns();
                for (Column column : columns) {//将真实数据库中的列与table中的列对比
                    String addColumnSQL = "ALTER TABLE " + originalTableName + " ADD ";
                    String colName = column.getName();
                    int dataType = column.getDataType();
                    if (!cols.contains(colName)) {
                        addColumnSQL += colName + " " + Int2dataType(dataType);
                        laucaStmt.addBatch(addColumnSQL);
                    }
                }

                //创建索引
                sql = "SELECT DBMS_METADATA.GET_DDL('INDEX',u.index_name) FROM USER_INDEXES u WHERE TABLE_NAME = '"
                        + originalTableName + "'";
                rs = oriStmt.executeQuery(sql);
                while (rs.next()) {
                    String indexSQL = rs.getString(1);
                    indexSQL = indexSQL.replaceAll("[ \\t]+", " ");
                    //“USER_NAME”."INDEX_NAME"——去除用户名
                    indexSQL = indexSQL.replaceAll(tmp, "");
                    indexSQL = indexSQL.replaceAll("\"", "");

                    index1 = indexSQL.indexOf("INDEX ");
                    index2 = indexSQL.indexOf(" ON ");
                    int index3 = indexSQL.indexOf(" ", index2 + 4);
                    int index4 = indexSQL.indexOf("(", index3 + 1);
                    int index5 = indexSQL.indexOf(")", index4 + 1);
                    String indexName = indexSQL.substring(index1 + 6, index2).trim();
                    //排除系统自动生成的主键约束与唯一约束上的唯一性索引(UNIQUE INDEX)
                    if (indexName.startsWith("SYS_C")) continue;
//                    indexName = "IDX_" + anonymousTableName + "_" + geneRandomName(4);
//                    String columns = indexSQL.substring(index4 + 1, index5);
//
//                    String[] columnsName = columns.split(",");
//                    columns = "(";
//                    for (int k = 0; k < columnsName.length; k++) {
//                        if (k != 0) columns += ", ";
//                        columns += table2Columns2Anonymity.get(originalTableName).get(columnsName[k].trim().toLowerCase());
//                    }
//                    columns += ")";
//                    //CREATE (?) INDEX idx_tableName_xxxx ON tableName (columnNames)
//                    indexSQL = indexSQL.substring(0, index1 + 6) + indexName + " ON "
//                            + anonymousTableName + indexSQL.substring(index3, index4) + columns;
                    //System.out.println(indexSQL);
                    //laucaStmt.addBatch(indexSQL);
                    FKs_Indexes.add(indexSQL);
                }
            }
            //创建外键
            for (Table table : tables) {
                String tableName = table.getName();
                ForeignKey[] foreignKeys = table.getForeignKeys();
                for (ForeignKey fk : foreignKeys) {
                    String fkSQL = "ALTER TABLE " + tableName + " ADD FOREIGN KEY (";
                    String[] localColumns = fk.getLocalColumns();
                    String referencedTable = fk.getReferencedTable();
                    String[] referencedColumns = fk.getReferencedColumns();
                    for (int j = 0; j < localColumns.length; j++) {
                        if (j != 0) fkSQL += ", ";
                        fkSQL += localColumns[j];
                    }
                    fkSQL += ") REFERENCES " + referencedTable + " (";
                    for (int j = 0; j < referencedColumns.length; j++) {
                        if (j != 0) fkSQL += ", ";
                        fkSQL += referencedColumns[j];
                    }
                    fkSQL += ") ON DELETE CASCADE";
                    //System.out.println(fkSQL);
                    //laucaStmt.addBatch(fkSQL);
                    FKs_Indexes.add(fkSQL);
                }
            }

            laucaStmt.executeBatch();
            System.out.println("建表成功！");
            oriStmt.close();
            laucaStmt.close();
        }catch (SQLException e) {
            e.printStackTrace();
        }
        return FKs_Indexes;
    }

    public ArrayList<String> createTables4Mysql() {
        //读取匿名化的中间状态文件，得到原表名、列名与匿名的对应
//        AnonymityInfoSerializer ais = new AnonymityInfoSerializer();
//        File infile = new File(Configurations.getAnonymitySaveFile());
//        Anonymity anonymity = new Anonymity();
//        anonymity = ais.read(infile);
//        Map<String, String > tables2Anonymity = anonymity.getTables2Anonymity();
//        Map<String, Map<String, String>> table2Columns2Anonymity = anonymity.getTable2Columns2Anonymity();

        TableInfoSerializer serializer = new TableInfoSerializer();
        List<Table> tables = serializer.read(new File(Configurations.getDataCharacteristicSaveFile()));

        //用于返回外键与索引，在导入数据后再建外键与索引
        ArrayList<String> FKs_Indexes = new ArrayList<String>();
        try {
            // 创建Statement
            Statement oriStmt = oriConn.createStatement();
            Statement laucaStmt = laucaConn.createStatement();
            //获取所有表的ddl
            String ddl = null;
            List<String> createFK = new ArrayList<>();
            laucaStmt.addBatch("SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0");
            laucaStmt.addBatch("SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0");
            for (Table value : tables) {
                Set<String> cols = new HashSet<>();
                String originalTableName = value.getName();
                //String anonymousTableName = entry.getValue();
                String sql = "SHOW CREATE TABLE " + originalTableName;
                // 发送sql语句，执行sql语句,得到返回结果
                ResultSet rs = oriStmt.executeQuery(sql);
                if (rs.next())
                    ddl = rs.getString(2);
                ddl = ddl.replaceAll("[ \\t]+", " ");

                int index = ddl.lastIndexOf(")");
                String partitionRule = null;
                if (ddl.contains("partition by")){
                    index = ddl.lastIndexOf("partition by") - 1;
                    partitionRule = ddl.substring(index + 1);
                }
                ddl = ddl.substring(0, index + 1);

                //一行一行处理ddl语句
                BufferedReader br = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(ddl.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8));
                String line;
                StringBuilder strbuf = new StringBuilder();
                ArrayList<String> constraints = new ArrayList<String>();
                try {
                    while ((line = br.readLine()) != null) {
                        line = line.trim();
                        if (line.equals("")) continue;
                        else if (line.startsWith("FOREIGN KEY ") || line.startsWith("UNIQUE KEY")
                                || line.startsWith("CONSTRAINT") || line.startsWith("KEY"))
                            constraints.add(line);
                        else
                            strbuf.append(line + "\r\n");

                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                ddl = strbuf.toString();

                Pattern commentPattern = Pattern.compile("/\\*.*\\*/");
                ddl = commentPattern.matcher(ddl).replaceAll("");

                //列名
                int index1 = 0, index2 = 0;
                while ((index1 = ddl.indexOf("`", index2 + 1)) != -1) {
                    if ((index2 = ddl.indexOf("`", index1 + 1)) == -1) break;
                    String originalColumnName = ddl.substring(index1 + 1, index2).toLowerCase();
                    cols.add(originalColumnName.trim());
//                    String anonymousColumnName = null;
//                    if(table2Columns2Anonymity.get(originalTableName).containsKey(originalColumnName)){
//                        anonymousColumnName = table2Columns2Anonymity.get(originalTableName).get(originalColumnName);
//                        ddl = ddl.substring(0, index1) + anonymousColumnName + ddl.substring(index2 + 1);
//                    }
//                    else{//可能是索引名，eg：`IDX_CUSTOMER_NAME`,`FKEY_CUSTOMER_1`
//                        String indexName = "IDX_" + anonymousTableName + "_" + geneRandomName(4);
//                        ddl = ddl.substring(0,index1) + indexName + ddl.substring(index2 + 1);
//                        index2 = index2 + (indexName.length()-originalColumnName.length());
//                    }
                }

                //去除末尾多余的逗号
                index1 = ddl.lastIndexOf(",");
                index2 = ddl.lastIndexOf(")");
                Pattern lastCommaWithBracket = Pattern.compile(",\\s+\\)");
                ddl = lastCommaWithBracket.matcher(ddl).replaceAll(")");
                //连接测试数据库，通过读取并修改的ddl语句建表
                laucaStmt.addBatch("DROP TABLE IF EXISTS " + originalTableName);
                if (partitionRule != null){
                    ddl = ddl + partitionRule;
                }
                System.out.println(ddl);

                laucaStmt.addBatch(ddl  );

                //增加字段
                Column[] columns = value.getColumns();
                for (Column column : columns) {//将真实数据库中的列与table中的列对比
                    String addColumnSQL = "ALTER TABLE " + originalTableName + " ADD COLUMN ";
                    String colName = column.getName();
                    int dataType = column.getDataType();
                    if (!cols.contains(colName)) {
                        addColumnSQL += colName + " " + Int2dataType(dataType);
                        laucaStmt.execute(addColumnSQL);
                    }
                }
                //处理外键，UNIQUE，索引等
                ArrayList<String> col = new ArrayList<String>();
                for(String cons : constraints){
                    String alter_sql = "ALTER TABLE " + originalTableName + " ADD ";
                    //去除末尾逗号
                    if(cons.endsWith(",")) cons = cons.substring(0, cons.length()-1);
                    alter_sql += cons;
                    FKs_Indexes.add(alter_sql);
                }
            }

//            for(int i=0;i<tables.size();i++) {
//                Table table = tables.get(i);
//                String tableName = table.getName();
//                ForeignKey[] foreignKeys = table.getForeignKeys();
//                for (ForeignKey fk : foreignKeys) {
//                    String fkSQL = "ALTER TABLE " + tableName + " ADD FOREIGN KEY (";
//                    String[] localColumns = fk.getLocalColumns();
//                    String referencedTable = fk.getReferencedTable();
//                    String[] referencedColumns = fk.getReferencedColumns();
//                    for (int j = 0; j < localColumns.length; j++) {
//                        if (j != 0) fkSQL += ", ";
//                        fkSQL += localColumns[j];
//                    }
//                    fkSQL += ") REFERENCES " + referencedTable + " (";
//                    for (int j = 0; j < referencedColumns.length; j++) {
//                        if (j != 0) fkSQL += ", ";
//                        fkSQL += referencedColumns[j];
//                    }
//                    fkSQL += ") ON DELETE CASCADE";
//                    //System.out.println(fkSQL);
//                    laucaStmt.addBatch(fkSQL);
//                }
//            }
            laucaStmt.addBatch("SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS");
            laucaStmt.addBatch("SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS");
            laucaStmt.executeBatch();
            System.out.println("建表成功！");
            oriStmt.close();
            laucaStmt.close();
        }catch (SQLException e ) {
            e.printStackTrace();
            System.exit(0);
        }
        return FKs_Indexes;
    }

    public ArrayList<String> createTables4Postgres() {
        //读取匿名化的中间状态文件，得到原表名、列名与匿名的对应
//        AnonymityInfoSerializer ais = new AnonymityInfoSerializer();
//        File infile = new File(Configurations.getAnonymitySaveFile());
//        Anonymity anonymity = new Anonymity();
//        anonymity = ais.read(infile);
//        Map<String, String > tables2Anonymity = anonymity.getTables2Anonymity();
//        Map<String, Map<String, String>> table2Columns2Anonymity = anonymity.getTable2Columns2Anonymity();
//        Map<String, String > anonymity2Tables = new HashMap<>();
//        for (Map.Entry<String, String> entry : tables2Anonymity.entrySet()) {
//            anonymity2Tables.put(entry.getValue(), entry.getKey());
//        }

        TableInfoSerializer serializer = new TableInfoSerializer();
        List<Table> tables = serializer.read(new File(Configurations.getDataCharacteristicSaveFile()));

        //用于返回外键与索引，在导入数据后再建外键与索引
        ArrayList<String> FKs_Indexes = new ArrayList<String>();
        try {
            // 创建Statement
            Statement oriStmt = oriConn.createStatement();
            Statement laucaStmt = laucaConn.createStatement();
            for(int i=0;i<tables.size();i++){
                Set<String> cols = new HashSet<>();
                Table table = tables.get(i);
                String originalTableName = table.getName();
                //String anonymousTableName = table.getName();
                //String originalTableName = anonymity2Tables.get(anonymousTableName);
                String ddl = "CREATE TABLE ";
                ddl += originalTableName +"(\n";
                String sql = "SELECT format_type(a.atttypid,a.atttypmod) as type,a.attname as name, a.attnotnull as notnull "
                        + "FROM pg_class as c,pg_attribute as a where c.relname = '"+originalTableName.toLowerCase()+"' "
                        + "and a.attrelid = c.oid and a.attnum>0";

                ResultSet rs = oriStmt.executeQuery(sql);
                if(rs.next()){
                    ddl += rs.getString("name");
                    ddl += " " + rs.getString("type");
                    if(rs.getString("notnull").equals("t"))
                        ddl += " NOT NULL";
                    else
                        ddl += " NULL";
                    cols.add(rs.getString("name"));
                }
                while (rs.next()) {
                    ddl += ",\n";
                    ddl += rs.getString("name");
                    ddl += " " + rs.getString("type");
                    if(rs.getString("notnull").equals("t"))
                        ddl += " NOT NULL";
                    else
                        ddl += " NULL";
                    cols.add(rs.getString("name"));
                }
                String[] primaryKeys = table.getPrimaryKey();
                if(primaryKeys.length > 0){
                    ddl += ",\nPRIMARY KEY (";
                    for(int j=0;j<primaryKeys.length;j++){
                        if(j!=0) ddl += ","+primaryKeys[j];
                        else ddl += primaryKeys[j];
                    }
                    ddl += "))";
                }else {
                    ddl += ")";
                }

                //System.out.println(ddl);
                laucaStmt.addBatch("DROP TABLE IF EXISTS " + originalTableName);
                laucaStmt.addBatch(ddl);

                //增加字段
                Column[] columns = table.getColumns();
                for(int j=0;j<columns.length;j++){//将真实数据库中的列与table中的列对比
                    String addColumnSQL = "ALTER TABLE " + originalTableName +" ADD COLUMN ";
                    String colName = columns[j].getName();
                    int dataType = columns[j].getDataType();
                    if(!cols.contains(colName)){
                        addColumnSQL += colName +" "+Int2dataType(dataType);
                        laucaStmt.addBatch(addColumnSQL);
                    }
                }
                //创建索引
                sql = "SELECT * FROM pg_indexes WHERE tablename = '" + originalTableName.toLowerCase()+"'";
                rs = oriStmt.executeQuery(sql);
                while(rs.next()){
                    if(rs.getString("indexname").contains("_pkey"))
                        continue;
                    String indexSQL = rs.getString("indexdef");
                    indexSQL = indexSQL.replaceAll("[ \\t]+", " ");
//                    int index1 = indexSQL.indexOf("INDEX ");
//                    int index2 = indexSQL.indexOf(" ON ");
//                    int index3 = indexSQL.indexOf(" ",index2+4);
//                    int index4 = indexSQL.indexOf("(",index3+1);
//                    int index5 = indexSQL.indexOf(")",index4+1);
//                    String columns = indexSQL.substring(index4+1,index5);
//                    String indexName = "idx_" + anonymousTableName + "_" + geneRandomName(4);;
//
//                    String[] columnsName = columns.split(",");
//                    columns = "(";
//                    for(int k=0;k<columnsName.length;k++){
//                        if(k!=0) columns += ", ";
//                        columns += table2Columns2Anonymity.get(originalTableName).get(columnsName[k].trim());
//                    }
//                    columns += ")";
//                    //CREATE (?) INDEX idx_tableName_xxxx ON tableName (USING ?) (columnNames)
//                    indexSQL = indexSQL.substring(0,index1+6)+indexName.toLowerCase()+" ON "
//                            + anonymousTableName + indexSQL.substring(index3,index4)+columns;
                    //System.out.println(indexSQL);
                    //laucaStmt.addBatch(indexSQL);
                    FKs_Indexes.add(indexSQL);
                }
            }
            //创建外键
            for(int i=0;i<tables.size();i++){
                Table table = tables.get(i);
                String tableName = table.getName();
                ForeignKey[] foreignKeys = table.getForeignKeys();
                for(ForeignKey fk : foreignKeys){
                    String fkSQL = "ALTER TABLE " + tableName + " ADD FOREIGN KEY (";
                    String[] localColumns = fk.getLocalColumns();
                    String referencedTable = fk.getReferencedTable();
                    String[] referencedColumns = fk.getReferencedColumns();
                    for(int j=0;j<localColumns.length;j++){
                        if(j!=0) fkSQL += ", ";
                        fkSQL += localColumns[j];
                    }
                    fkSQL += ") REFERENCES " + referencedTable +" (";
                    for(int j=0;j<referencedColumns.length;j++){
                        if(j!=0) fkSQL += ", ";
                        fkSQL += referencedColumns[j];
                    }
                    fkSQL += ") ON DELETE CASCADE";
                    //System.out.println(fkSQL);
                    //laucaStmt.addBatch(fkSQL);
                    FKs_Indexes.add(fkSQL);
                }
            }
            laucaStmt.executeBatch();
            System.out.println("建表成功！");
            oriStmt.close();
            laucaStmt.close();

        }catch (SQLException e) {
            e.printStackTrace();
        }
        return FKs_Indexes;
    }

    //随机生成索引名
    public static String geneRandomName(int length){
        String str = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        Random random = new Random();
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < length; i++) {
            int number = random.nextInt(36);
            sb.append(str.charAt(number));
        }
        return sb.toString();
    }

    //数据类型转换
    private String Int2dataType(int dataType) {

        switch (dataType) {
            case 0:
                if(Configurations.getDatabaseType().equals("oracle"))
                    return "number";
                else
                    return "integer";
            case 1:
                return "real";
            case 2:
                return "decimal";
            case 3:
                if(Configurations.getDatabaseType().equals("oracle"))
                    return "date";
                else
                    return "timestamp";
            case 4:
                return "varchar(20)";
            case 5:
                return "boolean";
            default:
                return "varchar(20)";
        }
    }

}

