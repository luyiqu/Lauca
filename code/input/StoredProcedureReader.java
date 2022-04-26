package input;

import abstraction.Column;
import abstraction.ForeignKey;
import abstraction.StoredProcedure;
import abstraction.Table;
import config.Configurations;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class StoredProcedureReader {

    TableInfoSerializer serializer = new TableInfoSerializer();
    List<Table> tables = serializer.read(new File(Configurations.getDataCharacteristicSaveFile()));

    public List<StoredProcedure> read(File StoredProcedureReader) {
        List<StoredProcedure> storedProcedure = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(StoredProcedureReader), "utf-8"))) {
            String inputLine = null;
            while ((inputLine = br.readLine()) != null) {
                // skip the blank lines and comments
                if (inputLine.matches("[\\s]*") || inputLine.matches("[ ]*##[\\s\\S]*")) {
                    continue;
                }

                if (inputLine.matches("[ \\t]*(Procedure)[ \\t]*\\[[\\s\\S^\\]]+\\][ \\t]*")) {
                    inputLine = inputLine.substring(inputLine.indexOf('[') + 1, inputLine.lastIndexOf(']')).trim();
                    String[] procedureInfoArr = inputLine.split(";");
                    String[] columnWithTypeANDTable = procedureInfoArr[1].split(",");

                    String procedureName = procedureInfoArr[0].trim();
//                    System.out.println(procedureName);
                    int procedureSize = -1;
                    List<Integer> types = new ArrayList<>();
                    List<Column> columns = new ArrayList<>();
                    List<Table> table4StoredProcedure = new ArrayList<>();
                    for (int i = 0; i < columnWithTypeANDTable.length; i++) {
                        String colWithType = columnWithTypeANDTable[i].trim();
                        if (colWithType.toLowerCase().contains("integer")||colWithType.toLowerCase().contains("varchar")||colWithType.toLowerCase().contains("real")) {
                            String columnName = colWithType.split("[ \\t]+")[0].toLowerCase();
                            String tableName = colWithType.split("[ \\t]+")[2];
                            table4StoredProcedure.add(searchTable(tableName));
                                Column tmpCol = searchColumn(tableName,columnName);
//                                System.out.println(tmpCol.getName());
                                columns.add(tmpCol);

                        }

                    }

                    procedureSize = columns.size();
                    Column[] tmp1 = new Column[columns.size()];
                    Table[] tmp2 = new Table[table4StoredProcedure.size()];
                    storedProcedure.add(new StoredProcedure(procedureName,procedureSize,columns.toArray(tmp1),table4StoredProcedure.toArray(tmp2)));
                }
            }
                } catch(IOException e){
                    e.printStackTrace();
                }

        return storedProcedure;
    }


    private int dataType2Int(String dataType) {
        switch (dataType) {
            case "integer":
                return 0;
            case "real":
                return 1;
            case "decimal":
                return 2;
            case "datetime":
                return 3;
            case "varchar":
                return 4;
            case "boolean":
                return 5;
            default:
                return -1;
        }
    }

    private Column searchColumn(String tableName, String columnName) {
        for (int i = 0; i < tables.size(); i++) {
            if (tables.get(i).getName().equals(tableName)) {
                Column[] columns = tables.get(i).getColumns();
                for (int j = 0; j < columns.length; j++) {
//                    System.out.println(columns[j].getName());
                    if (columns[j].getName().equals(columnName)) {
                        return columns[j];
                    }
                }
            }
        }
        return null;
    }

    private Table searchTable(String tableName){
        for(int i = 0; i < tables.size();i++){
            if(tables.get(i).getName().equals(tableName)){
                return tables.get(i);
            }

        }
        return null;
    }


    public static void main(String[] args) {
        StoredProcedureReader storedProcedureReader = new StoredProcedureReader();
        List<StoredProcedure> storedProcedures = storedProcedureReader.read(new File("D://storedProcedure.txt"));
        for (StoredProcedure storedProcedure:storedProcedures){
            System.out.println(storedProcedure.getName());
            for(Column column:storedProcedure.getColumn()){
                System.out.println(column.getName());
            }
        }
        return;


    }

}



