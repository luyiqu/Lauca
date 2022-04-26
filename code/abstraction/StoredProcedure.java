package abstraction;

public class StoredProcedure {
    private String name = null;
    private long size;
    private Column[] column = null;
    private Table[] table = null;
    //deleteLocate[0] = -1 表明存储过程中的column都是在数据库中~
//    private int[] deleteLocate = null;

    public StoredProcedure(String name,long size,Column[] column,Table[] table){
        this.name = name;
        this.size = size;
        this.column = column;
        this.table = table;
//        this.deleteLocate = deleteLocate;
    }

    public Column[] getColumn() {
        return column;
    }

    public String getName() {
        return name;
    }

    public long getSize() {
        return size;
    }

    public Table[] getTable(){ return table; }

}
