package abstraction;

import java.io.Serializable;
import java.util.Arrays;

public class ForeignKey implements Serializable, Cloneable {

	private static final long serialVersionUID = 1L;

	private String[] localColumns = null;
	private String referencedTable = null;
	// 'referencedColumns' must be the primary key of 'referencedTable'
	private String[] referencedColumns = null;
	private double averageReferenceScale;

	public ForeignKey(String[] localColumns, String referencedTable, String[] referencedColumns,
			double averageReferenceScale) {
		super();
		this.localColumns = localColumns;
		this.referencedTable = referencedTable;
		this.referencedColumns = referencedColumns;
		this.averageReferenceScale = averageReferenceScale;
	}

	public String[] getLocalColumns() {
		return localColumns;
	}

	public String getReferencedTable() {
		return referencedTable;
	}

	public String[] getReferencedColumns() {
		return referencedColumns;
	}

	public double getAverageReferenceScale() {
		return averageReferenceScale;
	}

	public void setAverageReferenceScale(double averageReferenceNumber) {
		this.averageReferenceScale = averageReferenceNumber;
	}

	public void setLocalColumns(String[] localColumns) { this.localColumns = localColumns; };

	public void setReferencedTable(String referencedTable) { this.referencedTable = referencedTable; }

	public void setReferencedColumns(String[] referencedColumns) { this.referencedColumns = referencedColumns; }

	// 因为ForeignKey中的信息不会被更新，所以可以被共享
	@Override
	public ForeignKey clone() {
		try {
			return (ForeignKey)super.clone();
		} catch (CloneNotSupportedException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public String toString() {
		return "\n\t\t\tForeignKey [localColumns=" + Arrays.toString(localColumns) + ", referencedTable=" 
				+ referencedTable + ", referencedColumns=" + Arrays.toString(referencedColumns) 
				+ ", averageReferenceScale=" + averageReferenceScale + "]";
	}
}
