package db;

public class DBColumn {
	
	String column_name;
	int ord_pos;
	boolean isNullable;
	String data_type;
	String table_name;
	boolean isPri;
	Object value;
	
	public DBColumn(String table_name, String column_name, String type, int ordinal_pos, boolean isNullable, boolean isPri, Object value){
		this.column_name = column_name;
		this.table_name = table_name;
		this.data_type = type;
		this.isNullable = isNullable;
		this.isPri = isPri;
		this.ord_pos = ordinal_pos;
		this.value = value;
	}

}
