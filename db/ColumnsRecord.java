package db;

import java.util.Arrays;
import java.util.List;

public class ColumnsRecord {
	
	
	public int rowid;
	public String table_name;
	public String column_name;
	public String data_type;
	public int ord_pos;
	public boolean isNullable;
	public boolean isPrimary;
	
	ColumnsRecord(String rowid, String table_name, String column_name, String data_type, String ord_pos, String isNullable, String isPrimary){
		this.rowid = Integer.parseInt(rowid);
		this.table_name = table_name;
		this.column_name = column_name;
		this.data_type = data_type;
		this.ord_pos = Integer.parseInt(ord_pos);
		this.isNullable = isNullable.equals("yes");
		this.isPrimary = isPrimary.equals("PRI");
	}
	
	public List<String> getRecord(){
		
		return Arrays.asList(Integer.toString(rowid), 
								table_name, 
								column_name,
								data_type,
								Integer.toString(ord_pos), 
								isNullable ? "yes" : "no",
								isPrimary ? "PRI" : "NULL"
							);
		
	}

}
