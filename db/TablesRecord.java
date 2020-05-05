package db;

import java.util.Arrays;
import java.util.List;

public class TablesRecord {
	
	public int rowid;
	public String table_name;
	public int number_of_rows;
	public int root_page;
	
	TablesRecord(String rowid, String table_name, String number_of_rows, String root_page){
		this.rowid = Integer.parseInt(rowid);
		this.table_name = table_name;
		this.number_of_rows = Integer.parseInt(number_of_rows);
		this.root_page = Integer.parseInt(root_page);
	}
	
	public List<String> getRecord(){
		
		return Arrays.asList(Integer.toString(this.rowid), 
								table_name, 
								Integer.toString(this.number_of_rows), 
								Integer.toString(this.root_page)
							);
		
	}

}
