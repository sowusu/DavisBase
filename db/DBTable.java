package db;

import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;



public class DBTable {
	
	String table_name;
	ArrayList<DBColumn> columns;//arranged in ordinal order
	long page_size;
	
	public DBTable(String table_name, long page_size){//constructor for reading table into memory
		

	}
	
	public DBTable(String table_name, ArrayList<DBColumn> columns, long page_size){//constructor for creating table
		this.table_name = table_name;
		this.page_size = page_size;
		this.columns = columns;
	}
	
	public static void createTable(String table_name, ArrayList<DBColumn> columns, long page_size){//constructor for creating table
		
		
		/*  Code to create a .tbl file to contain table data */
		try {
			/*  Create RandomAccessFile tableFile in read-write mode.
			 *  Note that this doesn't create the table file in the correct directory structure
			 */
			RandomAccessFile tableFile = new RandomAccessFile("data/" + table_name, "rw");
			tableFile.setLength(page_size);
			tableFile.seek(0);
			tableFile.write(0x0D);
			
			/* set parent pointer for first page to show it has no parent(it's root page)*/
			tableFile.seek(10);
			tableFile.write(new byte[]{(byte) 0xFF,(byte) 0xFF,(byte) 0xFF,(byte) 0xFF});
			/* set rightmost pointer for first page to show it has no right sibling*/
			tableFile.seek(6);
			tableFile.write(new byte[]{(byte) 0xFF,(byte) 0xFF,(byte) 0xFF,(byte) 0xFF});
			
		}
		catch(Exception e) {
			System.out.println(e);
		}
		
	}
	
	public static int insertRecord(ArrayList<DBColumn> record, String table_to_insert, int root_page_offset, int number_of_records) throws IOException{
		RandomAccessFile insert_file = new RandomAccessFile(table_to_insert, "rw");
		int pageOffset = getInsertPageOffset(insert_file, root_page_offset);
		int row_id = number_of_records + 1; 
		byte[] cell = generateCellFromRecord(record, row_id);
		return insertCellInFile(cell, insert_file, pageOffset, -1, root_page_offset);
		
	}
	
	/*returns new root page offset if root page changes else returns -1*/
	public static int insertCellInFile(byte[] cell, RandomAccessFile insert_file, int page_offset, int new_child_page_offset, int root_page_offset) throws IOException{
		insert_file.seek(page_offset);
		boolean isLeafNode = insert_file.readByte() == 0x0D;
		insert_file.seek(page_offset + 2);
		short number_of_cells = insert_file.readShort();
		int cell_content_offset = insert_file.readShort();
		if (cell_content_offset == 0) cell_content_offset = 512;//if not cell content, set offset to end of page
		int page_header_end = page_offset + 16 + 2*(number_of_cells + 1);//add 1 for the new cell_content_offset value
		int page_body_start = page_offset + cell_content_offset;
		if (cell.length <= page_body_start - page_header_end){//enough space for new cell
			insert_file.seek(page_body_start - cell.length);
			insert_file.write(cell);
			/*add new cell details in header	*/
			insert_file.seek(page_offset + 2);
			insert_file.writeShort(number_of_cells + 1);
			short new_cell_content_offset = (short) (cell_content_offset - cell.length);
			insert_file.seek(page_offset + 4);
			insert_file.writeShort(new_cell_content_offset);
			insert_file.seek(page_header_end - 2);
			insert_file.writeShort(new_cell_content_offset);
			
			if (!isLeafNode){
				/* if successful insert into interior page, set interior page's rightmost
				 * page to newly create child page (it may be leaf or interior page)
				 */
				insert_file.seek(page_offset + 6);
				insert_file.writeInt(new_child_page_offset);
			}
			return root_page_offset;
		}
		else{//current page is full 
			/* create new page	*/
			int new_page_offset = (int) insert_file.length();
			insert_file.setLength(insert_file.length() + 512);
			/* update old page headers	*/
			if (isLeafNode){//set right sibling ptr
				insert_file.seek(page_offset + 6);
				insert_file.writeInt(new_page_offset);
			}
			insert_file.seek(page_offset + 10);
			int parent_page_offset = insert_file.readInt();
			/* set new page headers	*/
			insert_file.seek(new_page_offset);
			if (isLeafNode){
				insert_file.write(0x0D);
			}
			else{
				insert_file.write(0x05);
			}
			insert_file.seek(new_page_offset + 2);
			insert_file.writeShort((short) 1);
			insert_file.writeShort((short) 512 - cell.length);
			insert_file.writeInt(-1);
			insert_file.seek(new_page_offset + 16);
			insert_file.writeShort((short) 512 - cell.length);
			/*insert cell in new page */
			insert_file.seek(new_page_offset + 512 - cell.length);
			insert_file.write(cell);
			if (!isLeafNode){
				/* if successful insert into interior page, set interior page's rightmost
				 * page to newly create child page (it may be leaf or interior page)
				 */
				insert_file.seek(page_offset + 6);
				insert_file.writeInt(new_child_page_offset);
			}
			/* create propagated interior node cell*/
			byte[] rowid_bytes = new byte[4];
			System.arraycopy(cell, 4, rowid_bytes, 0, 4);
			if (isLeafNode){
				System.arraycopy(cell, 2, rowid_bytes, 0, 4);
			}
			byte[] propagated_cell = combineArrays(getIntByteArray(page_offset), rowid_bytes);
			if (parent_page_offset == -1){//we're at root
				/* create new root page	*/
				int new_root_page_offset = (int) insert_file.length();
				insert_file.setLength(insert_file.length() + 512);
				
				/* set root page headers	*/
				insert_file.seek(new_root_page_offset);
				insert_file.write(0x05);
				insert_file.seek(new_root_page_offset + 10);
				insert_file.writeInt(-1);
				/* update parents of left and right child pages*/
				insert_file.seek(page_offset + 10);
				insert_file.writeInt(new_root_page_offset);
				insert_file.seek(new_page_offset + 10);
				insert_file.writeInt(new_root_page_offset);
				
				return insertCellInFile(propagated_cell, insert_file, new_root_page_offset, new_page_offset, new_root_page_offset);
				
			}
			else {
				/* update parent of newly created right child */
				insert_file.seek(new_page_offset + 10);
				insert_file.writeInt(parent_page_offset);
				return insertCellInFile(propagated_cell, insert_file, parent_page_offset, new_page_offset, root_page_offset);
			}
			
		}
	}
	
	public static int getInsertPageOffset(RandomAccessFile raf, int root_page_offset) throws IOException{
		int cur_page_offset = root_page_offset;
		while (true) {
			raf.seek(cur_page_offset);
			byte page_type = raf.readByte();
			if (page_type == 0x05){//interior node
				raf.seek(cur_page_offset + 6);
				cur_page_offset = raf.readInt();
			}
			else break;
		}
		return cur_page_offset;
	}
	
	public static byte[] generateCellFromRecord(ArrayList<DBColumn> record, int row_id){
		//iterate through each column obj and for each generate code for record header
		byte[] record_header = new byte[record.size() + 1];
		byte[] record_body = new byte[0];
		record_header[0] = (byte) record.size();
		for (int i = 1; i < record.size() + 1; i++){
			record_header[i] = getTypeCode(record.get(i-1).data_type, record.get(i-1).value);
			record_body = combineArrays(record_body, generateColByteArray(record.get(i-1)));
		}
		
		byte[] cell_payload = combineArrays(record_header, record_body);
		byte[] payload_size_bytes = getShortByteArray((short) cell_payload.length);
		byte[] row_id_bytes = getIntByteArray(row_id);
		byte[] cell_header = combineArrays(payload_size_bytes, row_id_bytes);
		return combineArrays(cell_header, cell_payload);
	}
	
	public static byte[] generateColByteArray(DBColumn c){
		ByteBuffer buf;
		byte[] bytes;
		switch(c.data_type){
		case "null":
			return new byte[0];
		case "tinyint":
			buf = ByteBuffer.allocate(1);
			buf.put((byte) c.value);
			bytes = buf.array();
			return bytes;
		case "smallint":
			buf = ByteBuffer.allocate(2);
			buf.putShort((short) c.value);
			bytes = buf.array();
			return bytes;
		case "int":
			buf = ByteBuffer.allocate(4);
			buf.putInt((int) c.value);
			bytes = buf.array();
			return bytes;
		case"bigint":
			buf = ByteBuffer.allocate(8);
			buf.putLong((long) c.value);
			bytes = buf.array();
			return bytes;
		case "float":
			buf = ByteBuffer.allocate(4);
			buf.putFloat((float) c.value);
			bytes = buf.array();
			return bytes;
		case "double":
			buf = ByteBuffer.allocate(8);
			buf.putDouble((double) c.value);
			bytes = buf.array();
			return bytes;
		case "year":
			buf = ByteBuffer.allocate(1);
			buf.put((byte) c.value);
			bytes = buf.array();
			return bytes;
		case "time":
			buf = ByteBuffer.allocate(4);
			buf.putInt((int) c.value);
			bytes = buf.array();
			return bytes;
		case "datetime":
			buf = ByteBuffer.allocate(8);
			buf.putLong((long) c.value);
			bytes = buf.array();
			return bytes;
		case "date":
			buf = ByteBuffer.allocate(8);
			buf.putLong((long) c.value);
			bytes = buf.array();
			return bytes;
		case "text":
			return ((String) c.value).getBytes();
		}
		
		return null;
	}
	
	public static byte getTypeCode(String type, Object value){
		
		switch(type){
		case "null":
			return 0x00;
		case "tinyint":
			return 0x01;
		case "smallint":
			return 0x02;
		case "int":
			return 0x03;
		case"bigint":
			return 0x04;
		case "float":
			return 0x05;
		case "double":
			return 0x06;
		case "year":
			return 0x08;
		case "time":
			return 0x09;
		case "datetime":
			return 0x0A;
		case "date":
			return 0x0B;
		case "text":
			return (byte) (12 + ((String) value).length());
		}
		
		return 0x00;
	}
	
	public static void createTableMetaData(String table_name, int page_size, int root_page) throws IOException{
		
		FileWriter tablesWriter = new FileWriter("data/davisbase_tables.csv");
		List<List<String>> table_rows = Arrays.asList(
													Arrays.asList("1", "davisbase_tables", "2", "0"),
													Arrays.asList("2", "davisbase_columns", "10", "0")
												);
		for (List<String> rowData : table_rows){
			tablesWriter.append(String.join(",", rowData));
			tablesWriter.append("\n");
		}
		
		tablesWriter.close();
		
	}
	
	private static short getShortFromByteArray(byte[] s){
		ByteBuffer wrapped = ByteBuffer.wrap(s);
		return wrapped.getShort();
	}
	
	private static byte[] combineArrays(byte[] arr1, byte[] arr2){
		byte[] newArr = new byte[arr1.length + arr2.length];
		System.arraycopy(arr1, 0, newArr, 0, arr1.length);
		System.arraycopy(arr2, 0, newArr, arr1.length, arr2.length);
		return newArr;
	}
	
	private static byte[] getShortByteArray(short value){
		ByteBuffer shBuf = ByteBuffer.allocate(2);
		shBuf.putShort(value);
		byte[] bytes = shBuf.array();
		return bytes;
	}
	
	private static byte[] getIntByteArray(int value){
		ByteBuffer intBuf = ByteBuffer.allocate(4);
		intBuf.putInt(value);
		byte[] bytes = intBuf.array();
		return bytes;
	}
	
	private static short getRecordNumber(RandomAccessFile file) throws IOException{
		byte[] rec_num = new byte[2];
		file.seek(1);
		file.read(rec_num);
		ByteBuffer wrapped = ByteBuffer.wrap(rec_num);
		return wrapped.getShort();
	}
	
	

}
