package db;

import java.io.*;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Scanner;
import java.util.SortedMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Pattern;

import static java.lang.System.out;

/**
 *  @author Chris Irwin Davis & Nana Kwame Owusu
 *  @version 1.0
 *  <b>
 *  <p>This is an example of how to create an interactive prompt</p>
 *  <p>There is also some guidance to get started wiht read/write of
 *     binary data files using RandomAccessFile class</p>
 *  </b>
 *
 */
public class DavisBaseMain {

	/* This can be changed to whatever you like */
	static String prompt = "davisql> ";
	static String version = "v1.0b(example)";
	static String copyright = "©2016 Chris Irwin Davis";
	static boolean isExit = false;
	/*
	 * Page size for alll files is 512 bytes by default.
	 * You may choose to make it user modifiable
	 */
	static long pageSize = 512; 

	/* 
	 *  The Scanner class is used to collect user commands from the prompt
	 *  There are many ways to do this. This is just one.
	 *
	 *  Each time the semicolon (;) delimiter is entered, the userCommand 
	 *  String is re-populated.
	 */
	static Scanner scanner = new Scanner(System.in).useDelimiter(";");
	
	static List<TablesRecord> tables_records;
	static List<ColumnsRecord> columns_records;
	
	/** ***********************************************************************
	 *  Main method
	 */
    public static void main(String[] args) {


		/* Display the welcome screen */
		splashScreen();

		/* Variable to collect user input from the prompt */
		String userCommand = ""; 
		
		/*setup data store if necessary */
		File dataDir = new File("data");
		if (!dataDir.isDirectory()){
			try{
				initializeDataStore();
			}
			catch (IOException io){
				System.out.println(io.getMessage());
			}
		}
		
		/*	read in metadata tables as list of respective objects	*/
		readInMetadata();

		while(!isExit) {
			System.out.print(prompt);
			/* toLowerCase() renders command case insensitive */
			userCommand = scanner.next().replace("\n", " ").replace("\r", "").trim().toLowerCase();
			// userCommand = userCommand.replace("\n", "").replace("\r", "");
			parseUserCommand(userCommand);
		}
		System.out.println("Exiting...");

		/*write back metadata into files on exit	*/
		try {
			writeBackMetadata();
		} catch (IOException e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}
    
    /** ***********************************************************************
	 *  Utility methods
	 */
    
    private byte[] intToByteArray ( final int i ) throws IOException {      
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        dos.writeInt(i);
        dos.flush();
        return bos.toByteArray();
    }

	/** ***********************************************************************
	 *  Static method definitions
	 * @throws IOException 
	 */
    
    static void writeBackMetadata() throws IOException{
    	try {
			File dataDir = new File("data");
			dataDir.mkdir();
			String[] oldTableFiles;
			oldTableFiles = dataDir.list();
			for (int i=0; i<oldTableFiles.length; i++) {
				File anOldFile = new File(dataDir, oldTableFiles[i]); 
				if (anOldFile.getName().equals("davisbase_tables.csv") || anOldFile.getName().equals("davisbase_columns.csv")){
					anOldFile.delete();
				}
			}
			
			/* write arrayList to files */
			FileWriter tablesWriter = new FileWriter("data/davisbase_tables.csv");
			for (TablesRecord rowData : tables_records){
				tablesWriter.append(String.join(",", rowData.getRecord()));
				tablesWriter.append("\n");
			}
			
			tablesWriter.close();
			
			FileWriter columnsWriter = new FileWriter("data/davisbase_columns.csv");

			for (ColumnsRecord rowData : columns_records){
				columnsWriter.append(String.join(",", rowData.getRecord()));
				columnsWriter.append("\n");
			}
			
			columnsWriter.close();
		}
		catch (SecurityException se) {
			out.println("Unable to create data container directory");
			out.println(se);
		}
    	
    	
    }
    
    static void readInMetadata(){
    	
    	tables_records = new ArrayList<TablesRecord>();
		String row;
		try{
			BufferedReader tableReader = new BufferedReader(new FileReader("data/davisbase_tables.csv"));
			while ((row = tableReader.readLine()) != null) {
			    String[] data = row.split(",");
			    tables_records.add(new TablesRecord(data[0], data[1], data[2], data[3]));
			}
			tableReader.close();
		}
		catch (IOException io){
			System.out.println(io.getMessage());
		}
		
		columns_records = new ArrayList<ColumnsRecord>();
		String c_row;
		try{
			BufferedReader columnReader = new BufferedReader(new FileReader("data/davisbase_columns.csv"));
			while ((c_row = columnReader.readLine()) != null) {
			    String[] data = c_row.split(",");
			    columns_records.add(new ColumnsRecord(data[0], data[1], data[2], data[3], data[4], data[5], data[6]));
			}
			columnReader.close();
		}
		catch (IOException io){
			System.out.println(io.getMessage());
		}
    }
    
    /**
	 *  Setup database_tables and database_columns tables if not already existing
	 *  in current OS location.
     * @throws IOException 
	 */
    
    static void initializeDataStore() throws IOException {

		/** Create data directory at the current OS location to hold */
		try {
			File dataDir = new File("data");
			dataDir.mkdir();
			String[] oldTableFiles;
			oldTableFiles = dataDir.list();
			for (int i=0; i<oldTableFiles.length; i++) {
				File anOldFile = new File(dataDir, oldTableFiles[i]); 
				anOldFile.delete();
			}
		}
		catch (SecurityException se) {
			out.println("Unable to create data container directory");
			out.println(se);
		}
		
		/*Initialize metadata files to store table and column metadata	*/
		//BufferedReader csvReader = new BufferedReader(new FileReader("data/davisbase_tables.csv"));
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
		
		FileWriter columnsWriter = new FileWriter("data/davisbase_columns.csv");
		List<List<String>> col_rows = Arrays.asList(
												Arrays.asList("1", "davisbase_tables", "rowid", "int", "1", "no", "NULL"),
												Arrays.asList("2", "davisbase_tables", "table_name", "text", "2", "no", "NULL"),
												Arrays.asList("3", "davisbase_tables", "record_count", "int", "3", "no", "NULL"),
												Arrays.asList("4", "davisbase_tables", "root_page", "smallint", "4", "no", "NULL"),
												Arrays.asList("5", "davisbase_columns", "rowid", "int", "1", "no", "NULL"),
												Arrays.asList("6", "davisbase_columns", "table_name", "text", "2", "no", "NULL"),
												Arrays.asList("7", "davisbase_columns", "column_name", "text", "3", "no", "NULL"),
												Arrays.asList("8", "davisbase_columns", "data_type", "text", "4", "no", "NULL"),
												Arrays.asList("9", "davisbase_columns", "ordinal_position", "tinyint", "5", "no", "NULL"),
												Arrays.asList("10", "davisbase_columns", "is_nullable", "text", "6", "no", "NULL")
												);
		for (List<String> rowData : col_rows){
			columnsWriter.append(String.join(",", rowData));
			columnsWriter.append("\n");
		}
		
		columnsWriter.close();
	}

	/**
	 *  Display the splash screen
	 */
	public static void splashScreen() {
		System.out.println(line("-",80));
        System.out.println("Welcome to DavisBaseLite"); // Display the string.
		System.out.println("DavisBaseLite Version " + getVersion());
		System.out.println(getCopyright());
		System.out.println("\nType \"help;\" to display supported commands.");
		System.out.println(line("-",80));
	}
	
	/**
	 * @param s The String to be repeated
	 * @param num The number of time to repeat String s.
	 * @return String A String object, which is the String s appended to itself num times.
	 */
	public static String line(String s,int num) {
		String a = "";
		for(int i=0;i<num;i++) {
			a += s;
		}
		return a;
	}
	
	public static void printCmd(String s) {
		System.out.println("\n\t" + s + "\n");
	}
	public static void printDef(String s) {
		System.out.println("\t\t" + s);
	}
	
		/**
		 *  Help: Display supported commands
		 */
		public static void help() {
			out.println(line("*",80));
			out.println("SUPPORTED COMMANDS\n");
			out.println("All commands below are case insensitive\n");
			out.println("SHOW TABLES;");
			out.println("\tDisplay the names of all tables.\n");
			//printCmd("SELECT * FROM <table_name>;");
			//printDef("Display all records in the table <table_name>.");
			out.println("SELECT <column_list> FROM <table_name> [WHERE <condition>];");
			out.println("\tDisplay table records whose optional <condition>");
			out.println("\tis <column_name> = <value>.\n");
			out.println("DROP TABLE <table_name>;");
			out.println("\tRemove table data (i.e. all records) and its schema.\n");
			out.println("UPDATE TABLE <table_name> SET <column_name> = <value> [WHERE <condition>];");
			out.println("\tModify records data whose optional <condition> is\n");
			out.println("VERSION;");
			out.println("\tDisplay the program version.\n");
			out.println("HELP;");
			out.println("\tDisplay this help information.\n");
			out.println("EXIT;");
			out.println("\tExit the program.\n");
			out.println(line("*",80));
		}

	/** return the DavisBase version */
	public static String getVersion() {
		return version;
	}
	
	public static String getCopyright() {
		return copyright;
	}
	
	public static void displayVersion() {
		System.out.println("DavisBaseLite Version " + getVersion());
		System.out.println(getCopyright());
	}
		
	public static void parseUserCommand (String userCommand) {
		
		/* commandTokens is an array of Strings that contains one token per array element 
		 * The first token can be used to determine the type of command 
		 * The other tokens can be used to pass relevant parameters to each command-specific
		 * method inside each case statement */
		// String[] commandTokens = userCommand.split(" ");
		ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));
		

		/*
		*  This switch handles a very small list of hardcoded commands of known syntax.
		*  You will want to rewrite this method to interpret more complex commands. 
		*/
		switch (commandTokens.get(0)) {
			case "select":
				System.out.println("CASE: SELECT");
				try{
					parseQuery(userCommand);
				}
				catch (SelectException se){
					System.out.println();
					System.out.println(line("!", 80));
					System.out.println("SELECT TABLE SYNTAX ERROR: " + se.msg);
					System.out.println(line("!", 80));
					System.out.println();
				}
				catch (IOException io){
					System.out.println();
					System.out.println(line("!", 80));
					System.out.println("SELECT TABLE SYNTAX ERROR: " + io.getMessage());
					System.out.println(line("!", 80));
					System.out.println();
				}
				break;
			case "drop":
				//System.out.println("CASE: DROP");
				dropTable(commandTokens);
				break;
			case "insert":
				//System.out.println("CASE: INSERT");
				try{
					insertRecord(userCommand);
				}
				catch (InsertTableException ite){
					System.out.println();
					System.out.println(line("!", 80));
					System.out.println("INSERT TABLE SYNTAX ERROR: " + ite.msg);
					System.out.println(line("!", 80));
					System.out.println();
				}
				catch (IOException io){
					System.out.println(io.getMessage());
				}
				break;
			case "create":
				//System.out.println("CASE: CREATE");
				try{
					parseCreateTable(userCommand);
				}
				catch (CreateTableException cte){
					System.out.println();
					System.out.println(line("!", 80));
					System.out.println("CREATE TABLE SYNTAX ERROR: " + cte.msg);
					System.out.println(line("!", 80));
					System.out.println();
				}
				break;
			case "update":
				System.out.println("CASE: UPDATE");
				parseUpdate(userCommand);
				break;
			case "show":
				//System.out.println("CASE: SHOW");
				if (commandTokens.get(1).equals("tables")){
					for (TablesRecord r: tables_records){
						System.out.println(r.table_name);
					}
				}
				else System.out.println("Correct syntax is: show tables;");
				break;
			case "help":
				help();
				break;
			case "version":
				displayVersion();
				break;
			case "exit":
				isExit = true;
				break;
			case "quit":
				isExit = true;
			default:
				System.out.println("I didn't understand the command: \"" + userCommand + "\"");
				break;
		}
	}
	

	/**
	 *  Stub method for insert records
	 *  @param insertString is a String of the user input
	 * @throws IOException 
	 * @throws InsertTableException 
	 */
	public static void insertRecord(String insertString) throws IOException, InsertTableException {
		//System.out.println("STUB: This is the insertString method.");
		//System.out.println("\tParsing the string:\"" + insertString + "\"");
		
		
		
		String[] insertTokens = insertString.split(" ");
		if (insertTokens[1].equals("into") && insertTokens[2].equals("table") && insertTokens[4].equals("values") && insertTokens.length == 6){
			String table_name = insertTokens[3];
			/*validate table exists	*/
			boolean table_exists = false;
			TablesRecord insert_table = null;
			for (TablesRecord t: tables_records){
				if (t.table_name.equals(table_name)){
					table_exists = true;
					insert_table = t;
				}
			}
			
			if (table_exists && !(insert_table.table_name.equals("davisbase_tables") || insert_table.table_name.equals("davisbase_columns"))){
				String valuesString = insertTokens[5].substring(1, insertTokens[5].length() - 1);
				String[] valueTokens = valuesString.split(",");
				ArrayList<ColumnsRecord> table_columns = new ArrayList<ColumnsRecord>();
				ArrayList<DBColumn> record = new ArrayList<DBColumn>();
				for (ColumnsRecord c: columns_records){
					if (c.table_name.equals(insert_table.table_name)){
						table_columns.add(c);
					}
				}
				boolean hasPrimary = false;
				boolean hasNotNull = false;
				Object[] values = new Object[table_columns.size()];
				if (table_columns.size() >= valueTokens.length){
					for (int i = 0; i < table_columns.size(); i++){
						//check to make sure i is range of valueTokens, if not check if column is nullable and set null
						if (i < valueTokens.length){
							if (table_columns.get(i).isPrimary){
								hasPrimary = true;
							}
							if (!table_columns.get(i).isNullable){
								hasNotNull = true;
							}
							try{
								values[i] = getColumnValue(valueTokens[i],table_columns.get(i));
							}
							catch (InsertTableException ite){
								throw ite;
							}
							catch (ParseException pe){
								throw new InsertTableException(pe.getMessage());
							}
							
						}
						else{
							if (table_columns.get(i).isNullable){
								values[i] = null;
							}
							else{
								throw new InsertTableException("You are attempting to assign a NULL value to a NOT NULL Column.");
							}
						}
					}
					if (hasPrimary && hasNotNull){
						ArrayList<DBColumn> recordToInsert = new ArrayList<DBColumn>();
						for (int i = 0; i < values.length; i++){
							recordToInsert.add(new DBColumn(table_columns.get(i).table_name, 
															table_columns.get(i).column_name,
															values[i] == null ? "null" : table_columns.get(i).data_type,
															table_columns.get(i).ord_pos,
															table_columns.get(i).isNullable,
															table_columns.get(i).isPrimary,
															values[i]
															)
											);
						}
						int new_root_page = DBTable.insertRecord(recordToInsert, "data/" + insert_table.table_name + ".tbl", insert_table.root_page, insert_table.number_of_rows);
						/*update davisbase_tables and davisbase_columns files if not themselves*/
						insert_table.number_of_rows++;
						insert_table.root_page = new_root_page;
					}
					else{
						throw new InsertTableException("INSERT either has no primary key or no NOT NULL column");
					}
				}
				else{
					throw new InsertTableException("There are more values than there are columns in the specified table.");
				}
			}
			else {
				throw new InsertTableException("Cannot insert into non-existent table or system tables");
			}
		}
		else{
			throw new InsertTableException("Wrong syntax used for insert. Tip: Make sure value array has no spaces;)");
		}
	}
	
	public static Object getColumnValue(String value, ColumnsRecord column) throws InsertTableException, ParseException{
		
		if (value.equals("null")) return null;
		Pattern pattern;
		switch(column.data_type){
		case "tinyint":
			pattern = Pattern.compile("-?\\d+");
			if (pattern.matcher(value).matches()) return Byte.valueOf(value).byteValue();
		case "smallint":
			pattern = Pattern.compile("-?\\d+");
			if (pattern.matcher(value).matches()) return Short.valueOf(value).shortValue();
		case "int":
			pattern = Pattern.compile("-?\\d+");
			if (pattern.matcher(value).matches()) return Integer.valueOf(value).intValue();
		case"bigint":
			pattern = Pattern.compile("-?\\d+");
			if (pattern.matcher(value).matches()) return Long.valueOf(value).longValue();
		case "float":
			pattern = Pattern.compile("-?\\d+(\\.\\d+)?");
			if (pattern.matcher(value).matches()) return Float.valueOf(value).floatValue();
		case "double":
			pattern = Pattern.compile("-?\\d+(\\.\\d+)?");
			if (pattern.matcher(value).matches()) return Double.valueOf(value).doubleValue();
		case "year":
			pattern = Pattern.compile("\\d{4}");
			if (pattern.matcher(value).matches()) {
				short yr = Short.valueOf(value).shortValue();
				if (yr <= 2127 && yr >= 1872){
					return (byte)  (yr - 2000);
				}
				else throw new InsertTableException("Range Out of Bounds for YEAR column value. Try a year from 1872 to 2127");
				
			}
			else throw new InsertTableException("Wrong format for YEAR column value. Try number between YYYY");
		case "time":
			pattern = Pattern.compile("-?\\d+");
			if (pattern.matcher(value).matches()) return Integer.valueOf(value).intValue();
			else throw new InsertTableException("Wrong format for TIME column value. Try number between 0 and 86400000");
		case "datetime":
			pattern = Pattern.compile("\\d{4}-\\d{2}-\\d{2}_\\d{2}:\\d{2}:\\d{2}");
			if (pattern.matcher(value).matches()) {
				SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss", Locale.ENGLISH);
				Date date = formatter.parse(value);
				return date.getTime();
			}
			else throw new InsertTableException("Wrong format for DATETIME column value. Try: YYYY-MM-DD_hh:mm:ss");
		case "date":
			pattern = Pattern.compile("\\d{4}-\\d{2}-\\d{2}");
			if (pattern.matcher(value).matches()) {
				SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
				Date date = formatter.parse(value);
				return date.getTime();
			}
			else throw new InsertTableException("Wrong format for DATE column value. Try: YYYY-MM-DD");
		case "text":
			return value;
		}
		
		return null;
	}
	
	/**
	 *  Stub method for dropping tables
	 *  @param dropTableString is a String of the user input
	 */
	public static void dropTable(List<String> dropTableTokens) {
		//System.out.println("STUB: This is the dropTable method.");
		//System.out.println("\tParsing the string:\"" + dropTableString + "\"");
		if (dropTableTokens.size() != 3 || !dropTableTokens.get(1).equals("table")){
			System.out.println("DROP TABLE command syntax error ");
		}
		else{
			String table_name = dropTableTokens.get(2);
			boolean found = false;
			TablesRecord tableToRemove = null;
			for (TablesRecord t: tables_records){
				if (t.table_name.equals(table_name)){
					found = true;
					tableToRemove = t;
				}
			}
			if (found){
				//remove table
				tables_records.remove(tableToRemove);
				//remove columns
				ArrayList<ColumnsRecord> toRemove = new ArrayList<ColumnsRecord>();
				for (ColumnsRecord c: columns_records){
					if (c.table_name.equals(table_name)){
						toRemove.add(c);
					}
				}
				columns_records.removeAll(toRemove);
			}
			else{
				System.out.println("Table: " + table_name + " does not exist");
			}
		}
		
	}
	
	/**
	 *  Stub method for executing queries
	 *  @param queryString is a String of the user input
	 * @throws SelectException 
	 * @throws IOException 
	 */
	public static void parseQuery(String queryString) throws SelectException, IOException {
		//System.out.println("STUB: This is the parseQuery method");
		//System.out.println("\tParsing the string:\"" + queryString + "\"");
		String[] selectTokens = queryString.split(" ");
		ArrayList<ColumnsRecord> table_columns = null;
		boolean hasWhere = false;
		if (selectTokens.length == 8){
			hasWhere = true;
		}
		else if (selectTokens.length != 4){
			throw new SelectException("Wrong select syntax");
		}
		if (selectTokens[2].equals("from")){ 
			String column_list = selectTokens[1];
			column_list = column_list.substring(1, column_list.length() - 1);
			boolean wildCard = column_list.equals("*");
			String table_name = selectTokens[3];
			ArrayList<String> column_list_tokens = null;
			if (!wildCard){
				column_list_tokens = new ArrayList<String>(Arrays.asList(column_list.split(",")));
			
			}
			String where_column = null;
			String op  = null;
			String value = null;
			int where_column_pos = -1;
			
			if (hasWhere){
				where_column = selectTokens[5];
				op = selectTokens[6];
				value = selectTokens[7];
				if (!op.equals("=")){
					throw new SelectException("SELECT WHERE clause only support equality (=)");
				}
			}
			
			/*
			 * get all records from table	
			 * 
			 */
			
			/*validate table exists	*/
			boolean table_exists = false;
			TablesRecord insert_table = null;
			ArrayList<ArrayList<String>> all_records;
			for (TablesRecord t: tables_records){
				if (t.table_name.equals(table_name)){
					table_exists = true;
					insert_table = t;
				}
			}
			
			if (table_exists && !(insert_table.table_name.equals("davisbase_tables") || insert_table.table_name.equals("davisbase_columns"))){
				table_columns = new ArrayList<ColumnsRecord>();
				int pos = 0;
				for (ColumnsRecord c: columns_records){
					if (c.table_name.equals(insert_table.table_name)){
						if (wildCard){
							table_columns.add(c);
						}
						else{
							if (column_list_tokens.contains(c.column_name)){
								table_columns.add(c);
							}
						}
						if (c.column_name.equals(where_column)){
							where_column_pos = pos;
						}
						pos++;
					}
					
				}
				
				all_records = readAllRecords("data/" + insert_table.table_name + ".tbl");
			}
			else{
				throw new SelectException("Table does not exist");
			}
			
			
			/*filter if where clause provided	*/
			ArrayList<ArrayList<String>> filtered_records;
			if (hasWhere){
				if (where_column_pos == -1){
					throw new SelectException("Where clause column does not exist in the table");
				}
				filtered_records = new ArrayList<ArrayList<String>>();
				for (ArrayList<String> record: all_records){
					if (record.get(where_column_pos).equals(value)){
						filtered_records.add(record);
					}
				}
			}
			else filtered_records = all_records;
			
			/*Display results based on requested columns	*/
			displayRecords(filtered_records, table_columns);
		}
		else {
			throw new SelectException("Wrong select syntax");
		}
	}
	
	public static ArrayList<ArrayList<String>> readAllRecords(String table_name) throws IOException{
		
		RandomAccessFile select_file = new RandomAccessFile(table_name, "rw");
		ArrayList<ArrayList<String>> records = new ArrayList<ArrayList<String>>();
		//first table will always start at page offset 0
		int cur_page_offset = 0;
		while (cur_page_offset != -1){
			select_file.seek(cur_page_offset + 2);
			int number_of_records = select_file.readShort();
			records.addAll(getPageRecords(select_file, cur_page_offset, number_of_records));
			select_file.seek(cur_page_offset + 6);
			cur_page_offset = select_file.readInt();
		}
		
		return records;
	}
	
	public static ArrayList<ArrayList<String>> getPageRecords(RandomAccessFile raf, int page_offset, int num_records) throws IOException{
		ArrayList<ArrayList<String>> page_records = new ArrayList<ArrayList<String>>();
		for (int i = 0; i < num_records; i++){
			raf.seek(page_offset + 16 + 2*i);
			short record_offset = raf.readShort();
			ArrayList<String> record = readRecord(raf, page_offset + record_offset);
			page_records.add(record);
		}
		return page_records;
	}
	
	public static ArrayList<String> readRecord(RandomAccessFile raf, int record_location) throws IOException{
		ArrayList<String> record = new ArrayList<String>();
		int payload_start = record_location + 6;
		raf.seek(payload_start);
		int rec_elt_number = raf.read();
		int column_start  = 0;
		for (int i = 0; i < rec_elt_number; i++){
			raf.seek(payload_start + i + 1);
			byte column_code = raf.readByte();
			Object[] pair = getColumnValue(column_code, payload_start + rec_elt_number + 1 + column_start, raf);//<byte_size, Record Column Value string>
			record.add((String) pair[1]);
			column_start += (int) pair[0];
		}
		return record;
	}
	
	public static Object[] getColumnValue(byte column_code, int column_start, RandomAccessFile raf) throws IOException{
		raf.seek(column_start);
		switch(column_code){
		case 0x00:
			return new Object[]{0, "NULL"};
		case 0x01:
			return new Object[]{1, Byte.toString(raf.readByte())};
		case 0x02:
			return new Object[]{2, Short.toString(raf.readShort())};
		case 0x03:
			return new Object[]{4, Integer.toString(raf.readInt())};
		case 0x04:
			return new Object[]{8, Long.toString(raf.readLong())};
		case 0x05:
			return new Object[]{4, Float.toString(raf.readFloat())};
		case 0x06:
			return new Object[]{8, Double.toString(raf.readDouble())};
		case 0x08:
			return new Object[]{1, Integer.toString( raf.readByte() + 2000)};
		case 0x09:
			return new Object[]{4, Integer.toString( raf.readInt())};
		case 0x0A:
			LocalDate datetime = Instant.ofEpochMilli(raf.readLong()).atZone(ZoneId.systemDefault()).toLocalDate();
			DateTimeFormatter f = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm:ss");
			return new Object[]{8, datetime.format(f)};
		case 0x0B:
			LocalDate date = Instant.ofEpochMilli(raf.readLong()).atZone(ZoneId.systemDefault()).toLocalDate();
			DateTimeFormatter g = DateTimeFormatter.ofPattern("yyyy-MM-dd");
			return new Object[]{8, date.format(g)};
		default:
			int str_byte_len = column_code - 12;
			byte[] b = new byte[str_byte_len];
			raf.read(b);
			return new Object[]{str_byte_len, new String(b)};
		}
	}
	
	public static void displayRecords(ArrayList<ArrayList<String>> filtered_records, ArrayList<ColumnsRecord> table_columns){
		System.out.println(line("-", 80));
		for (ColumnsRecord c: table_columns){
			System.out.print(c.column_name);
			System.out.print("\t\t");
			System.out.print("|");
		}
		System.out.println("\n" + line("-", 80));
		for (ArrayList<String> r: filtered_records){
			//System.out.println(r.toString());
			for (ColumnsRecord c: table_columns){
				System.out.print(r.get(c.ord_pos - 1));
				System.out.print("\t\t");
				System.out.print("|");
			}
			System.out.println();
		}
	}

	/**
	 *  Stub method for updating records
	 *  @param updateString is a String of the user input
	 */
	public static void parseUpdate(String updateString) {
		System.out.println("STUB: This is the dropTable method");
		System.out.println("Parsing the string:\"" + updateString + "\"");
	}

	
	/**
	 *  Stub method for creating new tables
	 *  @param queryString is a String of the user input
	 * @throws CreateTableException 
	 */
	public static void parseCreateTable(String createTableString) throws CreateTableException {
		
		//System.out.println("STUB: Calling your method to create a table");
		//System.out.println("Parsing the string:\"" + createTableString + "\"");
		ArrayList<String> createTableTokens = new ArrayList<String>(Arrays.asList(createTableString.split(" ")));

		/* Define table file name */
		String tableFileName = createTableTokens.get(2) + ".tbl";
		
		/*check if table already exists and throw error if it does */
		File tableFile = new File("data/" + tableFileName);
		if (tableFile.isFile()){
			throw new CreateTableException("Table Already exists");
		}

		/*parse columns and their attributes */
		String tmp_token = createTableString.split("\\(")[1];
		if (tmp_token.length() <= 1){
			throw new CreateTableException("Create table syntax error");
		}
		String columnTokens = tmp_token.split("\\)")[0];
		//System.out.println(tableFileName);
		//System.out.println(columnTokens);
		
		ArrayList<DBColumn> c_tokens = getColumnTokObjects(createTableTokens.get(2), columnTokens);
		
		/*create table */
		DBTable.createTable(tableFileName, c_tokens, pageSize);
		

		
		/*  Code to insert a row in the davisbase_tables table 
		 *  i.e. database catalog meta-data 
		 */
		tables_records.add(new TablesRecord(Integer.toString(tables_records.size() + 1), 
											createTableTokens.get(2), 
											Integer.toString(0), 
											Integer.toString(0)
											)
							);
		
		/*  Code to insert rows in the davisbase_columns table  
		 *  for each column in the new table 
		 *  i.e. database catalog meta-data 
		 */
		for (DBColumn c: c_tokens){
			columns_records.add(new ColumnsRecord(
					Integer.toString(columns_records.size() + 1), 
					createTableTokens.get(2), 
					c.column_name,
					c.data_type,
					Integer.toString(c.ord_pos), 
					c.isNullable ? "yes" : "no",
					c.isPri ? "PRI"	: "NULL"
					)
				);
		}
		
		
	}
	
	public static ArrayList<DBColumn> getColumnTokObjects(String tableName, String columnTokens) throws CreateTableException{//comma-delimited string of column tokens
		ArrayList<DBColumn> c_tok_objects = new ArrayList<DBColumn>();
		String[] tokens = columnTokens.split(",");
		if (tokens.length == 0 || columnTokens.equals("")){
			throw new CreateTableException("No column values declared");
		}
		int ord_pos = 1;
		for (String tok: tokens){
			//System.out.println("Processing column: " + tok);
			c_tok_objects.add(createColumnObj(tableName, tok.trim(), ord_pos));
			ord_pos++;
		}
		return c_tok_objects;
	}
	
	public static DBColumn createColumnObj(String tableName, String c_tok, int ord_pos) throws CreateTableException{//space-separated column string element
		
		//System.out.println("Creating column: " + c_tok);
		DBColumn res;
		String[] c_elts = c_tok.split(" ");

		boolean isPri = false;
		if (ord_pos == 1 && c_elts[1].equals("int")) isPri = true;
		
		if (!isTypeValid(c_elts[1])) throw new CreateTableException("Column type is invalid");
		
		if (c_elts.length == 2){
			res = new DBColumn(tableName, c_elts[0], c_elts[1], ord_pos, isPri ? false : true, isPri, null);
		}
		else if (c_elts.length > 2){
			
			res = new DBColumn(tableName, c_elts[0], c_elts[1], ord_pos, false, isPri, null);
		}
		else{
			res = null;
			throw new CreateTableException("Column description is not complete!");
		}
		
		return res;
	}
	
	public static boolean isTypeValid(String type){
		ArrayList<String> validTypes = new ArrayList<String>(Arrays.asList("tinyint", "smallint", "int", "bigint", "year", "float", "double", "datetime", "date", "text"));
		return validTypes.contains(type);
	}
	
}