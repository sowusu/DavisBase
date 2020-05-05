package db;

public class CreateTableException extends Exception{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	String msg;
	
	CreateTableException(String msg){
		this.msg = msg;
	}
	
	public String toString(){
		return "CREATE TABLE ERROR: " + msg;
	}
}
