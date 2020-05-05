package db;

public class InsertTableException extends Exception{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	String msg;
	
	InsertTableException(String msg){
		this.msg = msg;
	}
	
	public String toString(){
		return "INSERT TABLE ERROR: " + msg;
	}
}
