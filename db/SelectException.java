package db;

public class SelectException extends Exception{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	String msg;
	
	SelectException(String msg){
		this.msg = msg;
	}
	
	public String toString(){
		return "SELECT TABLE ERROR: " + msg;
	}
}
