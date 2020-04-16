package ScottsTots;

public class DBAppException extends Exception {
	public String message;

	public DBAppException(String message) {
		super(message);
		this.message = message;
	}
}
