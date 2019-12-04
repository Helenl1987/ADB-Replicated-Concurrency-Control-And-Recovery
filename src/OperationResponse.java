public class OperationResponse {
	public boolean success;
	public int readResult;
	
	public OperationResponse(boolean success) {
		this.success = success;
	}
	
	public OperationResponse(boolean success, int readResult) {
		this.success = success;
		this.readResult = readResult;
	}
}