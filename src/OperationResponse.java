/*
 * Author: Jiahui Li (jl10005)
 * Date: 2019-12-06
 * Description: OperationResponse by read and write
 * */

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