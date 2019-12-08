/*
 * Author: Zimo Li (zl2521), Jiahui Li (jl10005)
 * Date: 2019-12-06
 * Description: Operation class
 * */
public class Operation {
    public enum OperationType {
        READ,
        WRITE,
        READONLY
    }
    public int transactionID;
    public int variableID;
    public OperationType operationType;
    public int valueToWrite;
    public int timestamp;
    
    public Operation(int transID, int varID, OperationType OpType, int value, int ts) {
    	this.transactionID = transID;
    	this.variableID = varID;
    	this.operationType = OpType;
    	this.valueToWrite = value;
    	this.timestamp = ts;
    }
}
