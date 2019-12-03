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
}
