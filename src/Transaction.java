import java.util.HashSet;

public class Transaction {
	private int startTime;
	private boolean isReadOnly;
	public boolean willAbort;
	public HashSet<Integer> visitedSites;
	
	public Transaction(int st, boolean isRonly) {
		this.startTime = st;
		this.isReadOnly = isRonly;
		this.visitedSites = new HashSet<Integer>();
	}

	public int getStartTime() {
		return startTime;
	}

	public boolean isReadOnly() {
		return isReadOnly;
	}
	
}
