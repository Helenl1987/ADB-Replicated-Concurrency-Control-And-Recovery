import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class DataManager {
	public enum SiteStatus {
		UP,
		DOWN
	}
	
	public class CommittedValues {
		public int value;
		public int commitTime;
		
		public CommittedValues(int value, int commitTime) {
			this.value = value;
			this.commitTime = commitTime;
		}
	}
	
	public enum LockType {
		READ,
		WRITE,
		IDLE
	}
	
	public class TransactionLockNode {
		public int transactionID;
		public LockType lockType;
		
		public TransactionLockNode(int transactionID, LockType lockType) {
			this.transactionID = transactionID;
			this.lockType = lockType;
		}
	}
	
	public class VariableLockNode {
		public LockType lockType;
		public HashSet<Integer> transactionIDs;
		public List<TransactionLockNode> waitlist;
				
		public VariableLockNode(LockType lockType) {
			this.lockType = lockType;
			this.transactionIDs = new HashSet<Integer>();
			this.waitlist = new ArrayList<TransactionLockNode>();
		}
	}
	
	public final int VARIABLECNT = 20;
	public final int SITECNT = 10;
	
	public int siteID;
	public SiteStatus siteStatus;
	public List<Integer> failureHistory;
	public List<Integer> recoveryHistory;
	
	private TreeMap<Integer, Integer> uncommitted;
	private TreeMap<Integer, List<CommittedValues>> committed;
	private HashMap<Integer, Boolean> upToDate;
	
	private HashMap<Integer, VariableLockNode> lockTable;
	private HashMap<Integer, Set<Integer>> transactionsToVariables;
	
	public DataManager(int siteID) {
		this.siteID = siteID;
		this.siteStatus = SiteStatus.UP;
		this.uncommitted = new TreeMap<Integer, Integer>();
		this.committed = new TreeMap<Integer, List<CommittedValues>>();
		this.lockTable = new HashMap<Integer, VariableLockNode>();
		this.transactionsToVariables = new HashMap<Integer, Set<Integer>>();
		
		for (int i = 1; i <= VARIABLECNT; i++) {
			if ((i % 2 == 0) || (1 + (i % 10) == this.siteID)) {
				this.committed.put(i, new ArrayList<CommittedValues>());
				this.committed.get(i).add(new CommittedValues(i*10, -1));
				this.upToDate.put(i, true);
				// copy all values to uncommitted table for possible modification
				this.uncommitted.put(i, i*10);
			}
		}
	}
	
	public void Fail(int timestamp) {
		this.siteStatus = SiteStatus.DOWN;
		this.uncommitted.clear();
		this.upToDate.clear();
		this.lockTable.clear();
		this.transactionsToVariables.clear();
		this.failureHistory.add(timestamp);
	}
	
	public void Recover(int timestamp) {
		this.siteStatus = SiteStatus.UP;
		this.recoveryHistory.add(timestamp);
		
		for (Map.Entry<Integer, List<CommittedValues>> entry: this.committed.entrySet()) {
			// copy latest values to uncommitted table for possible modification
			this.uncommitted.put(entry.getKey(), entry.getValue().get(0).value);
			if (entry.getKey() % 2 == 0) {
				this.upToDate.put(entry.getKey(), false);
			}
			else {
				this.upToDate.put(entry.getKey(), true);
			}
		}
	}
	
	public void DumpAll() {
		System.out.printf("site %d - ", this.siteID);
		for (Map.Entry<Integer, List<CommittedValues>> entry: this.committed.entrySet()) {
			System.out.printf("x%d: %d, ", entry.getKey(), entry.getValue().get(0));
		}
		System.out.printf("\n");
	}
	
	public void DumpOne(int variableID) {
		System.out.printf("site %d - x%d: %d\n", this.siteID, this.committed.get(variableID).get(0));
	}
	
	private boolean ReadLockCheck(int transactionID, int variableID) {
		if (this.lockTable.containsKey(variableID) == false) {
			return true;
		}
		VariableLockNode vln = this.lockTable.get(variableID);
		if (vln.transactionIDs.contains(transactionID)) {
			return true;
		}
		if (vln.lockType == LockType.WRITE) {
			return false;
		}
		for (TransactionLockNode tln: this.lockTable.get(variableID).waitlist) {
			if (tln.transactionID != transactionID && tln.lockType != LockType.READ) {
				return false;
			}
		}
		return true;
	}
	
	public boolean AcquireReadLock(int transactionID, int variableID) {
		if (this.upToDate.get(variableID) == false) {
			return false;
		}
		if (ReadLockCheck(transactionID, variableID)) {
			if (this.lockTable.containsKey(variableID) == false) {
				this.lockTable.put(variableID, new VariableLockNode(LockType.READ));
			}
			if (this.lockTable.get(variableID).lockType == LockType.IDLE) {
				this.lockTable.get(variableID).lockType = LockType.READ;
			}
			this.lockTable.get(variableID).transactionIDs.add(transactionID);
			return true;
		}
		else {
			for (TransactionLockNode tln: this.lockTable.get(variableID).waitlist) {
				if (tln.transactionID == transactionID && tln.lockType == LockType.READ) {
					return false;
				}
			}
			this.lockTable.get(variableID).waitlist.add(new TransactionLockNode(transactionID, LockType.READ));
			return false;
		}
	}
	
	private boolean WriteLockCheck(int transactionID, int variableID) {
		VariableLockNode vln = this.lockTable.get(variableID);
		if (vln.transactionIDs.contains(transactionID) && vln.lockType == LockType.WRITE) {
			return true;
		}
		if ((vln.lockType == LockType.IDLE || (vln.lockType == LockType.READ && vln.transactionIDs.size() == 1 && vln.transactionIDs.contains(transactionID))) == false) {
			return false;
		}
		for (TransactionLockNode tln: this.lockTable.get(variableID).waitlist) {
			if (tln.transactionID != transactionID) {
				return false;
			}
		}
		return true;
	}
	
	public boolean AcquireWriteLock(int transactionID, int variableID) {
		if (WriteLockCheck(transactionID, variableID)) {
			if (this.lockTable.containsKey(variableID) == false) {
				this.lockTable.put(variableID, new VariableLockNode(LockType.WRITE));
			}
			this.lockTable.get(variableID).lockType = LockType.WRITE;
			this.lockTable.get(variableID).transactionIDs.add(transactionID);
			return true;
		}
		else {
			for (TransactionLockNode tln: this.lockTable.get(variableID).waitlist) {
				if (tln.transactionID == transactionID && tln.lockType == LockType.WRITE) {
					return false;
				}
			}
			this.lockTable.get(variableID).waitlist.add(new TransactionLockNode(transactionID, LockType.WRITE));
			return false;
		}
	}
	
	public OperationResponse Read(Operation operation) {
		if (this.upToDate.get(operation.variableID) == false) {
			return new OperationResponse(false);
		}
		if (this.lockTable.containsKey(operation.variableID) == false || this.lockTable.get(operation.variableID).transactionIDs.contains(operation.transactionID) == false) {
			System.out.printf("Error: transaction performs read before acquiring read locks\n");
			return new OperationResponse(false);
		}
		else {
			// must read from memory to ensure read-your-writes principal
			int value = this.uncommitted.get(operation.variableID);
			return new OperationResponse(true, value);
		}
	}
	
	public OperationResponse Write(Operation operation) {
		if (this.lockTable.containsKey(operation.variableID) == false || this.lockTable.get(operation.variableID).lockType != LockType.WRITE 
				|| this.lockTable.get(operation.variableID).transactionIDs.contains(operation.transactionID) == false) {
			System.out.printf("Error: transaction performs write before acquiring write locks\n");
			return new OperationResponse(false);
		}
		else {
			this.uncommitted.put(operation.variableID, operation.valueToWrite);
			if (this.transactionsToVariables.containsKey(operation.transactionID) == false) {
				this.transactionsToVariables.put(operation.transactionID, new HashSet<Integer>());
			}
			this.transactionsToVariables.get(operation.transactionID).add(operation.variableID);
			return new OperationResponse(true);
		}
	}
	
	public OperationResponse ReadOnly(Operation operation) {
		if (this.committed.containsKey(operation.variableID) == false) {
			return new OperationResponse(false);
		}
		for (CommittedValues cv: this.committed.get(operation.variableID)) {
			if (cv.commitTime <= operation.timestamp) {
				if (operation.variableID % 2 == 0) {
					// check stale data
					for (int failTime: this.failureHistory) {
						if (failTime > cv.commitTime && failTime <= operation.timestamp) {
							return new OperationResponse(false);
						}
					}
					return new OperationResponse(true, cv.value);
				}
				else {
					return new OperationResponse(true, cv.value);
				}
			}
		}
		return new OperationResponse(false);
	}
	
	private void ReassignLocks() {
		boolean grantLock = false;
		while (true) {
			grantLock = false;
			for (Map.Entry<Integer, VariableLockNode> entry: this.lockTable.entrySet()) {
				VariableLockNode vln = entry.getValue();
				TransactionLockNode tln = entry.getValue().waitlist.get(0);
				if (vln.waitlist.isEmpty() == false) {
					if (tln.lockType == LockType.READ) {
						if (vln.lockType == LockType.IDLE || vln.lockType == LockType.READ || vln.transactionIDs.contains(tln.transactionID)) {
							grantLock = true;
							if (vln.lockType == LockType.IDLE) {
								vln.lockType = LockType.READ;
							}
							vln.transactionIDs.add(tln.transactionID);
							vln.waitlist.remove(0);
						}
					}
					else {
						if (vln.lockType == LockType.IDLE || 
								(vln.transactionIDs.contains(tln.transactionID) && (vln.lockType == LockType.WRITE || vln.transactionIDs.size() == 1))) {
							grantLock = true;
							vln.lockType = LockType.WRITE;
							vln.transactionIDs.add(tln.transactionID);
							vln.waitlist.remove(0);
						}
					}
				}
			}
			if (grantLock == false) {
				break;
			}
		}
	}
	
	public void Commit(int transactionID, int timestamp) {
		for (int variableID: this.transactionsToVariables.get(transactionID)) {
			this.committed.get(variableID).add(new CommittedValues(this.uncommitted.get(variableID), timestamp));
			this.upToDate.put(variableID, true);
		}
		this.transactionsToVariables.remove(transactionID);
		
		for (Map.Entry<Integer, VariableLockNode> entry: this.lockTable.entrySet()) {
			if (entry.getValue().transactionIDs.contains(transactionID)) {
				entry.getValue().transactionIDs.remove(transactionID);
				for (TransactionLockNode tln: entry.getValue().waitlist) {
					if (tln.transactionID == transactionID) {
						entry.getValue().waitlist.remove(tln);
					}
				}
				if (entry.getValue().transactionIDs.size() == 0) {
					entry.getValue().lockType = LockType.IDLE;
				}
			}
		}
		ReassignLocks();
	}
	
	public void Abort(int transactionID) {
		for (int variableID: this.transactionsToVariables.get(transactionID)) {
			this.uncommitted.put(variableID, this.committed.get(variableID).get(0).value);
		}
		this.transactionsToVariables.remove(transactionID);
		
		for (Map.Entry<Integer, VariableLockNode> entry: this.lockTable.entrySet()) {
			if (entry.getValue().transactionIDs.contains(transactionID)) {
				entry.getValue().transactionIDs.remove(transactionID);
				for (TransactionLockNode tln: entry.getValue().waitlist) {
					if (tln.transactionID == transactionID) {
						entry.getValue().waitlist.remove(tln);
					}
				}
				if (entry.getValue().transactionIDs.size() == 0) {
					entry.getValue().lockType = LockType.IDLE;
				}
			}
		}
		ReassignLocks();
	}
	
	private boolean VTConflict(VariableLockNode vln, TransactionLockNode tln) {
		if (vln.lockType == LockType.IDLE) {
			return false;
		}
		if (vln.lockType == LockType.WRITE) {
			if (vln.transactionIDs.contains(tln.transactionID)) {
				return false;
			}
			return true;
		}
		if (tln.lockType == LockType.READ) {
			return false;
		}
		if (vln.transactionIDs.contains(tln.transactionID) && vln.transactionIDs.size() == 1) {
			return false;
		}
		return true;
	}
	
	private boolean TTConflict(TransactionLockNode tln1, TransactionLockNode tln2) {
		if (tln1.transactionID == tln2.transactionID) {
			return false;
		}
		if (tln1.lockType == LockType.READ && tln2.lockType == LockType.READ) {
			return false;
		}
		return true;
	}
	
	public HashMap<Integer, HashSet<Integer>> GenWaitGraph() {
		HashMap<Integer, HashSet<Integer>> waitGraph = new HashMap<Integer, HashSet<Integer>>();
		for (Map.Entry<Integer, VariableLockNode> entry: this.lockTable.entrySet()) {
			VariableLockNode vln = entry.getValue();
			if (vln.lockType != LockType.IDLE && vln.waitlist.isEmpty() == false) {
				for (TransactionLockNode tln: vln.waitlist) {
					if (VTConflict(vln, tln)) {
						for (int transactionID: vln.transactionIDs) {
							if (tln.transactionID != transactionID) {
								if (waitGraph.containsKey(tln.transactionID) == false) {
									waitGraph.put(tln.transactionID, new HashSet<Integer>());
								}
								waitGraph.get(tln.transactionID).add(transactionID);
							}
						}
					}
				}
				
				for (int i = 0; i < vln.waitlist.size(); i++) {
					for (int j = 0; j < i; j++) {
						TransactionLockNode tln1 = vln.waitlist.get(i);
						TransactionLockNode tln2 = vln.waitlist.get(j);
						if (TTConflict(tln1, tln2)) {
							if (waitGraph.containsKey(tln1.transactionID) == false) {
								waitGraph.put(tln1.transactionID, new HashSet<Integer>());
							}
							waitGraph.get(tln1.transactionID).add(tln2.transactionID);
						}
					}
				}
			}
		}
		return waitGraph;
	}
}











