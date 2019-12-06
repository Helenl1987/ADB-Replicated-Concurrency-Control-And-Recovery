import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class TransactionManager {
	private int time;
	private DataManager[] DM;
	private boolean[] siteStatus = new boolean[DataManager.SITECNT+1];
	private HashMap<Integer, Transaction> transactions; // TransactionID begins from 1
	private HashMap<Integer, ArrayList<Integer>> itemSites;
	private ArrayList<Operation> pendingOperations;
	
	public TransactionManager(DataManager[] _DM) {
		this.time = 0;
		this.DM = _DM;
		transactions = new HashMap<Integer, Transaction>();
		itemSites = new HashMap<Integer, ArrayList<Integer>>();
		pendingOperations = new ArrayList<Operation>();
		for(int i = 1; i <= DataManager.SITECNT; i++) {
			// suppose all the sites are up at the starting point
			siteStatus[i] = true;
			ArrayList<Integer> tmp = new ArrayList<Integer>();
			itemSites.put(i, tmp);
		}
		for(int site = 1; site <= DataManager.SITECNT; site++) {
			for(int var = 1; var <= DataManager.VARIABLECNT; var++) {
				if((var%2 == 0) || (1+(var%10) == site)) {
					itemSites.get(site).add(var);
				}
			}
		}
		
	}
	
	// execution simulator
	public void Run(BufferedReader reader) {
		String line;
		while(true) {
			if(DetectDeadLock()) {
				ExecuteReadWrite();
			}
			
			try {
				line = reader.readLine();
				if(line == null) break;
				ParseCommand(line);
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			ExecuteReadWrite();
			time += 1;
		}
		
	}
	
	private String[] ParseLine(String line) {
		// first try to get rid of comments (starting with '//')
		int idx = line.indexOf("//");
		if (idx != -1) {
			line = line.substring(0,idx);
		}
		if (line.length() == 0) {
			return null;
		}
		String[] result = new String[2];
		int first = line.indexOf("(");
		int second = line.indexOf(")");
		result[0] = line.substring(0, first);
		result[1] = line.substring(first+1, second);
		return result;
	}
	
	private int ParseID(String line) {
		String num = line.substring(1);
		return Integer.parseInt(num);
	}
	
	private void ParseCommand(String line) {
		String[] commands = ParseLine(line);
		if(commands == null) {
			return;
		}
		if(commands[0].equals("begin")) {
			int transactionID = ParseID(commands[1]);
			Begin(transactionID, false);
		} else if(commands[0].equals("beginRO")) {
			int transactionID = ParseID(commands[1]);
			Begin(transactionID, true);
		} else if(commands[0].equals("end")) {
			int transactionID = ParseID(commands[1]);
			Finish(transactionID);
		} else if(commands[0].equals("fail")){
			int siteID = Integer.parseInt(commands[1]);
			Fail(siteID);
		} else if(commands[0].equals("recover")){
			int siteID = Integer.parseInt(commands[1]);
			Recover(siteID);
		} else if(commands[0].equals("dump")) {
			if(commands[1].length() == 0) {
				DumpAll();
			} else if(commands[1].charAt(0) == 'x') {
				int idx = ParseID(commands[1]);
				DumpItem(idx);
			} else {
				int siteID = Integer.parseInt(commands[1]);
				DumpSite(siteID);
			}
		} else if(commands[0].equals("R")) {
			String[] params = commands[1].split(",");
			int transactionID = ParseID(params[0]);
			int variableID = ParseID(params[1]);
			Transaction curTrans = transactions.get(transactionID);
			if(curTrans.willAbort) {
				System.out.println("DEBUG: About to abort so ignore this read command when parsing");
				return;
			}
			Operation op = new Operation(transactionID, variableID, Operation.OperationType.READ, -1, curTrans.getStartTime());
			if(curTrans.isReadOnly()) {
				op.operationType = Operation.OperationType.READONLY;
			}
			pendingOperations.add(op);
		} else if(commands[0].equals("W")) {
			String[] params = commands[1].split(",");
			int transactionID = ParseID(params[0]);
			int variableID = ParseID(params[1]);
			int value = Integer.parseInt(params[2]);
			Transaction curTrans = transactions.get(transactionID);
			if(curTrans.willAbort) {
				System.out.println("DEBUG: About to abort so ignore this write command when parsing");
				return;
			}
			Operation op = new Operation(transactionID, variableID, Operation.OperationType.WRITE, value, curTrans.getStartTime());
			pendingOperations.add(op);
		} else {
			System.out.println("DEBUG: Invalid command name");
		}
	}
	
	private void ExecuteReadWrite() {
		ArrayList<Operation> leftOperations = new ArrayList<Operation>();
		for(int i = 0; i < pendingOperations.size(); i++) {
			Operation op = pendingOperations.get(i);
			if (transactions.get(op.transactionID).willAbort) {
				System.out.println("DEBUG: About to abort so ignore this command when execution");
				continue;
			}
			if(op.operationType == Operation.OperationType.READ) {
				if(!Read(op)){
					leftOperations.add(op);
				}
			} else if(op.operationType == Operation.OperationType.READONLY) {
				if(!ReadOnly(op)) {
					leftOperations.add(op);
				}
			} else {
				if(!Write(op)) {
					leftOperations.add(op);
				}
			}
		}
		pendingOperations = leftOperations;
	}
	
	private void Fail(int siteID) {
		if (siteStatus[siteID]) {
			DM[siteID].Fail(this.time);
			siteStatus[siteID] = false;
			for(Map.Entry<Integer, Transaction> entry: transactions.entrySet()) {
				Transaction ts = entry.getValue();
				int transactionID = entry.getKey();
				if(!ts.isReadOnly() && !ts.willAbort) {
					if(ts.visitedSites.contains(siteID)) {
						//Abort the transactions that has visited the failed site
						Abort(transactionID);
					}
				}
			}
		}
		// TODO: aborted transaction should continue to execute until commit time
		// TODO: so what it actually does in DM.Fail and DM.Recover and DM.Abort
	}
	
	private void Recover(int siteID) {
		DM[siteID].Recover(time);
		siteStatus[siteID] = true;
	}
	
	private void DumpAll() {
		for(int i = 1; i <= DataManager.SITECNT; i++) {
			DumpSite(i);
		}
	}
	
	private void DumpSite(int siteID) {
		DM[siteID].DumpAll();
	}
	
	private void DumpItem(int variableID) {
		for(int siteID: itemSites.get(variableID)) {
			DM[siteID].DumpOne(variableID);
		}
	}
	
	private void Abort(int transactionID) {
		Transaction ts = transactions.get(transactionID);
		if(!ts.willAbort) {
			for(int i=1; i < DM.length; i++) {
				DM[i].Abort(transactionID);
			}
			ts.willAbort = true;
			System.out.println("Debug: abort T"+transactionID);
		}
	}
	
	private void Begin(int transactionID, boolean isRonly) {
		// assuming transactionID increase 1 each time from 1
		Transaction ts = new Transaction(time, isRonly);
		transactions.put(transactionID, ts);
	}
	
	private void Finish(int transactionID) {
		Transaction ts = transactions.get(transactionID);
		if(ts.willAbort) {
			System.out.println("T"+transactionID+" aborts");
		} else {
			for(int i=1; i < DM.length; i++) {
				DM[i].Commit(transactionID, time);
			}
			System.out.println("T"+transactionID+" commits");
		}
		transactions.remove(transactionID);
	}
	
	private boolean Read(Operation op) {
		int variableID = op.variableID;
		int transactionID = op.transactionID;
		for(int siteID: itemSites.get(variableID)) {
			if(!siteStatus[siteID]) {
				continue;
			}
			if(DM[siteID].AcquireReadLock(transactionID, variableID)) {
				OperationResponse or = DM[siteID].Read(op);
				if(or.success) {
					transactions.get(transactionID).visitedSites.add(siteID);
					System.out.printf("x%d: %d\n", op.variableID, or.readResult);
					return true;
				} else {
					System.out.println("DEBUG: supposed to read after having read lock");
					//TODO: Suppose to release read lock here
				}
			}
		}
		return false;
	}
	
	private boolean ReadOnly(Operation op) {
		for(int siteID: itemSites.get(op.variableID)) {
			if(!siteStatus[siteID]) {
				continue;
			}
			OperationResponse or = DM[siteID].ReadOnly(op);
			if(or.success) {
				System.out.printf("x%d: %d", op.variableID, or.readResult);
				return true;
			}
		}
		return false;
	}
	
	private boolean Write(Operation op) {
		int variableID = op.variableID;
		int transactionID = op.transactionID;
		boolean success = true;
		for(int siteID: itemSites.get(variableID)) {
			if(!siteStatus[siteID]) {
				continue;
			}
			success &= DM[siteID].AcquireWriteLock(transactionID, variableID);
		}
		if (success) {
			for(int siteID: itemSites.get(variableID)) {
				if(!siteStatus[siteID]) {
					continue;
				}
				DM[siteID].Write(op);
				transactions.get(transactionID).visitedSites.add(siteID);
			}
			return true;
		} // TODO: suppose to release all the write locks assigned here
		return false;
	}
	
	private boolean hasCycle(int cur, int root, HashMap<Integer, HashSet<Integer>> graph, HashSet<Integer> path) {
		path.add(cur);
		for(int child:graph.get(cur)) {
			if(child == cur) {
				return true;
			}
			if(!path.contains(child)) {
				if(hasCycle(child, root, graph, path)) {
					return true;
				}
			}
		}
		return false;
	}
	
	private boolean DetectDeadLock() {
		// combine waiting graphs of the sites into one graph
		HashMap<Integer, HashSet<Integer>> waitGraph = new HashMap<Integer, HashSet<Integer>>();
		for (int i = 1; i <= DataManager.SITECNT; i++) {
			HashMap<Integer, HashSet<Integer>> graph = DM[i].GenWaitGraph();
			for(Map.Entry<Integer, HashSet<Integer>> entry: graph.entrySet()) {
				for(int child:entry.getValue()) {
					waitGraph.get(entry.getKey()).add(child);
				}
			}
		}
		
		if(waitGraph.isEmpty()) {
			return false;
		}
		
		// find the youngest transaction in any cycles
		int youngestTime = -1;
		int abortID = -1;
		for(Map.Entry<Integer, HashSet<Integer>> entry: waitGraph.entrySet()) {
			int transID = entry.getKey();
			int startTime = transactions.get(transID).getStartTime();
			if(hasCycle(transID, transID, waitGraph, new HashSet<Integer>())){
				if(youngestTime < startTime) {
					youngestTime = startTime;
					abortID = transID;
				}
			}
		}
		if(abortID != -1) {
			Abort(abortID);
			return true;
		}
		
		return false;
	}
}