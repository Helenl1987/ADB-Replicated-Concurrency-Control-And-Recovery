import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.InputStreamReader;

public class App {
	public static void main(String args[]) {
		DataManager[] DM = new DataManager[DataManager.SITECNT+1];
		for(int i = 1; i <= DataManager.SITECNT; i++) {
			DM[i] = new DataManager(i);
		}
		TransactionManager TM = new TransactionManager(DM);
		
		if (args.length > 0) {
			String filename = args[0];
//			String filename = "/Users/Helen/Documents/workspacejava/ADB-Replicated-Concurrency-Control-And-Recovery/test/Test6";
			try {
				BufferedReader reader = new BufferedReader(new FileReader(filename));
				TM.Run(reader);
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		else {
			BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
			TM.Run(reader);
		}

	}
}
