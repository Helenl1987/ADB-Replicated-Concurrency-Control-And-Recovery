import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;

public class App {
	public static void main(String args[]) {
		DataManager[] DM = new DataManager[DataManager.SITECNT+1];
		for(int i = 1; i <= DataManager.SITECNT; i++) {
			DM[i] = new DataManager(i);
		}
		TransactionManager TM = new TransactionManager(DM);
		
		String filename = "";
		try {
			BufferedReader reader = new BufferedReader(new FileReader(filename));
			TM.Run(reader);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}