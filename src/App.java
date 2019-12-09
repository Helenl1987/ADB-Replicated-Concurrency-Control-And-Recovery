import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

/*
 * Author: Zimo Li (zl2521), Jiahui Li (jl10005)
 * Date: 2019-12-08
 * Description: application starting point 
 * */

public class App {
	public static void main(String args[]) {
		DataManager[] DM = new DataManager[DataManager.SITECNT+1];
		for(int i = 1; i <= DataManager.SITECNT; i++) {
			DM[i] = new DataManager(i);
		}
		
		BufferedWriter writer = null;
		BufferedReader reader = null;
		
		if (args.length == 1) {
			String filename = args[0];
//			String filename = "/Users/Helen/Documents/workspacejava/ADB-Replicated-Concurrency-Control-And-Recovery/test/Test6";

			try {
				reader = new BufferedReader(new FileReader(filename));
				writer = new BufferedWriter(new OutputStreamWriter(System.out));
				TransactionManager TM = new TransactionManager(DM, writer);
				TM.Run(reader);
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} 
		else if(args.length == 0){
			reader = new BufferedReader(new InputStreamReader(System.in));
			writer = new BufferedWriter(new OutputStreamWriter(System.out));
			TransactionManager TM = new TransactionManager(DM, writer);
			TM.Run(reader);
		} else {
			String filename = args[0];
			String outname = args[1];
		 
			try {
				reader = new BufferedReader(new FileReader(filename));
				File fout = new File(outname);
				FileOutputStream fos = new FileOutputStream(fout);
				writer = new BufferedWriter(new OutputStreamWriter(fos));
				TransactionManager TM = new TransactionManager(DM, writer);
				TM.Run(reader);
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
		}
		
		try {
			if(writer!=null) {
				writer.close();
			}
			if(reader!=null) {
				reader.close();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

	}
}
