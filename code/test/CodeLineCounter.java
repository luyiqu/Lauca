package test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;

public class CodeLineCounter {

	public static void main(String[] args) throws Exception {

		File[] packageFiles = new File(".//src").listFiles();
//		System.out.println(packageFiles.length);
		ArrayList<File> codeFiles = new ArrayList<File>();
		for (int i = 0; i < packageFiles.length; i++) {
//			System.out.println(i);
//			System.out.println(Arrays.asList(packageFiles[i].listFiles()));
			codeFiles.addAll(Arrays.asList(packageFiles[i].listFiles()));
		}
		int count = 0;
		for (int i = 0; i < codeFiles.size(); i++) {
			if (!codeFiles.get(i).getName().contains("java")) {
				continue;
			}
			
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(codeFiles.get(i))));
			String inputLine = null;
			while ((inputLine = br.readLine()) != null) {
//				if (inputLine.matches("[ \t\n\\{\\}]*")) {
//					continue;
//				}
				
				if (inputLine.matches("[ \t\n]*")) {
					continue;
				}
				count++;
			}
			br.close();
		}
		System.out.println(count);
	}
}
