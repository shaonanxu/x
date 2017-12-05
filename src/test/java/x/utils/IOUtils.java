package x.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class IOUtils {
	
	public static String[] readFile(String path) throws IOException{
		BufferedReader br = new BufferedReader(new FileReader(new File(path)));
		String tmp = null;
		List<String> li = new ArrayList<>();
		while((tmp = br.readLine()) != null) {
			li.add(tmp);
		}
		return li.toArray(new String[li.size()]);
	}
	

}
