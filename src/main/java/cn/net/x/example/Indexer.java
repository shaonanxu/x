package cn.net.x.example;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

public class Indexer {
	
	public static final String PATH = "/home/shaonanxu/Dick/ycloud/testindex";

	public static Directory testDirector() throws IOException {
		Path p = Paths.get(PATH);
		return FSDirectory.open(p);
	}
	
	public static void main(String[] args) throws IOException {
		IndexWriterConfig conf = new IndexWriterConfig(new WhitespaceAnalyzer());
		conf.setUseCompoundFile(false);
		conf.setRAMBufferSizeMB(128);
		IndexWriter iw = new IndexWriter(testDirector(), conf);
		long a = addIndexes(iw);
		iw.forceMerge(1);
		iw.commit();
		iw.close();
		System.out.println("COST:" + (System.currentTimeMillis()-a));
	}

	private static long addIndexes(IndexWriter iw) throws FileNotFoundException {
		return importByFile(iw, new BufferedReader(
				new FileReader(new File("/home/shaonanxu/Dick/ycloud/qingyun/document/3.txt"))), true);
	}
	
	protected static long importByFile(IndexWriter iw, BufferedReader br, boolean cache) {
		Random rAge = new Random();
		String[] fns = {"phone_num", "nickname", "sex", "city", "grade", "age"};
		int[] fts = {0,1,0,0,0,0};
		final int n = 700;
		try {
			if(cache) {
				String tmp = null;
				final List<Document> list = new ArrayList<>();
				while((tmp = br.readLine()) != null) {
					String[] aa = tmp.split(",");
					for(int j=0;j<n;j++) {
						Document doc = new Document();
						for(int i=0;i<fns.length;i ++) {
							int ft = fts[i];
							String value = aa[i];
							if(i == 1)
								value = value + " copy" + j;
							else if(i == 5)
								value = String.valueOf(rAge.nextInt(50));
							switch (ft) {
							case 0:
								doc.add(new StringField(fns[i], value, Store.YES));
								break;
							case 1:
								doc.add(new TextField(fns[i], value, Store.YES));
								break;
							}
						}	
						list.add(doc);
					}
				}
				System.out.println("START");
				long a = System.currentTimeMillis();
				for(Document d : list) {
					iw.addDocument(d);
				}
				return a;
			} else {
				String tmp = null;
				final List<String> txt = new ArrayList<>();
				while((tmp = br.readLine()) != null) {
					txt.add(tmp);
				}
				System.out.println("START");
				long a = System.currentTimeMillis();
				for(int z=0;z<txt.size();z++) {
					tmp = txt.get(z);
					String[] aa = tmp.split(",");
					for(int j=0;j<n;j++) {
						Document doc = new Document();
						for(int i=0;i<fns.length;i ++) {
							int ft = fts[i];
							String value = aa[i];
							if(i == 1)
								value = value + " copy" + j;
							else if(i == 5)
								value = String.valueOf(rAge.nextInt(50));
							switch (ft) {
							case 0:
								doc.add(new StringField(fns[i], value, Store.YES));
								break;
							case 1:
								doc.add(new TextField(fns[i], value, Store.YES));
								break;
							}
						}
						iw.addDocument(doc);
					}
				}
				return a;
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(br != null)
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
		}
		return 0;
	}
}
