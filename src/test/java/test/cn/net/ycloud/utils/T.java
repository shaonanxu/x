package test.cn.net.ycloud.utils;

import java.io.IOException;
import java.nio.file.Paths;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;

public class T {
	
	public static void main(String[] args) throws IOException {
		IndexWriterConfig conf = new IndexWriterConfig(new StandardAnalyzer());
		conf.setUseCompoundFile(false);
		FSDirectory d = FSDirectory.open(Paths.get("/home/shaonanxu/Document/temp"));
		IndexWriter iw = new IndexWriter(d, conf);
	 	for(int i=0;i<50;i++) {
			iw.addDocument(doc(i));
		}
		iw.commit();
		iw.close();
	}

	static Document doc(int i) {
		Document doc = new Document();
		doc.add(new StringField("name", "abc copy" + i, Store.YES));
		doc.add(new StringField("age", "1" + i, Store.YES));
		doc.add(new StringField("tf", "aa" + i, Store.YES));
		return doc;
	}
}
