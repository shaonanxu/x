package cn.net.x.example;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.RAMFile;
import org.apache.lucene.store.RAMInputStream;

import cn.net.x.example.HdfsIndexer.MyRAMDirectory;

public class TestNormalDick {
	
	public static void main(String[] args) throws IOException {
		MyRAMDirectory d = new MyRAMDirectory();
		IndexWriterConfig iconf = new IndexWriterConfig(new StandardAnalyzer());
		iconf.setUseCompoundFile(false);
		IndexWriter iw = new IndexWriter(d, iconf);
		for(int a=0;a<100000;a++)
		iw.addDocument(doc(a));
		iw.commit();
		iw.close();
		for(String name : d.listAll()) {
			FileOutputStream fout = new FileOutputStream(new File("/home/shaonanxu/temp/" + name));
			LuceneIOUtils.flush(fout, d.getRAMFile(name));
			fout.flush();
			fout.close();
		}
	}
	
	static Document doc(int i) {
		Document d = new Document();
		d.add(new StringField("a", "aaa" + i, Store.YES));
		return d;
	}
	
	static class LuceneIOUtils {
		
		public static long flush(FileOutputStream output, RAMFile file) throws IOException {
			if(file == null) return 0;
			byte[] buf = new byte[4096];
			long total = file.getLength();
			RAMInputStream input = new RAMInputStream("", file);
			while(total > 0) {
				long len = Math.min(4096, total);
				input.readBytes(buf, 0, (int)len);
				output.write(buf);
				total -= len;
			}
			input.close();
			return file.ramBytesUsed();
		}
		
	}

}
