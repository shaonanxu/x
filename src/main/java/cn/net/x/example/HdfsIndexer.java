package cn.net.x.example;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.store.RAMFile;
import org.apache.lucene.store.RAMInputStream;
import org.apache.lucene.store.RAMOutputStream;

import cn.net.ycloud.ydb.hdfsBlockDirectore.BlockFileDirectory;
import cn.net.ycloud.ydb.hdfsBlockDirectore.HdfsSsdUtils;
import cn.net.ycloud.ydb.ydbDirectory.realtime.RealTimeDirectoryPrority;

public class HdfsIndexer extends Indexer{
	
	static Logger log = Logger.getLogger(HdfsIndexer.class);
	
	static String input = "/data/xsn/testindex.txt";
	static String output = "/data/xsn/output";
	
	static Configuration conf = new  Configuration();
	
	public static void main(String[] args) throws IOException {
		if(args != null && args.length == 2) {
			int maxRam = Integer.valueOf(args[1]);
			if("1".equals(args[0])) {
				test1(maxRam);
			} else if("2".equals(args[0])) {
				test2(maxRam);
			} else {
				System.out.println("ERROR");
			}
		} else {
			System.out.println("ERROR");
		}
	}
	
	
	static class LuceneIOUtils {
		
		public static long flush(FSDataOutputStream output, String name, RAMFile file) throws IOException {
			if(file == null) return 0;
			byte[] buf = new byte[4096];
			long total = file.getLength();
			RAMInputStream input = new RAMInputStream(name, file);
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
	
	static class MyRAMDirectory extends RAMDirectory {
		
		public RAMFile getRAMFile(String name) {
			return super.fileMap.get(name);
		}
		
	}
	
	static void test2(int maxRAM) throws IOException {
		MyRAMDirectory d = new MyRAMDirectory();
		IndexWriterConfig iconf = new IndexWriterConfig(new StandardAnalyzer());
		iconf.setUseCompoundFile(true);
		iconf.setRAMBufferSizeMB(maxRAM);
		IndexWriter iw = new IndexWriter(d, iconf);
		FileSystem fs = FileSystem.get(URI.create(input) ,conf);
		FSDataInputStream stream = fs.open(new Path(input));
		BufferedReader br = new BufferedReader(new InputStreamReader(stream));
		long a = Indexer.importByFile(iw, br, false);
		iw.forceMerge(1);
		iw.close();
		for(String name : d.listAll()) {
			FileSystem outfs = FileSystem.get(URI.create(output + "/" + name),conf);
			FSDataOutputStream outputStream = outfs.create(new Path(output + "/" + name));
			LuceneIOUtils.flush(outputStream, name, d.getRAMFile(name));
			outputStream.flush();
			outputStream.close();
		}
		d.close();
		long cost = System.currentTimeMillis()-a;
		log.info("COST:" +cost);
		br.close();
	}
	
	public static void test1(int maxRAM) throws IOException {
		HdfsSsdUtils ssd = new HdfsSsdUtils(new Path[]{new Path(output), new Path(output)}, new HashSet<String>(),conf);
		BlockFileDirectory directory = new BlockFileDirectory(RealTimeDirectoryPrority.GetSchedule(), 
				RealTimeDirectoryPrority.getPriorty("lazy_write", "test_table"), ssd, conf,(short)2);
		IndexWriterConfig iconf = new IndexWriterConfig(new StandardAnalyzer());
		iconf.setRAMBufferSizeMB(maxRAM);
		IndexWriter iw = new IndexWriter(directory, iconf);
		FileSystem fs = FileSystem.get(URI.create(input),conf);
		FSDataInputStream stream = fs.open(new Path(input));
		BufferedReader br = new BufferedReader(new InputStreamReader(stream));
		long a = Indexer.importByFile(iw, br, true);
		iw.forceMerge(1);
		iw.close();
		long cost = System.currentTimeMillis()-a;
//		OutputStream os = FileSystem.get(URI.create("/data/xsn/log.log"),conf).create(new Path("/data/xsn/log.log"));
//		System.setOut(new PrintStream(os));
		log.info("COST:" +cost);
		br.close();
	}
	
	static class HdfsDirectory extends FSDirectory{

		protected HdfsDirectory(java.nio.file.Path path, LockFactory lockFactory) throws IOException {
			super(path, lockFactory);
		}

		@Override
		public IndexInput openInput(String name, IOContext context) throws IOException {
			
			return null;
		}
		
	}

}
