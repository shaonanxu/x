package cn.net.ycloud.example;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.solr.schema.IndexSchemaImpl;
import org.apache.solr.schema.SchemaField;
import org.xml.sax.InputSource;

import cn.net.ycloud.ydb.ReuseOjbect.YdbFieldRecycle;
import cn.net.ycloud.ydb.hdfsBlockDirectore.BlockFileDirectory;
import cn.net.ycloud.ydb.hdfsBlockDirectore.HdfsSsdUtils;
import cn.net.ycloud.ydb.ydbDirectory.realtime.RealTimeDirectoryPrority;

public class CreateIndex {
 public static void main(String[] args) throws IOException {
	 long a=1000000000000l;
	 System.out.println(a);
	//对接的数据结构定义
	String schemaPath="/opt/ydbsoftware/ya100/schema.xml";
	String indexPath=args[0];
	Configuration conf=new  Configuration(); 
	HdfsSsdUtils ssd=new HdfsSsdUtils(new Path[]{new Path(indexPath),new Path(indexPath)}, new HashSet<String>(),conf);
	IndexSchemaImpl schema=new IndexSchemaImpl("xxxtablename", schemaPath, new InputSource(new FileInputStream(schemaPath)));
	BlockFileDirectory directory = new BlockFileDirectory(RealTimeDirectoryPrority.GetSchedule(), RealTimeDirectoryPrority.getPriorty("lazy_write", schema.getTableName()), ssd, conf,(short)2);
	IndexWriterConfig iconf = new IndexWriterConfig(schema.getIndexAnalyzer());
	IndexWriter luceneWriter = new IndexWriter(directory, iconf);
	
	//这两个对象用于缓冲,避免反复创建lucene对象,可以减轻GC压力
	ArrayList<IndexableField> doc=new ArrayList<IndexableField>();
	ArrayList<YdbFieldRecycle> recycle=new ArrayList<YdbFieldRecycle>();

	for(int i=0;i<1000;i++)
	{
		//清空上一条记录
		doc.clear();
		{
			//content列
			SchemaField schemaField = schema.getField("content");
			if (schemaField != null) {
				schemaField.setYdbValue(doc,"170998311"+i, recycle);
			}
		}
		{
			//usernick列
			SchemaField schemaField = schema.getField("usernick");
			if (schemaField != null) {
				schemaField.setYdbValue(doc,"张三"+i, recycle);
			}
		}
		{
			//amtlong列
			SchemaField schemaField = schema.getField("amtlong");
			if (schemaField != null) {
				schemaField.setYdbValue(doc,i, recycle);
			}
		}
		luceneWriter.updateDocument(null,doc);
		//释放对象到对象池中-而不是交给gc进行回收
		YdbFieldRecycle.cleanlist(recycle);
	}
	luceneWriter.forceMerge(1);
	luceneWriter.close();
	System.out.println("finish build index "+indexPath);
}
}
