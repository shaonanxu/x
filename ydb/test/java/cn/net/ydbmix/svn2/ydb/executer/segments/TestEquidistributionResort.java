package cn.net.ydbmix.svn2.ydb.executer.segments;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.BitDocSet;

public class TestEquidistributionResort {
	
	public static void main(String[] args) throws IOException {
		testResort();
	}
	
	private static void testResort() throws IOException {
		YdbLeafReader r = new YdbLeafReader();
		IndexSearcher is = makeIndex();
		BitDocSet baseDocs = new BitDocSet();
		TopDocs td = is.search(new TermQuery(new Term("a", "a4")), 100);
		System.out.println(td.totalHits);
		for(ScoreDoc sd : td.scoreDocs) {
			System.out.println(is.doc(sd.doc).get("b"));
		}
		r.contex.reader = is.getIndexReader().leaves().get(0).reader();
		outPrint(EquidistributionResort.resort(r, params, schema, baseDocs, 0, 10));
		outPrint(EquidistributionResort.resort(r, params, schema, baseDocs, 10, 10));
	}
	
	private static IndexSearcher makeIndex() throws IOException {
		Directory d = new RAMDirectory();
		IndexWriterConfig conf = new IndexWriterConfig(new StandardAnalyzer());
		IndexWriter iw = new IndexWriter(d, conf);
		for(int i=0;i<1000;i ++) {
			iw.addDocument(doc(i));
		}
		iw.commit();
		iw.close();
		return new IndexSearcher(DirectoryReader.open(d));
	}
	
	private static Random r = new Random();
	
	private static Document doc(int id) {
		Document doc = new Document();
		doc.add(new StringField("a", "a" + (id%10), Store.YES));
		int v = r.nextInt(10);
		doc.add(new StoredField("b", "abc" + v));
		doc.add(new BinaryDocValuesField("c", new BytesRef("abc" + id)));
		doc.add(new NumericDocValuesField("d", v));
		return doc;
	}
	
	private static void outPrint(EquidistributionResort.Result<?> result) {
		for(Object e : result.result) {
			System.out.println(e);
		}
	}
	
	public static final class YdbLeafReader {
		public Context contex = new Context();
		
		class Context {
			LeafReader reader;
			LeafReader reader() {
				return reader;
			}
		}
		
		public long getAtomickey() {
			return 1;
		}
		
		public long getLeafkey() {
			return 1;
		}
		
	}
	
	private static SolrParams params = new SolrParams() {
		
		@Override
		public String[] getParams(String param) {
			return new String[] {"d"};
		}
		
		@Override
		public Iterator<String> getParameterNamesIterator() {
			return null;
		}
		
		@Override
		public String get(String param) {
			return null;
		}
	};
	
	private static IndexSchema schema = null;
	
}
