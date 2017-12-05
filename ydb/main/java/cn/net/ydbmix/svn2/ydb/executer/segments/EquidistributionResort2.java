package cn.net.ydbmix.svn2.ydb.executer.segments;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocSet;

//import cn.net.ycloud.ydb.core.util.YdbLeafReader;
import cn.net.ydbmix.svn2.ydb.executer.segments.TestEquidistributionResort.YdbLeafReader;

public class EquidistributionResort2 {
	
	public final static int MAX_COUNT = 1000;
	
	private EquidistributionResort2() {}
	
	public static Result<?> resort(YdbLeafReader r, SolrParams params, IndexSchema schema, DocSet baseDocs, int offset, int limit) throws IOException{
		String[] equidFields = params.getParams("dis_field");
		if(equidFields == null || equidFields.length == 0) return null;
		else if(equidFields.length == 1) {
			String field = equidFields[0];
			LeafReader reader = r.contex.reader();
			DocValuesType docValuesType = reader.getFieldInfos().fieldInfo(field).getDocValuesType();
			switch (docValuesType) {
			case NONE:
				// 
				return null;
			case NUMERIC:
				final NumericDocValues ndv = reader.getNumericDocValues(field);
				return new Resorter<Long>().resort(r, baseDocs, offset, limit, new ValuesWrapper<Long>() {
					@Override
					public Long getValue(int index) {
						return ndv.get(index);
					}
				});
			case BINARY:
				final BinaryDocValues bdv = reader.getBinaryDocValues(field);
				return new Resorter<BytesRef>().resort(r, baseDocs, offset, limit, new ValuesWrapper<BytesRef>() {
					@Override
					public BytesRef getValue(int index) {
						return BytesRef.deepCopyOf(bdv.get(index));
					}
				});
			default:
				throw new RuntimeException(" *** Unknow type of DocValuesField the field("+field+")  ");
			}
		} else {
			LeafReader reader = r.contex.reader();
			FieldInfos fieldInfos = reader.getFieldInfos();
			MutilValuesWrapper wrapper = new MutilValuesWrapper();
			for(String f : equidFields) {
				DocValuesType docValuesType = fieldInfos.fieldInfo(f).getDocValuesType();
				switch (docValuesType) {
				case NONE:
					break;
				case NUMERIC:
					wrapper.add(DocValues.getNumeric(r.contex.reader(), f));
					break;
				case BINARY:
					wrapper.add(DocValues.getBinary(r.contex.reader(), f));
					break;
				default:
					throw new RuntimeException(" *** Unknow type of DocValuesField the field("+f+")  ");
				}
			}
			return new Resorter<String>().resort(r, baseDocs, offset, limit, wrapper);
		}
	}
	
	private static class Resorter <V>{
		
		Result<YDBScoreDoc<V>> resort(YdbLeafReader r, DocSet baseDocs, int offset, int limit, ValuesWrapper<V> wrapper){
			if (offset < 0 || limit <= 0) return null;
			Map<V, Count> valueCount = new HashMap<>();
			int totalHits = 0;
			final int capacity = offset + limit;
			YDBScoreDoc<V> top = null;
			boolean queueFull = false;
			PriorityQueue<YDBScoreDoc<V>> queue = newPriorityQueue(capacity);
			for(DocIterator it = baseDocs.iterator();it.hasNext();) {
				totalHits ++;
				int docID = it.next();
				V v = wrapper.getValue(docID);
				Count count = valueCount.get(v);
				if(count == null) {
					count = new Count(MAX_COUNT);
					valueCount.put(v, count);
				} else {
					count.dec();
				}
				if(queueFull) {
					if(count.value() < top.score) continue;
					top.doc = docID;
					top.score = count.value();
					top.fieldValue = v;
					top = queue.updateTop();
				} else {
					YDBScoreDoc<V> sd = new YDBScoreDoc<V>(docID, count.value(), r.getAtomickey(), r.getLeafkey());
					sd.fieldValue = v;
					top = queue.add(sd);
					queueFull = totalHits == capacity;
				}
			}
			return this.results(queue, totalHits, offset, limit);
		}
		
		Result<YDBScoreDoc<V>> results (PriorityQueue<YDBScoreDoc<V>> queue, int totalHits, int offset, int limit) {
			int size = totalHits < queue.size() ? totalHits : queue.size();
			if (offset >= size) return null;
			limit = Math.min(queue.size()-offset, limit);
			int len = Math.min(queue.size(), limit);
			YDBScoreDoc<V>[] results = new YDBScoreDoc[len];
			for (int i=queue.size()-offset-limit;i>0;i--) {queue.pop(); }
			for (int i = limit-1;i >= 0;i--) { 
				results[i] = queue.pop();
			}
			return new Result<YDBScoreDoc<V>>(totalHits, results);
		}
		
		PriorityQueue<YDBScoreDoc<V>> newPriorityQueue(int capacity) {
			return new PriorityQueue<YDBScoreDoc<V>>(capacity) {
				@Override
				protected boolean lessThan(YDBScoreDoc<V> a, YDBScoreDoc<V> b) {
					if(a.score > b.score) return false;
					else if(a.score < b.score) return true;
					return a.doc < b.doc;
				}
			};
		}
	}

	public static class Result<V> {
		public final int count;
		public final V[] result;

		private Result(int count, V[] results) {
			this.count = count;
			this.result = results;
		}
		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append("RESULT: Count:")
				.append(count)
				.append(", Values[");
			boolean f = true;
			for(V v : result) {
				if(f) f = false;
				else sb.append(",");
				sb.append(v.toString());
			}
			sb.append("]");
			return sb.toString();
		}
	}
	
	public static class YDBScoreDoc<V> {
		private int doc;
		private int score;
		public final long atomicKey;
		public final long leafKey;
		private V fieldValue;
		
		public YDBScoreDoc (int doc, int score, long atomicKey, long leafKey) {
			this.doc = doc;
			this.score = score;
			this.atomicKey = atomicKey;
			this.leafKey = leafKey;
		}
		public int doc() {
			return doc;
		}
		public int score() {
			return score;
		}
		public V getFieldValue() {
			return fieldValue;
		}
		
		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append("AMOTIC_KEY:").append(this.atomicKey)
				.append(",LEAF_KEY:").append(this.leafKey)
				.append(",VALUE").append(this.fieldValue.toString());
			return sb.toString();
		}
	}

	private static interface ValuesWrapper<V>{
		V getValue(int index);
	}

	private static class Count {
		int i;
		Count(int initValue){
			this.i = initValue;
		}
		int dec() {
			return --i;
		}
		int value() {
			return i;
		} 
	}
	
	private static class MutilValuesWrapper implements ValuesWrapper<String> {
		private List<Object> values = new ArrayList<>();
		@Override
		public String getValue(int index) {
			StringBuilder sb = new StringBuilder();
			for(int i=0;i<values.size();i++) {
				Object e = values.get(i);
				if(e instanceof NumericDocValues) 
					sb.append("_").append(((NumericDocValues) e).get(index));
				else if(e instanceof BinaryDocValues)
					sb.append("_").append(((BinaryDocValues) e).get(index));
			}
			return sb.toString();
		}
		
		void add(Object e) {
			this.values.add(e);
		}
	}
}
