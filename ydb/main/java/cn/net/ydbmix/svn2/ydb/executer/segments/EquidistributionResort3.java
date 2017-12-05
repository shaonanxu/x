package cn.net.ydbmix.svn2.ydb.executer.segments;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.PriorityQueue;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.compare.UniqTypeNum;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.BitDocSet;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocSet;

import cn.net.ycloud.ydb.core.util.YdbLeafReader;
import cn.net.ydbmix.svn1.ydb.search.selectTopNdatarow.SortParse;
import cn.net.ydbmix.svn1.ydb.search.selectTopNdatarow.TopNTermNum;
import cn.net.ydbmix.svn1.ydb.search.selectTopNdatarow.TopNTermNumCmp;
import cn.net.ydbmix.svn2.ydb.core.index.YdbDocValuesFieldsList;
import cn.net.ydbmix.svn2.ydb.core.index.YdbInvertDoclist;
import cn.net.ydbmix.svn2.ydb.core.index.YdbInvertDoclist.InvertDocs;
import cn.net.ydbmix.svn2.ydb.core.index.YdbInvertDoclist.SkipEnum;

public class EquidistributionResort3 {
	
	public static class UnsupportInvertDoclistException extends Exception {
		private static final long serialVersionUID = 1L;
		public UnsupportInvertDoclistException(String field) {
			super(" *** Unsupport invert doclist, number of terms in the field(" + field + ") more than " + MAX_COUNT + " ) *** ");
		}
	}
	
	public final static int MAX_COUNT = YdbInvertDoclist.TERM_LIMIT;
	private static Resorter<String> resorter = new Resorter<String>();
	private static Resorter4Sort<String> resorter4Sort = new Resorter4Sort<>();
	
	private EquidistributionResort3() {}
	
	public static Result<?> resortStage1(YdbLeafReader r, SolrParams params, String field, IndexSchema schema, DocSet baseDocs, int offset, int limit) 
			throws UnsupportInvertDoclistException, IOException{
		String[] sortFields = params.getParams(FacetParams.FACET_CROSS_SORT_FL);
		if(sortFields == null) {
			return resortStage1(r, field, schema, baseDocs, null, offset, limit);
		} else {
			SortParse sort = new SortParse(params, schema);
   			if (sort.isNeedSortInsegment) {
   				int howMany = offset + limit;
   				final Comparator<TopNTermNum> comparator = Collections.reverseOrder(sort.numCmp);
   				PriorityQueue<TopNTermNum> queue = new PriorityQueue<TopNTermNum>(howMany) {
					@Override
					protected boolean lessThan(TopNTermNum a, TopNTermNum b) {
						return comparator.compare(a, b)>0 ? false : true;
					}
   				};
   	   			YdbDocValuesFieldsList sortufs = new YdbDocValuesFieldsList(sort.sorts, r.parent,schema, r, r.contex.reader(), params);
   				final int len = sort.sorts.length;
   				TopNTermNum top = null;
   				TopNTermNum tempTermNum = new TopNTermNum(0, null); 
   				boolean queueFull = false;
   				int totalHits;
   				TopNTermNumCmp numCmp = sort.numCmp;
   				final DocValuesGetOrdInterface[] labelParser = sortufs.ord;
   				for(DocIterator it = baseDocs.iterator();it.hasNext();) {
   					int docID = it.nextDoc();
   					Number[] tmd = new Number[len];
   					for (int i = 0; i < len; i++) {
   						tmd[i] = labelParser[i].getFirstOrd(docID);
					}
   					if(queueFull) {
   						tempTermNum.docid = docID;
   						tempTermNum.tm = tmd;
   						if(comparator.compare(top, tempTermNum) < 0) continue;
   						top.tm = tmd;
   						top.docid = docID;
   						top = queue.updateTop();
   					} else {
						top = queue.add(new TopNTermNum(docID, tmd));
   						queueFull = totalHits == howMany;
   					}
   				}
   				List<TopNTermNum> orderDocIDs = new ArrayList<>();
   				DocSet newBaseDocs = new BitDocSet(); 
   				for (int i = queue.size()-1;i >= 0;i--) { 
   					TopNTermNum e = queue.pop();
   					newBaseDocs.add(e.docid);
   					orderDocIDs.add(e);
   				}
   				return resortStage1(r, field, schema, newBaseDocs, orderDocIDs.iterator(), offset, limit);
   			} else {
   				return resortStage1(r, field, schema, baseDocs, null, offset, limit);
   			}
		}
	}
	
	private static Result<?> resortStage1(YdbLeafReader r,  String field, IndexSchema schema, DocSet baseDocs, Iterator<TopNTermNum> it, int offset, int limit) 
			throws UnsupportInvertDoclistException, IOException{
		// 转正排
		final Map<Integer, String> valuesMap = new HashMap<>();
		boolean isFetch = YdbInvertDoclist.fetch(schema, field, r.contex.reader(), "", new InvertDocs() {
			@Override
			public long each(String termKey, PostingsEnum docsEnum, int freq, int termcount) throws IOException {
				SkipEnum skipEnum = new SkipEnum(docsEnum, baseDocs);
				int docId = 0;
				int howMany = offset + limit;
				while ((docId = skipEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS && howMany-- > 0) {
					valuesMap.put(docId, termKey);
				}
				return 0;
			}
		}, baseDocs.size());
		if(!isFetch) 
			throw new UnsupportInvertDoclistException(field);
		
		ValuesWrapper<String> wrapper = new ValuesWrapper<String>() {
			@Override
			public String getValue(int index) {
				return valuesMap.get(index);
			}
		};
		if(it == null)
			return resorter.resortStage1(r, valuesMap.keySet().iterator(), offset, limit, wrapper);
		else 
			return resorter4Sort.resortStage1(r, it, offset, limit, wrapper);
	}
	
	public static Result<?> resortStage2(SolrParams params, IndexSchema schema, Result<?>[] results, int offset, int limit) {
		int sum = 0;
		for(Result<?> r : results) {
			sum += r.count;
		}
		int howMany = offset + limit;
		if(offset >= sum || howMany > sum) return null;
		String[] sortFields = params.getParams(FacetParams.FACET_CROSS_SORT_FL);
		if(sortFields == null) {
			return resorter.resortStage2(howMany, results, offset, limit);
		} else {
			SortParse sort = new SortParse(params, schema);
   			if (sort.isNeedSortInsegment) {
   				final boolean[] isdesc = sort.numCmp.isdesc;
   				return resorter4Sort.resortStage2(sum, howMany, results, offset, limit, new Comparator<YDBScoreDoc4Sort<String,Number[]>>() {
					@Override
					public int compare(YDBScoreDoc4Sort<String, Number[]> a, YDBScoreDoc4Sort<String, Number[]> b) {
						return UniqTypeNum.compare(a.sortValues, b.sortValues, isdesc);
					}
				});
   			} else {
   				return resorter.resortStage2(howMany, results, offset, limit);
   			}
		}
	}
	
	private static class Resorter <V>{
		
		Result<YDBScoreDoc<V>> resortStage1(YdbLeafReader r, Iterator<Integer> docIterator, int offset, int limit, ValuesWrapper<V> wrapper){
			if (offset < 0 || limit <= 0) return null;
			Map<V, Count> valueCount = new HashMap<>();
			int totalHits = 0;
			final int capacity = offset + limit;
			YDBScoreDoc<V> top = null;
			boolean queueFull = false;
			PriorityQueue<YDBScoreDoc<V>> queue = newPriorityQueue(capacity);
			for(;docIterator.hasNext();) {
				totalHits ++;
				int docID = docIterator.next();
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
		
		Result<YDBScoreDoc<V>> resortStage2(final int howMany, Result<?>[] results, int offset, int limit){
			PriorityQueue<YDBScoreDoc<V>> queue = this.newPriorityQueue(howMany);
			Map<V, Count> valueCount = new HashMap<>();
			int totalHits = 0;
			YDBScoreDoc<V> top = null;
			boolean queueFull = false;
			for(Result<?> r : results) {
				if(r.count == 0) continue;
				for(int i=0;i<r.result.length;i++) {
					@SuppressWarnings("unchecked")
					YDBScoreDoc<V> sd = (YDBScoreDoc<V>) r.result[i];
					totalHits ++;	
					V v = sd.fieldValue;
					Count count = valueCount.get(v);
					if(count == null) {
						count = new Count(MAX_COUNT);
						valueCount.put(v, count);
					} else {
						count.dec();
					}
					if(queueFull) {
						if(count.value() < top.score) continue;
						top.doc = sd.doc;
						top.atomicKey = sd.atomicKey;
						top.leafKey = sd.leafKey;
						top.score = count.value();
						top.fieldValue = v;
						top = queue.updateTop();
					} else {
						sd.score = count.value();
						top = queue.add(sd);
						queueFull = totalHits == howMany;
					}
				}
			}
			return this.results(queue, totalHits, offset, limit);
		}
		
		Result<YDBScoreDoc<V>> results (PriorityQueue<? extends YDBScoreDoc<V>> queue, int totalHits, int offset, int limit) {
			int size = totalHits < queue.size() ? totalHits : queue.size();
			if (offset >= size) return null;
			limit = Math.min(queue.size()-offset, limit);
			int len = Math.min(queue.size(), limit);
			@SuppressWarnings("unchecked")
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
	
	private static class Resorter4Sort <V>{
		
		private Comparator<YDBScoreDoc4Sort<V,Number[]>> resortComparator = new Comparator<YDBScoreDoc4Sort<V,Number[]>>() {
			@Override
			public int compare(YDBScoreDoc4Sort<V, Number[]> a, YDBScoreDoc4Sort<V, Number[]> b) {
				if(a.score > b.score) return 1;
				else if(a.score < b.score) return -1;
				else { 
					if(a.doc > b.doc) return 1;
					else return -1;
				}
			}
		};

		Result<YDBScoreDoc4Sort<V, Number[]>> resortStage1(YdbLeafReader r, Iterator<TopNTermNum> docIterator, int offset, int limit, ValuesWrapper<V> wrapper){
			if (offset < 0 || limit <= 0) return null;
			Map<V, Count> valueCount = new HashMap<>();
			int totalHits = 0;
			final int capacity = offset + limit;
			YDBScoreDoc4Sort<V, Number[]> top = null;
			boolean queueFull = false;
			PriorityQueue<YDBScoreDoc4Sort<V, Number[]>> queue = newPriorityQueue(capacity, resortComparator);
			for(;docIterator.hasNext();) {
				totalHits ++;
				TopNTermNum ttn = docIterator.next();
				int docID = ttn.docid;
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
					top.sortValues = ttn.tm;
					top = queue.updateTop();
				} else {
					YDBScoreDoc4Sort<V, Number[]> sd = new YDBScoreDoc4Sort<V, Number[]>(docID, count.value(), r.getAtomickey(), r.getLeafkey());
					sd.fieldValue = v;
					sd.sortValues = ttn.tm;
					top = queue.add(sd);
					queueFull = totalHits == capacity;
				}
			}
			return this.results(queue, totalHits, offset, limit);
		}

		@SuppressWarnings("unchecked")
		Result<YDBScoreDoc4Sort<V, Number[]>> resortStage2(int sum, int howMany, Result<?>[] results, int offset, int limit, Comparator<YDBScoreDoc4Sort<V, Number[]>> comparator){
			// sort 
			PriorityQueue<YDBScoreDoc4Sort<V, Number[]>> queue0 = new PriorityQueue<YDBScoreDoc4Sort<V, Number[]>>(sum) {
				@Override
				protected boolean lessThan(YDBScoreDoc4Sort<V, Number[]> a, YDBScoreDoc4Sort<V, Number[]> b) {
					return comparator.compare(a, b)>0 ? false : true;
				}
			};
			for(Result<?> r : results) {
				if(r.count == 0) continue;
				for(int i=0;i<r.result.length;i++) {
					queue0.add((YDBScoreDoc4Sort<V, Number[]>) r.result[i]);
				}
			}
			
			// 打散排序
			Map<V, Count> valueCount = new HashMap<>();
			YDBScoreDoc4Sort<V, Number[]> top = null;
			int totalHits = 0;
			boolean queueFull = false;
			PriorityQueue<YDBScoreDoc4Sort<V, Number[]>> queue1 = this.newPriorityQueue(howMany, this.resortComparator);
			for(int i = queue0.size(); i > 0; i--) {
				YDBScoreDoc4Sort<V, Number[]> sd = queue0.pop();
				totalHits ++;	
				V v = sd.fieldValue;
				Count count = valueCount.get(v);
				if(count == null) {
					count = new Count(MAX_COUNT);
					valueCount.put(v, count);
				} else {
					count.dec();
				}
				if(queueFull) {
					if(count.value() < top.score) continue;
					top.doc = sd.doc;
					top.atomicKey = sd.atomicKey;
					top.leafKey = sd.leafKey;
					top.score = count.value();
					top.fieldValue = v;
					top.sortValues = sd.sortValues;
					top = queue1.updateTop();
				} else {
					sd.score = count.value();
					top = queue1.add(sd);
					queueFull = totalHits == howMany;
				}
			}
			return this.results(queue1, totalHits, offset, limit);
		}
		
		Result<YDBScoreDoc4Sort<V, Number[]>> results (PriorityQueue<? extends YDBScoreDoc4Sort<V, Number[]>> queue, int totalHits, int offset, int limit) {
			int size = totalHits < queue.size() ? totalHits : queue.size();
			if (offset >= size) return null;
			limit = Math.min(queue.size()-offset, limit);
			int len = Math.min(queue.size(), limit);
			@SuppressWarnings("unchecked")
			YDBScoreDoc4Sort<V, Number[]>[] results = new YDBScoreDoc4Sort[len];
			for (int i=queue.size()-offset-limit;i>0;i--) {queue.pop(); }
			for (int i = limit-1;i >= 0;i--) { 
				results[i] = queue.pop();
			}
			return new Result<YDBScoreDoc4Sort<V, Number[]>>(totalHits, results);
		}
		
		PriorityQueue<YDBScoreDoc4Sort<V, Number[]>> newPriorityQueue(int capacity, Comparator<YDBScoreDoc4Sort<V, Number[]>> comparator) {
			return new PriorityQueue<YDBScoreDoc4Sort<V, Number[]>>(capacity) {
				@Override
				protected boolean lessThan(YDBScoreDoc4Sort<V, Number[]> a, YDBScoreDoc4Sort<V, Number[]> b) {
					return comparator.compare(a, b) > 1 ? false : true;
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
		public int doc;
		protected transient int score;
		public long atomicKey;
		public long leafKey;
		public V fieldValue;
		
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
	
	public static class YDBScoreDoc4Sort<V1, V2> extends YDBScoreDoc<V1> {
		private V2 sortValues;
		public YDBScoreDoc4Sort(int doc, int score, long atomicKey, long leafKey) {
			super(doc, score, atomicKey, leafKey);
		}
		public V2 getSortFieldValues() {
			return this.sortValues;
		}
	}

	private static interface ValuesWrapper<V>{
		V getValue(int index);
	}

	private static class Count {
		private int i;
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
}
