package cn.net.ydbmix.svn2.ydb.executer.segments;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.Logger;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.util.Bits;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.mdrill.MdrillUtils;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocSet;

import cn.net.ycloud.ydb.core.util.YdbLeafReader;
import cn.net.ycloud.ydb.core.util.YdbSearchAutoRecycle;
import cn.net.ycloud.ydb.core.util.YdbSearchAutoRef;
import cn.net.ycloud.ydb.json.JSONArray;
import cn.net.ycloud.ydb.json.JSONException;
import cn.net.ycloud.ydb.utils.TimerPools;
import cn.net.ycloud.ydb.utils.UniqConfig;
import cn.net.ycloud.ydb.utils.Utils;
import cn.net.ydbmix.svn1.ydb.search.selectTopNdatarow.SortParse;
import cn.net.ydbmix.svn1.ydb.search.selectTopNdatarow.TopNRecordCount;
import cn.net.ydbmix.svn1.ydb.search.selectTopNdatarow.TopNTermCrcValue;
import cn.net.ydbmix.svn1.ydb.search.selectTopNdatarow.TopNTermNum;
import cn.net.ydbmix.svn1.ydb.search.selectTopNdatarow.TopNTermNumCmp;
import cn.net.ydbmix.svn1.ydb.search.selectTopNdatarow.TopNTermValue;
import cn.net.ydbmix.svn2.ydb.LazyWriter.LazyWriter;
import cn.net.ydbmix.svn2.ydb.core.index.YdbDocValues.DocValuesGetOrdInterface;
import cn.net.ydbmix.svn2.ydb.core.index.YdbDocValuesFieldsList;
import cn.net.ydbmix.svn2.ydb.hdfsBlockDirectore.BlockCacgeStat;
import cn.net.ydbmix.svn2.ydb.search.FieldValueGet;
import cn.net.ydbmix.svn2.ydb.search.utils.ArraysKeyLong;
import cn.net.ydbmix.svn2.ydb.search.utils.SearchUtils;
import cn.net.ydbmix.svn2.ydb.timecachemap.TimeCacheMap.ExpiredCallback;
import cn.net.ydbmix.svn2.ydb.timecachemap.TimeCacheMapNoLock;

import static org.apache.lucene.index.DocValuesType.NONE;
import static org.apache.lucene.index.DocValuesType.NUMERIC;
import static org.apache.lucene.index.DocValuesType.BINARY;

public class SelectTopN2 {
    static Logger LOG = Logger.getLogger("ycloud.SelectTopN");
    public static int getStat()
    {
    	return Lazy.LAZY().STAGE2_READER.size();
    }
    public static class Lazy{

    	ExecutorService pool=Executors.newCachedThreadPool();
    	final YdbSearchAutoRecycle recycle=new YdbSearchAutoRecycle();
    	ExpiredCallback<Object, YdbSearchAutoRef> calback=new ExpiredCallback<Object, YdbSearchAutoRef>() {

			@Override
			public void expire(Object key, YdbSearchAutoRef val) {
				recycle.add(val);
			}

			@Override
			public void commit() {
				pool.execute(new Runnable() {
					
					@Override
					public void run() {
						recycle.finishReference();
						
					}
				});
			}
		}; 
        public  TimeCacheMapNoLock<Object, YdbSearchAutoRef> STAGE2_READER = new TimeCacheMapNoLock<Object, YdbSearchAutoRef>(Integer.MAX_VALUE,new TimerPools("SelectTopN_crc_cache_timer"),UniqConfig.INSTANCE().Stagesnapshop(),3,calback);

		private static Object LOCK = new Object();
		private static AtomicReference<Lazy> LAZYY =new AtomicReference<Lazy>(null);
		public static Lazy LAZY() {
			Lazy rtn=LAZYY.get();
			if(rtn!=null)
			{
				return rtn;
			}
			
			
			synchronized (LOCK) {
				rtn=LAZYY.get();
				if(rtn!=null)
				{
					return rtn;
				}
				rtn = new Lazy();

				LAZYY.set(rtn);
				return rtn;
			}
		}
}
    

    public void SetReader(String path, SolrParams params,YdbSearchAutoRef reader)
    {
    	Lazy.LAZY().STAGE2_READER.put(Arrays.asList(params.get("mdrill.crc.key.set"),Utils.pathToString(path)),reader.stage2R());
    }
    
    
    public YdbSearchAutoRef PopReader(final YdbSearchAutoRecycle recycle,String path, SolrParams params)
 {
		YdbSearchAutoRef ref = null;

		String crcKeep=params.get("mdrill.crc.key.keep","false");
		


		try {
			if(crcKeep.equals("true"))
			{
				YdbSearchAutoRef ref2= Lazy.LAZY().STAGE2_READER.get(Arrays.asList(params.get("mdrill.crc.key.get"), Utils.pathToString(path)));
				ref=ref2.stage2R();
			}else
			{
				ref = Lazy.LAZY().STAGE2_READER.remove(Arrays.asList(params.get("mdrill.crc.key.get"), Utils.pathToString(path)));

			}
		} finally {
			if (ref != null) {
				ref.setRecycle(recycle);
			}
		}

		return ref;
	}
    
	public HashMap<String, Object> Stage1_merger(
			List<HashMap<String, Object>> listm, SolrParams params,
			IndexSchema schema) throws IOException {
		SortParse sort = new SortParse(params, schema);
		int offset = params.getInt(FacetParams.YDB_CROSS_OFFSET, 0);
		int limit = params.getInt(FacetParams.FACET_CROSS_LIMIT, 100);
		int limit_offset = offset + limit;

		PriorityQueue<TopNTermCrcValue> topItems = 
				new PriorityQueue<TopNTermCrcValue>(Math.max(limit_offset, 1), Collections.reverseOrder(sort.crcCmp));

		TopNRecordCount recordCount = new TopNRecordCount();

		for (HashMap<String, Object> map : listm) {
			Object[] count_o = (Object[]) map.get("count");
			recordCount.shardsMerge(new TopNRecordCount(count_o));
			Object[] list_o = (Object[]) map.get("list");

			for (Object row : list_o) {
				TopNTermCrcValue newrow = new TopNTermCrcValue((Object[])row);
//				LOG.info("+++"+newrow.toString());
				SearchUtils.put2QueueDetail(newrow, topItems, limit_offset, sort.crcCmp);
			}
		}

		java.util.ArrayList<TopNTermCrcValue> recommendations = new ArrayList<TopNTermCrcValue>(
				topItems.size());
		recommendations.addAll(topItems);
		Collections.sort(recommendations, sort.crcCmp);

			ArrayList<Object[]> list=new ArrayList<Object[]>(recommendations.size());
		for (int i = offset; i < recommendations.size(); i++) {
			TopNTermCrcValue crcval = recommendations.get(i);
//			LOG.info("==="+crcval.toString());

			list.add(crcval.toArrayList());
		}

		
		HashMap<String, Object> rtnmap = new HashMap<String, Object>(4);
		rtnmap.put("count", recordCount.toArrayList());
		rtnmap.put("list", list.toArray(new Object[list.size()]));
		return rtnmap;
	}
   	
   	
	public HashMap<String, Object> export(LazyWriter cnt,YdbLeafReader r,SolrParams params,IndexSchema schema,DocSet baseDocs,String debugIdentify,String debugIdentify2,long[] debugtsarr) throws IOException, JSONException
   	{
   		
			return ExportTopN.export(cnt,r, params, schema, baseDocs, debugIdentify, debugIdentify2, debugtsarr);

   		
   	}
	
	public Map<String, Object> selectTopNStage1(YdbLeafReader r, SolrParams params, IndexSchema schema,
   			DocSet baseDocs, String debugIdentify, String debugIdentify2, long[] debugtsarr, long searchdiff) throws IOException{
		
		String[] fields = params.getParams("");
		final LeafReader reader = r.contex.reader();
		if(fields == null || fields.length == 0) {
			return this.SelectTopN_Stage1(r, params, schema, baseDocs, debugIdentify, debugIdentify2, debugtsarr, searchdiff);
		} else if(fields.length == 1) {
			String field = fields[0];
			
			for(Iterator<Integer> it=baseDocs.iterator();it.hasNext();) {
				int doc = it.next();
			}
		} else {
			
		}
		Map<String,Object> ret = new HashMap<String, Object>(4);
		ret.put("count", recordCount.toArrayList());
		ret.put("list", list.toArray(new Object[list.size()]));
		return ret;
	}
	

   	public HashMap<String, Object> SelectTopN_Stage1(YdbLeafReader r,SolrParams params,IndexSchema schema,
   			DocSet baseDocs,String debugIdentify,String debugIdentify2,long[] debugtsarr,long searchdiff) throws IOException
   	{
   		
    	long debugts=System.currentTimeMillis();
   		int limit = params.getInt(FacetParams.FACET_CROSS_LIMIT, 100);
   		int limit_offset=limit;
   		
   		TopNRecordCount recordCount=new TopNRecordCount() ;
   		String crcOutputSet=params.get("mdrill.crc.key.set");
   		java.util.ArrayList<TopNTermCrcValue> recommendations=new ArrayList<TopNTermCrcValue>(1);
   		if(baseDocs.size()>0)
   		{ 			
   			SortParse sort=new SortParse(params, schema);
   		
   			PriorityQueue<TopNTermNum> res=new PriorityQueue<TopNTermNum>(limit_offset, Collections.reverseOrder(sort.numCmp));
   			YdbDocValuesFieldsList sortufs=new YdbDocValuesFieldsList(sort.sorts,r.parent,schema,r,r.contex.reader(),params);

   			int doc = -1;
   			recordCount.inc(baseDocs.size());
   			if (sort.isNeedSortInsegment) {
				BlockCacgeStat.IncSearchHitDocall(baseDocs.size());

				final int len = sort.sorts.length;
				final DocValuesGetOrdInterface[] labelParser = sortufs.ord;
				DocIterator iter = baseDocs.iterator();

				Number[] tmd_g = new Number[len];

				TopNTermNum mrow_g = new TopNTermNum(doc, tmd_g);
				TopNTermNumCmp numCmp = sort.numCmp;

				int addcnt = 0;
				int skipcnt = 0;
				long ts1 = System.currentTimeMillis();

				while (iter.hasNext()) {
					doc = iter.nextDoc();
					for (int i = 0; i < len; i++) {
						tmd_g[i] = labelParser[i].getFirstOrd(doc);
					}
					if (res.size() < limit_offset) {
						Number[] tmd = new Number[len];
						for (int i = 0; i < len; i++) {
							tmd[i] = tmd_g[i];
						}
						++addcnt;
						res.add(new TopNTermNum(doc, tmd));
					} else if (numCmp.compare(res.peek(), mrow_g) > 0) {
						Number[] tmd = new Number[len];
						for (int i = 0; i < len; i++) {
							tmd[i] = tmd_g[i];
						}
						++addcnt;
						res.add(new TopNTermNum(doc, tmd));
						res.poll();
					} else {
						++skipcnt;
					}

				}
				long ts2=System.currentTimeMillis();
				if(LOG.isDebugEnabled()){
					LOG.info("selectTopN top 10000:res:"+res.size()+",limit_offset:"+limit_offset+",skipcnt:"+skipcnt+",addcnt:"+addcnt+",baseDocs:"+baseDocs.size()+",diff:"+(ts2-ts1));
				}
   			} else {
				DocIterator iter = baseDocs.iterator();
				while (iter.hasNext()) {
					doc = iter.nextDoc();
					Number[] tm = new Number[] { doc };

					TopNTermNum row = new TopNTermNum(doc, tm);
					SearchUtils.put2QueueDetail(row, res, limit_offset, sort.numCmp);
					if (res.size() >= limit_offset) {
						break;
					}
				}
			}
   		
   		
	   		PriorityQueue<TopNTermCrcValue> topItems = new PriorityQueue<TopNTermCrcValue>(limit_offset, Collections.reverseOrder(sort.crcCmp));
	   		for (TopNTermNum row : res) {
	   			String[] tm=new String[sort.sorts.length];
	   			for(int i=0;i<sort.sorts.length;i++)
	   			{
	   				DocValuesGetOrdInterface col= sortufs.ord[i];
	   				tm[i]=col.lookupOrd(row.tm[i]);//.tNumToString(col.uif.termNum(row.docid),col.filetype,col.ti,"0");
	   			}
	   			long[] crcs=new long[]{row.docid,r.getAtomickey(),r.getLeafkey()};
	   			
	   			TopNTermCrcValue newrow =new TopNTermCrcValue(crcs, tm);
	   			SearchUtils.put2QueueDetail(newrow, topItems, limit_offset, sort.crcCmp);
	   		}
   		
   		
   			recommendations = new ArrayList<TopNTermCrcValue>(topItems.size());
   			recommendations.addAll(topItems);
   			Collections.sort(recommendations, sort.crcCmp);
   			long endts=System.currentTimeMillis();

   			if(LOG.isDebugEnabled())
			{
				if(debugtsarr!=null&&debugtsarr.length>0&&endts-debugtsarr[0]>5000l)
	    		{
					LOG.debug("notice:"+debugIdentify+"@"+recordCount.getCount()+"@"+(endts-debugts)+"@"+searchdiff+"@"+debugIdentify2+"@"+new JSONArray(Arrays.asList(debugtsarr)).toString());
	    		}else{
	    			LOG.debug(debugIdentify+"@"+recordCount.getCount()+"@"+(endts-debugts)+"@"+searchdiff);
	    		}
			}
			
   		}
   			
   			HashMap<String,Object> rtnmap = new HashMap<String, Object>(4);
   			rtnmap.put("count", recordCount.toArrayList());
   			
	   			ArrayList<Object[]> list=new ArrayList<Object[]>(recommendations.size());

   			if(recommendations.size()>0)
   			{
   				List<? extends Object> key=Arrays.asList(crcOutputSet,r.getAtomickey(),r.getLeafkey());

   				ConcurrentHashMap<ArraysKeyLong, Long> cache;
   				

					cache=MdrillUtils.CRC_CACHE_SIZEMap().get(key);
	   				if(cache==null)
	   				{
		   					cache=new ConcurrentHashMap<ArraysKeyLong,Long>(recommendations.size());
		   					
		   					MdrillUtils.CRC_CACHE_SIZEMap().put(key, cache);
	   				}
	   				
   	   			
   	   			for (int i=0;i<recommendations.size();i++) {
   	   				TopNTermCrcValue crcval=recommendations.get(i);
   	   				cache.put(new ArraysKeyLong(crcval.crc), crcval.crc[0]);
   	   				list.add(crcval.toArrayList());
   	   			}
   			}
   			
   			
   			rtnmap.put("list", list.toArray(new Object[list.size()]));
	    

   			return rtnmap;

   	}
   	
   	public TopNRecordCount Stage1RecordCount(HashMap<String, Object> stage_1_result_merger){
		return new TopNRecordCount((Object[]) stage_1_result_merger.get("count"));
   	}
   	

 	
 	public class TopNResult{
 		public int code=0;
		public ArrayList<TopNTermValue> list;
		
		@Override
		public String toString() {
			return "TopNResult [list=" + list
					+ "]";
		}
 	}
 	
	public TopNRecordCount finalResultCount(HashMap<String, Object> stage_1_result_merger)
 	{
		return new TopNRecordCount((Object[]) stage_1_result_merger.get("count"));
 	}
 	public TopNResult finalResult(int start,int end,Object[] list_o ,Map<ArraysKeyLong, Object[]> stage_2_result_merger)
 	{
 		TopNResult rtn=new TopNResult();
		rtn.list=new ArrayList<TopNTermValue>();

		for (int i=start;i<end&&i<list_o.length;i++) {
			TopNTermCrcValue newrow = new TopNTermCrcValue((Object[])list_o[i]);
			Object[] val=stage_2_result_merger.get(new ArraysKeyLong(newrow.crc));
			rtn.list.add(new TopNTermValue(val));
		}
		
		return rtn;
 	}
 	
 	public String[] Stage1_crc(int start ,int end,Object[] list_o)
 	{
		ArrayList<String> crclistBuffer=new ArrayList<String>((end-start)*2+4);

		for (int i=start;i<end&&i<list_o.length;i++) {
			TopNTermCrcValue newrow = new TopNTermCrcValue((Object[])list_o[i]);
			crclistBuffer.add(String.valueOf(newrow.crc[0]));
			crclistBuffer.add(String.valueOf(newrow.crc[1]));
			crclistBuffer.add(String.valueOf(newrow.crc[2]));

		}
		
		return crclistBuffer.toArray(new String[crclistBuffer.size()]);
 	}
   	
    public Map<ArraysKeyLong,Object[]> SelectTopN_Stage2_merger(
			List<Map<ArraysKeyLong,Object[]>> listm, SolrParams params,
			IndexSchema schema) throws IOException
   	{ 	
		Map<ArraysKeyLong,Object[]> crcvalue=new HashMap<ArraysKeyLong,Object[]>();
		for(Map<ArraysKeyLong,Object[]> m:listm)
		{
			crcvalue.putAll(m);
		}
		return crcvalue;
   	}
    
    public long[] ParseLongs(String[] ls)
	 {
		 if(ls==null)
		 {
			 return new long[0];
		 }
		 long[] rtn=new long[ls.length];
		 for(int i=0;i<rtn.length;i++)
		 {
			 rtn[i]=Long.parseLong(ls[i]);
		 }
		 
		 return rtn;
	 }
    
    public Map<ArraysKeyLong,Object[]> SelectTopN_Stage2(ArrayList<ArraysKeyLong> crclist,YdbLeafReader r,SolrParams params,IndexSchema schema) throws IOException, JSONException
	{
		String[] fields=params.getParams(FacetParams.FACET_FIELD);

		String crcget=params.get("mdrill.crc.key.get",null);;
		String crcKeep=params.get("mdrill.crc.key.keep","false");;

		
		final ConcurrentHashMap<ArraysKeyLong, Long> cache;
		if(crcKeep.equals("true"))
		{
			cache=MdrillUtils.CRC_CACHE_SIZEMap().get(Arrays.asList(crcget,r.getAtomickey(),r.getLeafkey()));
		}else{
			cache=MdrillUtils.CRC_CACHE_SIZEMap().remove(Arrays.asList(crcget,r.getAtomickey(),r.getLeafkey()));
		}
				

		Map<ArraysKeyLong,Object[]> crcvalue=null;
		if(cache!=null&&cache.size()>0)
		{
			if(crclist.size()>0)
			{
				
				TreeMap<ArraysKeyLong,Integer> crc2doc=new TreeMap<ArraysKeyLong, Integer>();

				for(ArraysKeyLong crc:crclist)
				{
					Long docid=cache.get(crc);
					if(docid!=null)
					{
						int doc=docid.intValue();
						crc2doc.put(crc, doc);
					}
				
				}

				
				if(crc2doc.size()>0)
				{
					crcvalue=new HashMap<ArraysKeyLong,Object[]>(crc2doc.size());
					if(r.parent.isRecycled())
					{
						LOG.error("search recycled "+r.parent.hasRecycledLog(),new IOException("search recycled"));
					}
//					IndexVersionFdtForReadSeq forread=new IndexVersionFdtForReadSeq(partion) ;
					FieldValueGet fvget=new FieldValueGet(fields, r.contex.reader(),r, schema,params);
					
					for(Entry<ArraysKeyLong, Integer> e:crc2doc.entrySet())
					{
						ArraysKeyLong crc=e.getKey();
						int doc=e.getValue();
						TopNTermValue tv=new TopNTermValue();
						tv.row=fvget.doc(doc);
						crcvalue.put(crc, tv.toArrayList());
					}
					
				}
				

			}
	
		}
		
		if(crcvalue==null)
		{
			crcvalue=new HashMap<ArraysKeyLong,Object[]>(1);
		}
		
		return crcvalue;
		

	}
    
   
	

}
