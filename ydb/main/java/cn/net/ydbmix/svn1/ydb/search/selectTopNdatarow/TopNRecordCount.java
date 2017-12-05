package cn.net.ydbmix.svn1.ydb.search.selectTopNdatarow;

import java.util.ArrayList;
import java.util.Collection;


public class TopNRecordCount {
//	private static int TYPE_NUM=DataRowTypeSeri.ser(DataRowType.TopNRecordCount);

	private long count=0l;
	
//	private ArrayList<String> debugMsg=new ArrayList<String>();
//	public ArrayList<String> getDebugMsg() {
//		return debugMsg;
//	}
	@Override
	public String toString() {
		return "TopNRecordCount [count=" + count  + "]";
	}
	public TopNRecordCount() {}
	public TopNRecordCount(Object[] nst)
	{
		this.count=(Long) nst[0];
	}
	
	public long getCount() {
		return count;
	}
	public void shardsMerge(TopNRecordCount g)
	{	
		TopNRecordCount o=(TopNRecordCount)g;
		count+=o.count;
	}
	

	
	public void inc()
	{
		this.count++;
	}
	
	public void inc(int num)
	{
		this.count+=num;
	}
	
	public Object[] toArrayList()
	{
		return new Object[]{this.count};
	}


}
