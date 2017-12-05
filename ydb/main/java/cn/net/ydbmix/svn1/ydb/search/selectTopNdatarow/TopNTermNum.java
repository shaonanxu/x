package cn.net.ydbmix.svn1.ydb.search.selectTopNdatarow;

import java.util.Arrays;


public class TopNTermNum {
	public int docid=0;
	public Number[] tm;
	public TopNTermNum(int docid, Number[] tm) {
		this.docid = docid;
		this.tm = tm;
	}
	
	
	@Override
	public String toString() {
		return "TopNTermNum [docid=" + docid + ", tm=" + Arrays.toString(tm)
				+ "]";
	}

	public TopNTermNum(){}
	

}
