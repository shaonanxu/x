package cn.net.ydbmix.svn1.ydb.search.selectTopNdatarow;

import java.util.Comparator;

import org.apache.solr.request.compare.UniqTypeNum;

public class TopNTermNumCmp implements Comparator<TopNTermNum> {
	public final boolean[] isdesc;

	public TopNTermNumCmp(boolean[] _isdesc) {
		this.isdesc = _isdesc;
	}

	@Override
	public  int compare(TopNTermNum o1, TopNTermNum o2) {
		return  UniqTypeNum.compare(o1.tm, o2.tm,this.isdesc);
	}

}
