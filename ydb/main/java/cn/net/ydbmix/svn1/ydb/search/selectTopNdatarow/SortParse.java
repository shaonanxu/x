package cn.net.ydbmix.svn1.ydb.search.selectTopNdatarow;

import java.util.ArrayList;

import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.compare.UniqTypeNum;
import org.apache.solr.request.mdrill.MdrillUtils;
import org.apache.solr.schema.IndexSchema;

import cn.net.ydbmix.svn2.ydb.core.index.YdbDocValues;

public class SortParse {
	

	public boolean isNeedSortInsegment;
	public String[] sorts;
	public TopNTermNumCmp numCmp;
	public TopNTermCrcValueCmp crcCmp;
	
	public SortParse()
	{
		this.isNeedSortInsegment = false;
		this.sorts=new String[0];

		this.numCmp=new TopNTermNumCmp(new boolean[0]);
   		this.crcCmp=new TopNTermCrcValueCmp(new boolean[0],new boolean[0]);
	}
	public SortParse(SolrParams params, IndexSchema schema) {
		String[] sortfl = params.getParams(FacetParams.FACET_CROSS_SORT_FL);

		ArrayList<String> sortList ;
		this.isNeedSortInsegment = false;
		if (sortfl == null) {
			sortfl = new String[0];
			sortList = new ArrayList<String>(1);
		} else {
			sortList = new ArrayList<String>( sortfl.length);
			for (int i = 0; i < sortfl.length; i++) {
				if (MdrillUtils.isNeedSortInsegment(sortfl[i])) {
					sortList.add(sortfl[i]);
					if(!(sortfl[i].equals("y_dbpartition_s")||sortfl[i].equals("y_partition_s")))
					{
						this.isNeedSortInsegment = true;
					}
				}
			}
		}

		sorts = sortList.toArray(new String[sortList.size()]);
		boolean[] sortSeg = new boolean[sorts.length];
		for (int i = 0; i < sorts.length; i++) {
			sortSeg[i] =YdbDocValues.isNum(schema, sorts[i]);// UnInvertedFieldUtils.isNumber(sorts[i], schema);
		}
		
		boolean[] isdesc=UniqTypeNum.ParseBoolean(params.getParams(FacetParams.FACET_CROSS_SORT_ISDESC));


		this.numCmp=new TopNTermNumCmp(isdesc);
   		this.crcCmp=new TopNTermCrcValueCmp(sortSeg,isdesc);
	}

}
