package test.cn.net.ycloud.ydb.tokenizer.highlight;

import org.junit.Test;

import cn.net.ycloud.ydb.tokenizer.highlight.YdbHighlighter;

public class TestHighlight {
	static final String QUERY = "YDB";
	
	@Test
	public void testOneTokenHitInForward() {
		String content = "全称延云,YDB是一个基于 Hadoop 分布式架构下的实时的、多维的、交互式的查询、统计、分" + 
				"析引擎,具有万亿数据规模下的秒级性能表现,并具备企业级的稳定可靠表现。" + 
				"一个细粒度的索引,精确粒度的索引。数据即时导入,索引即时生成,通过索引高效定位到相" + 
				"关数据。";
		System.out.println("============ 命中一个词,词位于起始位置 ================");
		System.out.println(content);
		System.out.println(" ## 文本长度小于摘要长度(200),上下文长度(20) ## ");
		System.out.println(YdbHighlighter.summaryHighlight(QUERY, content, "<pre>", "</pre>", "...", 20, 200));
		System.out.println(" ## 文本长度小于摘要长度(200),上下文长度(50) ## ");
		System.out.println(YdbHighlighter.summaryHighlight(QUERY, content, "<pre>", "</pre>", "...", 50, 200));
		System.out.println(" ## 文本长度大于摘要长度(50),上下文长度(20)## ");
		System.out.println(YdbHighlighter.summaryHighlight(QUERY, content, "<pre>", "</pre>", "...", 20, 50));
		System.out.println(" ## 文本长度大于摘要长度(50),上下文长度(30)## ");
		System.out.println(YdbHighlighter.summaryHighlight(QUERY, content, "<pre>", "</pre>", "...", 30, 50));
	}
	
	@Test
	public void testOneTokenHitInBack() {
		String content = "全称延云,是一个基于 Hadoop 分布式架构下的实时的、多维的、交互式的查询、统计、分" + 
				"析引擎,具有万亿数据规模下的秒级性能表现,并具备企业级的稳定可靠表现。" + 
				"一个细粒度的索引,精确粒度的索引。数据即时导入,索引即时生成,通过索引高效定位到相" + 
				"关数据。YDB";
		System.out.println("============ 命中一个词,词位于结束位置 ================");
		System.out.println(content);
		System.out.println(" ## 文本长度小于摘要长度(200),上下文长度(20) ## ");
		System.out.println(YdbHighlighter.summaryHighlight(QUERY, content, "<pre>", "</pre>", "...", 20, 200));
		System.out.println(" ## 文本长度小于摘要长度(200),上下文长度(50) ## ");
		System.out.println(YdbHighlighter.summaryHighlight(QUERY, content, "<pre>", "</pre>", "...", 50, 200));
		System.out.println(" ## 文本长度大于摘要长度(50),上下文长度(20)## ");
		System.out.println(YdbHighlighter.summaryHighlight(QUERY, content, "<pre>", "</pre>", "...", 20, 50));
		System.out.println(" ## 文本长度大于摘要长度(50),上下文长度(30)## ");
		System.out.println(YdbHighlighter.summaryHighlight(QUERY, content, "<pre>", "</pre>", "...", 30, 50));
	}
	
	@Test
	public void testMultiTokenHit() {
		String content = "全称延云,YDB是一个基于 Hadoop 分布式架构下的实时的、多维的、交互式的查询、统计、分" + 
				"析引擎,具有万亿数据规模下的秒级性能表现,并具备企业级的稳定可靠表现。" + 
				"YDB是一个细粒度的索引,精确粒度的索引。数据即时导入,索引即时生成,通过索引高效定位到相" + 
				"关数据。YDB";
		System.out.println("============ 命中多个词 ================");
		System.out.println(content);
		System.out.println(" ## 文本长度小于摘要长度(200),上下文长度(20) ## ");
		String query = QUERY + " Hadoop";
		System.out.println(YdbHighlighter.summaryHighlight(query, content, "<pre>", "</pre>", "...", 20, 200));
		System.out.println(" ## 文本长度小于摘要长度(200),上下文长度(50) ## ");
		System.out.println(YdbHighlighter.summaryHighlight(query, content, "<pre>", "</pre>", "...", 50, 200));
		System.out.println(" ## 文本长度大于摘要长度(50),上下文长度(20)## ");
		System.out.println(YdbHighlighter.summaryHighlight(query, content, "<pre>", "</pre>", "...", 20, 50));
		System.out.println(" ## 文本长度大于摘要长度(50),上下文长度(30)## ");
		System.out.println(YdbHighlighter.summaryHighlight(query, content, "<pre>", "</pre>", "...", 30, 50));
	}
	
	@Test
	public void testHighlight() {
		String content = "全称延云,YDB是一个基于 Hadoop 分布式架构下的实时的、多维的、交互式的查询、统计、分" + 
				"析引擎,具有万亿数据规模下的秒级性能表现,并具备企业级的稳定可靠表现。" + 
				"YDB是一个细粒度的索引,精确粒度的索引。数据即时导入,索引即时生成,通过索引高效定位到相" + 
				"关数据。YDB";
		System.out.println("============ 全文高亮 ================");
		System.out.println(content);
		System.out.println(YdbHighlighter.highlight(QUERY, content, "<pre>", "</pre>"));
	}
	
	public static void testSummaryHighlight () {
		// 需要高亮的内容
		String query = QUERY + " Hadoop";
		// 全文内容
		String content = "全称延云,YDB是一个基于 Hadoop 分布式架构下的实时的、多维的、交互式的查询、统计、分" + 
				"析引擎,具有万亿数据规模下的秒级性能表现,并具备企业级的稳定可靠表现。" + 
				"YDB是一个细粒度的索引,精确粒度的索引。数据即时导入,索引即时生成,通过索引高效定位到相" + 
				"关数据。YDB";
		
		String result = YdbHighlighter.summaryHighlight(query, content, "<pre>", "</pre>", "...", 10, 50);
		System.out.println(result);
		System.out.println(result.length());
	}
	
	public static void main(String[] args) {
//		testHighlight();
		testSummaryHighlight();
	}
}
