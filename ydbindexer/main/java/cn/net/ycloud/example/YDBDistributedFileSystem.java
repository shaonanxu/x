package cn.net.ycloud.example;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemLinkResolver;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.DFSUtil;

public class YDBDistributedFileSystem extends org.apache.hadoop.hdfs.DistributedFileSystem {

	YdbDFSClient ydbclient;

	@Override
	public void initialize(URI uri, Configuration conf) throws IOException {
		super.initialize(uri, conf);
		setConf(conf);
	    this.ydbclient = new YdbDFSClient(uri, conf, statistics);

	}

	boolean ydbverifyChecksum = true;

	@Override
	public void setVerifyChecksum(boolean verifyChecksum) {
		super.setVerifyChecksum(verifyChecksum);
		this.ydbverifyChecksum = verifyChecksum;
	}

	private String getYdbPathName(Path file) {
		checkPath(file);
		String result = file.toUri().getPath();
		if (!DFSUtil.isValidName(result)) {
			throw new IllegalArgumentException("Pathname " + result + " from " + file + " is not a valid DFS filename.");
		}
		return result;
	}

	@Override
	public FSDataInputStream open(Path f, final int bufferSize) throws IOException {
		statistics.incrementReadOps(1);
		Path absF = fixRelativePart(f);
		return new FileSystemLinkResolver<FSDataInputStream>() {
			@Override
			public FSDataInputStream doCall(final Path p) throws IOException, UnresolvedLinkException {
				final DFSInputStream dfsis = ydbclient.open(getYdbPathName(p), bufferSize, ydbverifyChecksum);
				return ydbclient.createWrappedInputStream(dfsis);
			}

			@Override
			public FSDataInputStream next(final FileSystem fs, final Path p) throws IOException {
				return fs.open(p, bufferSize);
			}
		}.resolve(this, absF);
	}
}
