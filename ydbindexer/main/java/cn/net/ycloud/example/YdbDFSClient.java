package cn.net.ycloud.example;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
//import org.apache.hadoop.fs.StorageType;

public class YdbDFSClient extends DFSClient {
	private static final Log LOG = LogFactory.getLog("ycloud.YdbDFSClient");

	URI ydbnameNodeUri;
	public YdbDFSClient(URI nameNodeUri, Configuration conf, FileSystem.Statistics stats) throws IOException {
		super(nameNodeUri, null, conf, stats);
		this.ydbnameNodeUri=nameNodeUri;
	}
	
	public static class DatanodeInfoAndType{
		public DatanodeInfoAndType(Object locations, Object storageIDs, Object storageTypes) {
			this.locations = locations;
			this.storageIDs = storageIDs;
			this.storageTypes = storageTypes;
		}
		Object locations;
		Object storageIDs;
		Object storageTypes;
	}
	

	
	  public LocatedBlocks SSD( LocatedBlocks rtn) throws IOException
	  {
		  
		  try{
				for(LocatedBlock b:rtn.getLocatedBlocks())
				{
					String last=b.toString();
					
					Object[] locations=b.getLocations();
					Object[] storageIDs=b.getStorageIDs();
			        Method m1 = b.getClass().getMethod("getStorageTypes");
//					Object[] storageTypes=b.getStorageTypes();
			        Object[] storageTypes=(Object[]) m1.invoke(b);
			        
					
					if(locations==null)
					{
						continue;
					}
					
					ArrayList<DatanodeInfoAndType> list=new ArrayList<DatanodeInfoAndType>();
					for(int i=0;i<locations.length;i++)
					{
						Object info=locations[i];
						Object sid=null;
						Object storageType=null;

						if(storageIDs!=null&&i<storageIDs.length)
						{
							sid=storageIDs[i];
						}
						if(storageTypes!=null&&i<storageTypes.length)
						{
							storageType=storageTypes[i];
						}
						
						if(storageType!=null&&String.valueOf(storageType).toLowerCase().indexOf("ssd")>=0)
						{
							list.add(new DatanodeInfoAndType(info, sid, storageType));
						}
					}
					
					for(int i=0;i<locations.length;i++)
					{
						Object info=locations[i];
						Object sid=null;
						Object storageType=null;

						if(storageIDs!=null&&i<storageIDs.length)
						{
							sid=storageIDs[i];
						}
						if(storageTypes!=null&&i<storageTypes.length)
						{
							storageType=storageTypes[i];
						}
						
						if(storageType!=null&&String.valueOf(storageType).toLowerCase().indexOf("ssd")>=0)
						{
						}else{
							list.add(new DatanodeInfoAndType(info, sid, storageType));
						}
					}
					
					
					for(int i=0;i<locations.length;i++)
					{
						DatanodeInfoAndType infotp=list.get(i);
						locations[i]=infotp.locations;

						if(storageIDs!=null&&i<storageIDs.length)
						{
							storageIDs[i]=infotp.storageIDs;
						}
						if(storageTypes!=null&&i<storageTypes.length)
						{
							storageTypes[i]=infotp.storageTypes;
						}
						
					}
					
					if(LOG.isDebugEnabled())
					{
						LOG.debug("ydbdfs:"+last+"@"+b+"@"+String.valueOf(ydbnameNodeUri));
					}
					
				}
			}
			catch(Throwable e)
			{
				LOG.info("SsdFirstInputStream",e);
			}
		  return rtn;
	  }
	  
	  
	  @Override
	  public LocatedBlocks getLocatedBlocks(String src, long start) throws IOException
	  {
		  LocatedBlocks rtn= super.getLocatedBlocks(src, start);
		 return SSD(rtn);
	  }
	@Override
	  public LocatedBlocks getLocatedBlocks(String src, long start, long length) throws IOException
	  {
		  LocatedBlocks rtn= super.getLocatedBlocks(src, start, length);
		 return SSD(rtn);
	  }
}