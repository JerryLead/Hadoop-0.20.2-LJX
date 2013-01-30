package org.apache.hadoop.metrics.jvm;



import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.hyperic.sigar.ProcCpu;
import org.hyperic.sigar.ProcMem;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;
import org.hyperic.sigar.SigarProxy;
import org.hyperic.sigar.SigarProxyCache;

/**
 * Singleton class which reports Java Process metrics (CPU/Mem/IO) to the metrics API.  
 * Any application can create an instance of this class in order to emit
 * Java VM metrics.  
 */
public class ResourceMetrics implements Updater {

	//private static final float M = 1024*1024;
    private static ResourceMetrics theInstance = null;
    private static Log log = LogFactory.getLog(ResourceMetrics.class);
    
    private MetricsRecord metrics;
    
    private long pid;
    /*
    private String isMap;
    private String jobid;
    private String taskid;
    
    //PS metrics
    private int ID;
    private String user;
    private String stime;
    private int size;
    private int rss;
    private int share;
    private String state;
    private String time;
    private int cpu;
    private String command;
    */
   
    //private static final String HEADER =
  	//        "PID\tUSER\tSTIME\tSIZE\tRSS\tSHARE\tSTATE\tTIME\t%CPU\tCOMMAND";
    private SigarProxy proxy;
    
    public synchronized static ResourceMetrics init(long pid, String jobid, String taskid, boolean isMap) {
        return init(pid, jobid, taskid, isMap, "metrics");
      }
      
      public synchronized static ResourceMetrics init(long pid, String jobid, String taskid, boolean isMap,
        String recordName) {
          if (theInstance != null) {
        	  theInstance.metrics.setTag("pid", pid);
        	  theInstance.metrics.setTag("jobid", jobid);
        	  theInstance.metrics.setTag("taskid", taskid);
        	  
        	  log.info("Reinitializing Resources Metrics with pid=" 
                      + pid + ", taskId=" + taskid);
          }
          else {
              log.info("Initializing Resources Metrics with pid=" 
                      + pid + ", taskId=" + taskid);
              theInstance = new ResourceMetrics(pid, jobid, taskid, isMap, recordName);
          }
          return theInstance;
      }
      
      /** Creates a new instance of ResourcesMetrics */
      private ResourceMetrics(long pid, String jobid, String taskid, boolean isMap,
        String recordName) {
          MetricsContext context = MetricsUtil.getContext("resource");
          metrics = MetricsUtil.createRecord(context, recordName);
          metrics.setTag("pid", pid);
          metrics.setTag("jobid", jobid);
          metrics.setTag("taskid", taskid);
          if(isMap)
        	  metrics.setTag("mapORreduce", "MAP");
          else
        	  metrics.setTag("mapORreduce", "REDUCE");
          
          Sigar sigar = new Sigar();
        
          proxy = SigarProxyCache.newInstance(sigar);
          
          this.pid = pid;
          context.registerUpdater(this);
      }
      
      /**
       * This will be called periodically (with the period being configuration
       * dependent).
       */
      public void doUpdates(MetricsContext context) {
          doResourcesUpdates();
          metrics.update();
      }
      
      private void doResourcesUpdates() {
    	

    	  try {
    		  ProcCpu cpu = proxy.getProcCpu(pid);
    		  metrics.setMetric("cpuPercent", (float)cpu.getPercent()); //Get the Process cpu usage.
    		  metrics.setMetric("sys", cpu.getSys()); //Get the Process cpu kernel time.
    		  metrics.setMetric("total", cpu.getTotal()); // Get the Process cpu time (sum of User and Sys).
    		  metrics.setMetric("user", cpu.getUser()); // Get the Process cpu user time.             
            
    		  ProcMem mem = proxy.getProcMem(pid);
    		  metrics.setMetric("size",mem.getSize()); //Get the Total process virtual memory.
    		  metrics.setMetric("resident", mem.getResident()); //Get the Total process resident memory.
    		  metrics.setMetric("share", mem.getShare()); // Get the Total process shared memory.

    	  } catch (SigarException e) {
    		  log.error(e);
    	  }
  
      }
    
	

}
