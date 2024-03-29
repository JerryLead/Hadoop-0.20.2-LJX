/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSError;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.mapred.JvmTask;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.jvm.JvmMetrics;
import org.apache.hadoop.metrics.jvm.ResourceMetrics;
import org.apache.log4j.LogManager;
import org.apache.hadoop.util.StringUtils;
import org.hyperic.sigar.Sigar;

/** 
 * The main() for child processes. 
 */

class Child {

  public static final Log LOG =
    LogFactory.getLog(TaskTracker.class);

  static volatile TaskAttemptID taskid = null;
  static volatile boolean isCleanup;
  
  //added by LijieXu
  private static String myMetrics = null;
 
  public static int getPid() {  
      RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();  
      String name = runtime.getName(); // format: "pid@hostname"  

      try {  
          return Integer.parseInt(name.substring(0, name.indexOf('@')));  
      } catch (Exception e) {  
          return -1;  
      }  
  }
  //added end
  
  public static void main(String[] args) throws Throwable {
    LOG.debug("Child starting");

    JobConf defaultConf = new JobConf();
    String host = args[0];
    int port = Integer.parseInt(args[1]);
    InetSocketAddress address = new InetSocketAddress(host, port);
    final TaskAttemptID firstTaskid = TaskAttemptID.forName(args[2]);
    final int SLEEP_LONGER_COUNT = 5;
    int jvmIdInt = Integer.parseInt(args[3]);
    JVMId jvmId = new JVMId(firstTaskid.getJobID(),firstTaskid.isMap(),jvmIdInt);
    TaskUmbilicalProtocol umbilical =
      (TaskUmbilicalProtocol)RPC.getProxy(TaskUmbilicalProtocol.class,
          TaskUmbilicalProtocol.versionID,
          address,
          defaultConf);
    int numTasksToExecute = -1; //-1 signifies "no limit"
    int numTasksExecuted = 0;
    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        try {
          if (taskid != null) {
            TaskLog.syncLogs(firstTaskid, taskid, isCleanup);
          }
        } catch (Throwable throwable) {
        }
      }
    });
    Thread t = new Thread() {
      public void run() {
        //every so often wake up and syncLogs so that we can track
        //logs of the currently running task
        while (true) {
          try {
            Thread.sleep(5000);
            if (taskid != null) {
              TaskLog.syncLogs(firstTaskid, taskid, isCleanup);
            }
          } catch (InterruptedException ie) {
          } catch (IOException iee) {
            LOG.error("Error in syncLogs: " + iee);
            System.exit(-1);
          }
        }
      }
    };
    t.setName("Thread for syncLogs");
    t.setDaemon(true);
    t.start();
    //for the memory management, a PID file is written and the PID file
    //is written once per JVM. We simply symlink the file on a per task
    //basis later (see below). Long term, we should change the Memory
    //manager to use JVMId instead of TaskAttemptId
    Path srcPidPath = null;
    Path dstPidPath = null;
    int idleLoopCount = 0;
    Task task = null;
    
    
    try {
      while (true) {
        taskid = null;
        JvmTask myTask = umbilical.getTask(jvmId);
        if (myTask.shouldDie()) {
          break;
        } else {
          if (myTask.getTask() == null) {
            taskid = null;
            if (++idleLoopCount >= SLEEP_LONGER_COUNT) {
              //we sleep for a bigger interval when we don't receive
              //tasks for a while
              Thread.sleep(1500);
            } else {
              Thread.sleep(500);
            }
            continue;
          }
        }
        idleLoopCount = 0;
        task = myTask.getTask();
        taskid = task.getTaskID();
        isCleanup = task.isTaskCleanupTask();
        // reset the statistics for the task
        FileSystem.clearStatistics();

        //create the index file so that the log files 
        //are viewable immediately
       
        TaskLog.syncLogs(firstTaskid, taskid, isCleanup);
        JobConf job = new JobConf(task.getJobFile());
        if (job.getBoolean("task.memory.mgmt.enabled", false)) {
          if (srcPidPath == null) {
            srcPidPath = new Path(task.getPidFile());
          }
          //since the JVM is running multiple tasks potentially, we need
          //to do symlink stuff only for the subsequent tasks
          if (!taskid.equals(firstTaskid)) {
            dstPidPath = new Path(task.getPidFile());
            FileUtil.symLink(srcPidPath.toUri().getPath(), 
                dstPidPath.toUri().getPath());
          }
        }
        //setupWorkDir actually sets up the symlinks for the distributed
        //cache. After a task exits we wipe the workdir clean, and hence
        //the symlinks have to be rebuilt.
        TaskRunner.setupWorkDir(job);

        numTasksToExecute = job.getNumTasksToExecutePerJvm();
        assert(numTasksToExecute != 0);
        TaskLog.cleanup(job.getInt("mapred.userlog.retain.hours", 24));

        task.setConf(job);

        defaultConf.addResource(new Path(task.getJobFile()));

        // Initiate Java VM metrics
        //JvmMetrics.init(task.getPhase().toString(), job.getSessionId());
     
        //added by LijieXu

        // Initiate Resource metrics
        
       
        Thread pidStat = null;
        if(task.isMapOrReduce() && job.getInt("child.monitor.metrics.seconds", 0) != 0) {
        	
        	 if(myMetrics == null) {
             	String logPath = System.getProperty("hadoop.log.dir");
             	if(logPath.endsWith(File.separator))
             		myMetrics = logPath + "myMetrics";
             	else
             		myMetrics = logPath + File.separator + "myMetrics";
             }
        	 final File myMetricsDir = new File(myMetrics, task.getJobID().toString());	
             if(!myMetricsDir.exists())
            	 myMetricsDir.mkdirs();			
             final int interval = job.getInt("child.monitor.metrics.seconds", 0);
             
             pidStat = new Thread() {
            	private int pid = getPid();
            	private String shellcommand = "exec '/usr/bin/pidstat' -dru -p " + pid 
        				+ " -h " + interval + " > " + myMetricsDir.getAbsolutePath()+ File.separator + taskid + ".metrics";
        		@Override
        		public void run() {	
            			Process p = null;
            			try {
            				p = new ProcessBuilder("bash", "-c", shellcommand).start();
            				
            				int exitCode = p.waitFor();
            				
            			} catch (IOException e) {
            				 LOG.error("Cannot create pidstat metrics file", e);
            			} catch (InterruptedException e) {
            				// TODO Auto-generated catch block
            				//e.printStackTrace();
            			} finally {
            				p.destroy();
            			}      		
        		}
        			
        	 };
        	 pidStat.start();
        	
        }
        
        Thread jvmStat = null;
        
    	if(task.isMapOrReduce() && job.getInt("child.monitor.jvm.seconds", 0) != 0 ) {
    		 if(myMetrics == null) {
              	String logPath = System.getProperty("hadoop.log.dir");
              	if(logPath.endsWith(File.separator))
              		myMetrics = logPath + "myMetrics";
              	else
              		myMetrics = logPath + File.separator + "myMetrics";
              }
         	 final File myMetricsDir = new File(myMetrics, task.getJobID().toString());	
             if(!myMetricsDir.exists())
             	 myMetricsDir.mkdirs();			
              
             final int jvmInterval = job.getInt("child.monitor.jvm.seconds", 0);
              
             jvmStat = new Thread() {
    		 
				 private String jvmPath = myMetricsDir.getAbsolutePath()+ File.separator + taskid + ".jvm";
				 private BufferedWriter writer = null;
				 
				 @Override
				 public void run() {
					 try {
						 writer = new BufferedWriter(new FileWriter(jvmPath));
						 Runtime rt = Runtime.getRuntime();
						 writer.write("Time\tJVMUsed\tTotal\tMax\n");
						 
						 while(!isInterrupted()) {
							 writer.write(System.currentTimeMillis() / 1000 + "\t");
							 writer.write((rt.totalMemory() - rt.freeMemory())/1024/1024 + "\t"
									 + rt.totalMemory()/1024/1024 + "\t"
									 + rt.maxMemory()/1024/1024 + "\n");
							 Thread.sleep(jvmInterval * 1000);
						 }
						
					 } catch (InterruptedException e) {
						// TODO Auto-generated catch block
						//e.printStackTrace();
						try {
							writer.close();
						} catch (IOException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
						return;
					 } catch (IOException e) {
						 LOG.error("Cannot create jvm metrics file: " + jvmPath, e);
						
						 try {
							writer.close();
						} catch (IOException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
						e.printStackTrace();
						return;
						 
					 } 
				 }
				 
             };
             jvmStat.start();	
		}
        
    
        
        //added end
       
    	//added by LijieXu JVM Statistics Thread
    	 Thread jstat = null;
         if(task.isMapOrReduce() && job.getInt("child.monitor.jstat.seconds", 0) != 0) {
         	
        	 if(myMetrics == null) {
               	String logPath = System.getProperty("hadoop.log.dir");
               	if(logPath.endsWith(File.separator))
               		myMetrics = logPath + "myMetrics";
               	else
               		myMetrics = logPath + File.separator + "myMetrics";
             }
        	 
          	 final File myMetricsDir = new File(myMetrics, task.getJobID().toString());	
             if(!myMetricsDir.exists())
              	 myMetricsDir.mkdirs();			
              
             final int interval = job.getInt("child.monitor.jstat.seconds", 0);
              
             jstat = new Thread() {
             	private int pid = getPid();
             	private String output = myMetricsDir.getAbsolutePath()+ File.separator + taskid + ".jstat";
             	private String date = "exec date +%s >> " + output;
             	private String gccapacity = "exec $JAVA_HOME/bin/jstat -gccapacity " + pid + " > " + output;
    			private String gc = "exec $JAVA_HOME/bin/jstat -gc -t " + pid + " " + interval + "s >> " + output;
    			
             	
         		@Override
         		public void run() {	
             			Process p = null;
             			try {	
             				p = new ProcessBuilder("bash", "-c", gccapacity).start();
        					p.waitFor();
        					
        					p = new ProcessBuilder("bash", "-c", date).start();
        					p.waitFor();
        				
        					p = new ProcessBuilder("bash", "-c", gc).start();
             				
             				int exitCode = p.waitFor();
             				
             			} catch (IOException e) {
             				LOG.error("From Jstat Read/Write", e);
             			} catch (InterruptedException e) {
             				// TODO Auto-generated catch block
             				//e.printStackTrace();
             			} finally {
             				p.destroy();
             			}      		
         		}
         			
         	 };
         	 jstat.start();
         	
         }
    	//added end
        
        // use job-specified working directory
        FileSystem.get(job).setWorkingDirectory(job.getWorkingDirectory());
        try {
          task.run(job, umbilical);             // run the task
        } finally {
          TaskLog.syncLogs(firstTaskid, taskid, isCleanup);
          if (!taskid.equals(firstTaskid) && 
              job.getBoolean("task.memory.mgmt.enabled", false)) {
            // delete the pid-file's symlink
            new File(dstPidPath.toUri().getPath()).delete();
          }
        }
        
        //added by Lijie Xu
        //if(task.isMapOrReduce()) {
        //	MetricsContext metricsContext = MetricsUtil.getContext("resource");
        //	metricsContext.close();
        //}       
        if(pidStat != null)
        	pidStat.interrupt();
        if(jvmStat != null)
        	jvmStat.interrupt();
        if(jstat != null)
        	jstat.interrupt();
        //added end
        
        if (numTasksToExecute > 0 && ++numTasksExecuted == numTasksToExecute) {
          break;
        }
      }
    } catch (FSError e) {
      LOG.fatal("FSError from child", e);
      umbilical.fsError(taskid, e.getMessage());
    } catch (Exception exception) {
      LOG.warn("Error running child", exception);
      try {
        if (task != null) {
          // do cleanup for the task
          task.taskCleanup(umbilical);
        }
      } catch (Exception e) {
        LOG.info("Error cleaning up" + e);
      }
      // Report back any failures, for diagnostic purposes
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      exception.printStackTrace(new PrintStream(baos));
      if (taskid != null) {
        umbilical.reportDiagnosticInfo(taskid, baos.toString());
      }
    } catch (Throwable throwable) {
      LOG.fatal("Error running child : "
                + StringUtils.stringifyException(throwable));
      if (taskid != null) {
        Throwable tCause = throwable.getCause();
        String cause = tCause == null 
                       ? throwable.getMessage() 
                       : StringUtils.stringifyException(tCause);
        umbilical.fatalError(taskid, cause);
      }
    } finally {
        
      RPC.stopProxy(umbilical);
      //MetricsContext metricsContext = MetricsUtil.getContext("jvm");
      //metricsContext.close();
      // Shutting down log4j of the child-vm... 
      // This assumes that on return from Task.run() 
      // there is no more logging done.
      LogManager.shutdown();
      
    }
  }
}
