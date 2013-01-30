package org.apache.hadoop.mapred;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import javax.imageio.ImageIO;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import cn.ac.iscas.counters.TaskCountersImage;
import cn.ac.iscas.jstat.JstatMetricsImage;
import cn.ac.iscas.jvm.JvmMetricsImage;
import cn.ac.iscas.metrics.TaskMetricsImage;

public class TaskPerfGraphServlet extends HttpServlet {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static String logDir = System.getProperty("hadoop.log.dir");

	/**
	 * Get the task performance metrics from local file system
	 */

	@Override
	public void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		String jobid = request.getParameter("jobid");
		String taskid = request.getParameter("taskid"); // attempt...
		String name = request.getParameter("name");

		if (jobid == null || taskid == null) {
			response.sendError(HttpServletResponse.SC_BAD_REQUEST,
					"Argument taskid is required");
			return;
		}

		File metricsFile = new File(logDir, "myMetrics" + File.separator
				+ jobid + File.separator + taskid + ".metrics");
		File countersFile = new File(logDir, "myMetrics" + File.separator
				+ jobid + File.separator + taskid + ".counters");
		File jvmFile = new File(logDir, "myMetrics" + File.separator
				+ jobid + File.separator + taskid + ".jvm");
		
		File jstatFile = new File(logDir, "myMetrics" + File.separator
				+ jobid + File.separator + taskid + ".jstat");

		List<BufferedImage> imageList = null;

		synchronized (ImageCache.class) {
			if (ImageCache.inCache(taskid))
				imageList = ImageCache.getImageList(taskid);
			else {
				boolean isMap = taskid.contains("_m_");
				imageList = new ArrayList<BufferedImage>();

				List<ArrayList<String>> countersList = null;
				List<ArrayList<String>> metricsList = null;
				List<ArrayList<String>>	jvmList = null;
				List<ArrayList<String>> jstatList = null;

				if (countersFile.exists())
					countersList = TaskCountersImage.parse(countersFile, isMap);
				if (metricsFile.exists())
					metricsList = TaskMetricsImage.parse(metricsFile);
				if (jvmFile.exists())
					jvmList = JvmMetricsImage.parse(jvmFile);
				if (jstatFile.exists())
					jstatList = JstatMetricsImage.parse(jstatFile);

				if (metricsList == null || metricsList.isEmpty()) {
					imageList.add(null);
					imageList.add(null);
				} else {
					imageList.add(TaskMetricsImage.plotCpuAndIO(metricsList));

					imageList.add(TaskMetricsImage.plotMEM(metricsList));
				}

				if (jvmList == null || jvmList.isEmpty()) {
					imageList.add(null);
					
				} else {
					imageList.add(JvmMetricsImage.plotJvmMem(jvmList));
				}
				
				if (jstatList == null || jstatList.isEmpty()) {
					imageList.add(null);
					imageList.add(null);
					imageList.add(null);
					imageList.add(null);
					imageList.add(null);
				}
				else {
					imageList.add(JstatMetricsImage.plotSurvivorSpace(jstatList));
					imageList.add(JstatMetricsImage.plotEdenAndNewGen(jstatList));
					imageList.add(JstatMetricsImage.plotOldGen(jstatList));
					imageList.add(JstatMetricsImage.plotPermGen(jstatList));
					imageList.add(JstatMetricsImage.plotGC(jstatList));
				}

				
				if (countersList == null || countersList.isEmpty()) {
					imageList.add(null);
					imageList.add(null);
					imageList.add(null);
					if(!isMap)
						imageList.add(null);
				} else {
					imageList.add(TaskCountersImage.plotMRRecords(countersList,
							isMap));
					
					imageList.add(TaskCountersImage.plotHDFS(countersList, 
							isMap));		
					
					imageList.add(TaskCountersImage.plotCombineRecords(countersList,
							isMap));
					
					imageList.add(TaskCountersImage.plotBytes(countersList,
							isMap));	
					
					if(!isMap)
						imageList.add(TaskCountersImage.plotReduceSplilledRecords(countersList));
				}
				

				ImageCache.addImage(taskid, imageList);

			}
		}
		response.reset();
		response.setContentType("image/png");
		OutputStream os = response.getOutputStream();

		BufferedImage image = null;

		if (name.equals("CPUAndIO"))
			image = imageList.get(0);
		else if (name.equals("Memory"))
			image = imageList.get(1);
		
		else if (name.equals("JVM"))
			image = imageList.get(2);
		
		else if (name.equals("S0S1"))
		    image = imageList.get(3);
		else if (name.equals("Eden"))
			image = imageList.get(4);
		else if (name.equals("Old"))
			image = imageList.get(5);
		else if (name.equals("Perm"))
			image = imageList.get(6);
		else if (name.equals("GC"))
			image = imageList.get(7);
		
		else if (name.equals("MRRecords"))
			image = imageList.get(8);
		else if (name.equals("HDFS"))
			image = imageList.get(9);
		else if (name.equals("CombineRecords"))
			image = imageList.get(10);
		else if (name.equals("Bytes"))
			image = imageList.get(11);
		else if (name.equals("SpilledRecords"))
			image = imageList.get(12);
		

		if (image != null)
			ImageIO.write(image, "png", os);
		os.close();

		/*
		 * String jobId = request.getParameter("jobid"); String taskId =
		 * request.getParameter("taskid"); //attempt... String name =
		 * request.getParameter("name");
		 * 
		 * if (jobId == null || taskId == null) {
		 * response.sendError(HttpServletResponse.SC_BAD_REQUEST,
		 * "Argument taskid is required"); return; } File metricsFile = new
		 * File(logDir, "myMetrics" + File.separator + jobId + File.separator +
		 * taskId + ".metrics"); File countersFile = new File(logDir,
		 * "myMetrics" + File.separator + jobId + File.separator + taskId +
		 * ".counters");
		 * 
		 * response.reset(); response.setContentType("image/png"); OutputStream
		 * os = response.getOutputStream();
		 * 
		 * BufferedImage image = null; boolean isMap = taskId.contains("_m_");
		 * List<ArrayList<String>> countersList = null; List<ArrayList<String>>
		 * metricsList = null;
		 * 
		 * if (countersFile.exists() && (name.equals("HDFS") ||
		 * name.equals("Records") || name.equals("Bytes"))) countersList =
		 * TaskCountersImage.parse(countersFile, isMap); else if
		 * (metricsFile.exists()) metricsList =
		 * TaskMetricsImage.parse(metricsFile);
		 * 
		 * if (name.equals("HDFS") && !countersList.isEmpty()) image =
		 * TaskCountersImage.plotHDFS(countersList, isMap); else if
		 * (name.equals("Records") && !countersList.isEmpty()) image =
		 * TaskCountersImage.plotRecords(countersList, isMap); else if
		 * (name.equals("Bytes") && !countersList.isEmpty()) image =
		 * TaskCountersImage.plotBytes(countersList, isMap);
		 * 
		 * 
		 * else if (name.equals("CPUAndIO") && !metricsList.isEmpty()) image =
		 * TaskMetricsImage.plotCpuAndIO(metricsList); else if
		 * (name.equals("Memory") && !metricsList.isEmpty()) image =
		 * TaskMetricsImage.plotMEM(metricsList);
		 * 
		 * if (image != null) ImageIO.write(image, "png", os); os.close();
		 */
	}

}
