<%@ page contentType="text/html; charset=UTF-8" import="javax.servlet.*"
	import="javax.servlet.http.*" import="java.io.*"
	import="java.lang.String" import="java.text.*" import="java.util.*"
	import="org.apache.hadoop.mapred.*" import="org.apache.hadoop.util.*"%>
<%!private static final long serialVersionUID = 2L;%>
<%
	//JobTracker tracker = (JobTracker) application.getAttribute("job.tracker");
	//String trackerName = 
	//         StringUtils.simpleHostname(tracker.getJobTrackerMachine());
	String jobid = request.getParameter("jobid");
	String taskid = request.getParameter("taskid");
	//JobID jobidObj = JobID.forName(jobid);
	//askAttemptID taskidObj = TaskAttemptID.forName(taskid);
	String isText = request.getParameter("text");

	String textHerf = "taskPerf.jsp?jobid=" + jobid + "&taskid="
			+ taskid + "&text=true";
	boolean isMap = taskid.contains("_m_");
%>

<html>
<head>
<title>Task Performance Graph of <%=taskid%></title>
</head>
<body>
	<%
	if(isText == null) {
		if (isMap)
			out.println("<h2>Map Task " + taskid + "</h2>");
		else
			out.println("<h2>Reduce Task " + taskid + "</h2>");

		out.println("<h2>Counters Graph</h2>");

		out.println("<p>");
		out.println("<img src=\"/taskPerfGraph?jobid=" + jobid + "&taskid="
				+ taskid + "&name=MRRecords\">"
				+ "<img src=\"/taskPerfGraph?jobid=" + jobid + "&taskid="
				+ taskid + "&name=HDFS\">");

		out.println("</p>");
		out.println("<p>");

		out.println("<img src=\"/taskPerfGraph?jobid=" + jobid + "&taskid="
				+ taskid + "&name=CombineRecords\">"
				+ "<img src=\"/taskPerfGraph?jobid=" + jobid + "&taskid="
				+ taskid + "&name=Bytes\">");
		if (!isMap) {
			out.println("</p><p><img src=\"/taskPerfGraph?jobid=" + jobid
					+ "&taskid=" + taskid + "&name=SpilledRecords\">");
		}
		out.println("</p>");
		out.println("<h2>Metrics From PidStat and Runtime working on MapReduce Tasks</h2>");
		out.println("<p>");
		out.println("<img src=\"/taskPerfGraph?jobid=" + jobid + "&taskid="
				+ taskid + "&name=CPUAndIO\"></p><p>"
				+ "<img src=\"/taskPerfGraph?jobid=" + jobid + "&taskid="
				+ taskid + "&name=Memory\">"
				+ "<img src=\"/taskPerfGraph?jobid=" + jobid + "&taskid="
				+ taskid + "&name=JVM\">");
		out.println("</p>");
		
		out.println("<h2>JVM Heap Usage</h2>");
		out.println("<p>");
		out.println("<img src=\"/taskPerfGraph?jobid=" + jobid + "&taskid="
				+ taskid + "&name=S0S1\">"
				+ "<img src=\"/taskPerfGraph?jobid=" + jobid + "&taskid="
				+ taskid + "&name=Eden\">"
				+ "<img src=\"/taskPerfGraph?jobid=" + jobid + "&taskid="
				+ taskid + "&name=Old\">"
				+ "<img src=\"/taskPerfGraph?jobid=" + jobid + "&taskid="
				+ taskid + "&name=Perm\">"
				+ "<img src=\"/taskPerfGraph?jobid=" + jobid + "&taskid="
				+ taskid + "&name=GC\">");
		out.println("</p>");
		
		out.println("<p>");
		out.println("<h2><a href = " + textHerf
				+ ">Text Version of Metrics/Counters/JVM/Jstat</a></h2>");
		out.println("</p>");

		out.println(ServletUtil.htmlFooter());
	}
	else if(isText.equalsIgnoreCase("true")) {
		String logPath = System.getProperty("hadoop.log.dir");
		String myMetrics;
     	if(logPath.endsWith(File.separator))
     		myMetrics = logPath + "myMetrics";
     	else
     		myMetrics = logPath + File.separator + "myMetrics";
     	String jobidDir = myMetrics + File.separator + jobid + File.separator;
     	
     	File countersFile = new File(jobidDir + taskid + ".counters");
     	File metricsFile = new File(jobidDir + taskid + ".metrics");
     	File jvmFile = new File(jobidDir + taskid + ".jvm");
     	File jstatFile = new File(jobidDir + taskid + ".jstat");
     	
		try {
			BufferedReader reader;
			String line;
			out.println("<h2>Counters Information</h2>");
			out.println("<pre>");
			if(countersFile.exists()) {
		
				reader = new BufferedReader(new FileReader(countersFile));
				while ((line = reader.readLine()) != null) {
					out.println(line);		
				}
				reader.close();
			}
			out.println("</pre>");
			
			out.println("<h2>Metrics Information</h2>");
			out.println("<pre>");
			if(metricsFile.exists()) {
		
				reader = new BufferedReader(new FileReader(metricsFile));
				while ((line = reader.readLine()) != null) {
					out.println(line);		
				}
				reader.close();
			}
			out.println("</pre>");
			
			out.println("<h2>JVM Memory Information</h2>");
			out.println("<pre>");
			if(jvmFile.exists()) {
		
				reader = new BufferedReader(new FileReader(jvmFile));
				while ((line = reader.readLine()) != null) {
					out.println(line);		
				}
				reader.close();
			}
			out.println("</pre>");
			
			out.println("<h2>JVM Heap Usage</h2>");
			out.println("<pre>");
			if(jstatFile.exists()) {
		
				reader = new BufferedReader(new FileReader(jstatFile));
				while ((line = reader.readLine()) != null) {
					out.println(line);		
				}
				reader.close();
			}
			out.println("</pre>");

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		out.println(ServletUtil.htmlFooter());
	}
	%>