package com.yunsom;

import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.mapreduce.Reducer;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
import org.apache.hadoop.util.GenericOptionsParser;  
  
import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

import java.io.IOException;  
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
   
public class Extract {  
   
	 public static class MyMapper extends Mapper<Object, Text, Text, IntWritable>{  
	        private final static IntWritable one = new IntWritable(1);  
	        private Text event = new Text(); 
	        String temp = null;
	        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {  
	        	String uid = null;
	        	String stoid = null;
				String stoids = null;
				String catid = null;
				String catids = null;
				String plaid = null;
				String braid = null;
				String braids = null;
				String comid = null;
				String comids = null;
				String ids = null;
				String host = null;
				String url = null;
	        	temp = value.toString();
	        	if (temp.indexOf("uid=") >= 0&& temp.indexOf("XMLHttpRequest")<0) {
					String[] strLogs = temp.split("\t");
					for (int i = 0; i < strLogs.length; i++) {
						if (strLogs[i].indexOf("url[") >= 0) {
							url = strLogs[i].substring(
									strLogs[i].indexOf("url[") + 4,
									strLogs[i].indexOf("]"));
							System.out.println(url);
							if(url.indexOf("store") >= 0 || url.indexOf("category") >= 0
									|| url.indexOf("brand") >= 0
									|| url.indexOf("commodity") >= 0){
							host = strLogs[i].substring(strLogs[i]
									.indexOf("url[") + 4);
							java.net.URL urls = new java.net.URL(host);
							plaid = urls.getHost();
							System.out.println("plaid" + "--" + plaid);
							String[] strUrls = url.split("/");
							int length = strUrls.length - 1;
							for (int j = 0; j < strUrls.length; j++) {
								if (strUrls[length].contains(".js")
										|| strUrls[length].contains(".css")
										|| strUrls[length].contains(".xml")
										|| strUrls[length].contains("ajax")
										|| strUrls[length].contains(".png")
										|| strUrls[length].contains(".jpg")
										) {
									System.out.println("222222222"
											+ strUrls[strUrls.length - 1]);
									plaid = null;
									break;
								}
								if (strUrls[j].equals("store")) {
									
									//Extract number
									
									try {
										stoids = strUrls[j + 1];
										if (stoids.split("\\?").length == 1) {
											boolean flag = false;
											flag = stoids.matches("[0-9]{1,}");
											if (flag) {
												stoid = stoids;
												flag = true;
											}
											else{
												stoid = null;
											}
										}

											else {
												stoid = stoids.split("\\?")[0];
											}
                      

								}catch (Exception e) {
										// TODO: handle exception
									catid = null;
										System.out.println(catid);}
								}
								if (strUrls[j].equals("category")) {
									
									//Extract number
									
									try {
										catids = strUrls[j + 1];
										if (catids.split("\\?").length == 1) {
											boolean flag = false;
											flag = catids.matches("[0-9]{1,}");
											if (flag) {
												catid = catids;
												flag = true;
											}
											else{
												catid = null;
											}
										}

											else {
												catid = catids.split("\\?")[0];
											}
                      

								}catch (Exception e) {
										// TODO: handle exception
									catid = null;
										System.out.println(catid);}
								}
								if (strUrls[j].equals("brand")) {
									
									//Extract number
									
									try {
										braids = strUrls[j + 1];
										if (braids.split("\\?").length == 1) {
											boolean flag = false;
											flag = braids.matches("[0-9]{1,}");
											if (flag) {
												braid = braids;
												flag = true;
											}
											else{
												braid = null;
											}
										}

											else {
												braid = braids.split("\\?")[0];
											}
                      

								}catch (Exception e) {
										// TODO: handle exception
									catid = null;
										System.out.println(braid);}
								}
								if (strUrls[j].equals("commodity")) {
									try {
										comids = strUrls[j + 1];
										if (comids.split("\\?").length == 1) {
											boolean flag = false;
											flag = comids.matches("[0-9]{1,}");
											if (flag) {
												comid = comids;
												flag = true;
											}

											else {
												byte[] jb = null;
												String s = null;
												byte[] b = null;
												String result = null;
												BASE64Decoder decoder = new BASE64Decoder();
												try {
													b = decoder.decodeBuffer(comids);															
													result = new String(b,"utf-8");															
													jb = result.getBytes("utf-8");
													if (jb != null) {
														s = new BASE64Encoder().encode(jb);
																													}
													if(s.equals(comids)){
														comid = result.split("x")[0];
													}
													else{
														plaid = null;
													}
													
												} catch (Exception e) {
													e.printStackTrace();
												}

											}

										} else {
											comid = comids.split("\\?")[1];
											Pattern pattern = Pattern.compile("\\d+");  
										    Matcher matcher = pattern.matcher(comid);  
										    while (matcher.find()) {  
										    	comid = matcher.group(0);  
										    }  
										}

										// System.out.println(result);
										System.out.println(comid);
									} catch (Exception e) {
										// TODO: handle exception
										comid = null;
										System.out.println(comid);
									}

								}

							}
					

						}
						
					}
						if (strLogs[i].indexOf("cookie[") >= 0) {
							String cookie = strLogs[i].substring(
									strLogs[i].indexOf("cookie[") + 7).replace(
									"]", "");
							//System.out.println("55555555555"+cookie);
							String[] cookies = cookie.split(";");
							for (int k = 0; k < cookies.length; k++) {
								if (cookies[k].contains("uid")) {
									uid = cookies[k].substring(cookies[k]
											.indexOf("uid=") + 4);
									

								}
							}
						}		
						
						
					
			}
			
					if (plaid == null) {
						ids = null;

					} else {
						ids = uid + "\t" + plaid + "\t" + stoid + "\t" + comid + "\t" + catid + "\t" + braid;
			            event.set(ids);  
			            context.write(event, one); 
					}		
			}
	        	 
	             
	        }  
	    }  
	   
	    public static class MyReducer extends Reducer<Text,IntWritable,Text,IntWritable> {  
	        private IntWritable result = new IntWritable();  
	   
	        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {  
	            String sr = null;
	        	int sum = 0;   
	            result.set(sum);  
	            context.write(key, result);  
	        }  
	    }  
	   
	    public static void main(String[] args) throws Exception { 
	    	Extract extract = new Extract();
	        Configuration conf = new Configuration();  
	        String[] ioArgs = new String[2];
	        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	        if (otherArgs.length != 2) {
	        	ioArgs[0] = "hdfs://yunsom/user/hadoop/logtable/day="+extract.getDay();
	        	ioArgs[1] = "hdfs://yunsom/user/hadoop/keyword_id/day="+extract.getDay();
	            //System.err.println("Usage: EventCount <in> <out>");  
	            //System.exit(2);  
	        } else{
	        	ioArgs[0] =otherArgs[0];
	        	ioArgs[1] = otherArgs[1];
	        } 
	        Job job = Job.getInstance(conf, "event count");  
	        job.setJarByClass(Extract.class);  
	        job.setMapperClass(MyMapper.class);  
	        job.setCombinerClass(MyReducer.class);  
	        job.setReducerClass(MyReducer.class);  
	        job.setOutputKeyClass(Text.class);  
	        job.setOutputValueClass(IntWritable.class);  
	        FileInputFormat.addInputPath(job, new Path(ioArgs[0]));  
	        FileOutputFormat.setOutputPath(job, new Path(ioArgs[1]));  
	        System.exit(job.waitForCompletion(true) ? 0 : 1);  
	    }

	    public String getDay() {
			// TODO Auto-generated method stub
			Date dNow = new Date();  
			Date dBefore = new Date();
			Calendar calendar = Calendar.getInstance(); 
			calendar.setTime(dNow);
			calendar.add(Calendar.DAY_OF_MONTH, -1); 
			dBefore = calendar.getTime();   
			SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMdd"); 
			String defaultStartDate = sdf.format(dBefore);    
			String defaultEndDate = sdf.format(dNow); 

			//System.out.println("前一天的时间是：" + defaultStartDate);
			//System.out.println("生成的时间是：" + defaultEndDate); 
			return defaultStartDate;
		}  
}  
