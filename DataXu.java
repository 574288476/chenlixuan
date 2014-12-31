package main;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import utils.Config;
import utils.DateTransformer;
import utils.Spliter;
import DataXu.utils.IpToGeo;

import com.google.common.base.Charsets;

import org.apache.commons.codec.binary.Base64;


public class DataXu extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		if(args.length<6){
			System.out.println("main.DataXu <input> <output> <confile> <type> <china.csv> <geo2en>");
			System.exit(0);
		}
		ToolRunner.run(new DataXu(), args);
	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(args[1]), true);
		conf.set("ConfPath", args[2]);
		conf.set("TYPE", args[3]);
		
		Job job = Job.getInstance(conf,"DataXu scan");
		job.setJarByClass(DataXu.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setMapperClass(DataXuMap.class);
		job.setNumReduceTasks(0);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileInputFormat.setInputDirRecursive(job, true);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.addCacheFile(URI.create(args[4]));
		job.addCacheFile(URI.create(args[5]));
		job.waitForCompletion(true);
		return 0;
		
	}

}

class DataXuMap extends Mapper<LongWritable, Text,Text,NullWritable>{

	Map<String, Integer> proper = null;
	Map<Long, String> ip_code_city = new HashMap<Long,String>();
	String dom = null;
	
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		ArrayList<String> str = Spliter.splits(value.toString(), "\t");
		if(str.size() < proper.size()){
			System.out.println("line length: "+str.size() + "  proper length: " + proper.size());
			System.out.println(value.toString());
			context.getCounter("Error_log", "shorter_then_require").increment(1);
		}else{
			String campaign_id = str.get(proper.get("campaign_id"));
			if(!campaign_id.equalsIgnoreCase("100197")){
				return;
			}else{
				campaign_id = "0Duv9P9Muq";
			}
			String lineitem_id = "0FSYFjIBIR";
			
			String ip_code = IpToGeo.getGeo(str.get(proper.get("user_ip")));
			String r="";
			String m="";
			if(ip_code != "null" && ip_code != null){
				if(ip_code_city.containsKey(Long.parseLong(ip_code))){
					String city = ip_code_city.get(Long.parseLong(ip_code));
					String[] rm = city.split("_");
					if(rm.length==2){
						r = rm[1];
						m = rm[0];
					}
				}
			}
			String price;
			if(proper.containsKey("win_price"))
				price = (Float.parseFloat(str.get(proper.get("win_price"))))/100000+"";
			else
				price = "";
			
			byte[] bi = Base64.encodeBase64(str.get(proper.get("bid_id")).getBytes());
			byte[] ci = Base64.encodeBase64(str.get(proper.get("creative_id")).getBytes());
			byte[] l = Base64.encodeBase64(str.get(proper.get("creative_id")).getBytes());
			String time = "[".concat(DateTransformer.DataXuTimeFormat(str.get(proper.get("time")))).concat("] ");
			String ip = "- ".concat(str.get(proper.get("user_ip"))).concat(" - - ");
			String urlEncoder = URLEncoder.encode(str.get(proper.get("url")), "utf-8");
			String url = "\"GET /".concat(dom).concat("?al=").concat(str.get(proper.get("language"))).concat("&").
					concat("btid=").concat(new String(bi)).concat("&").
					concat("ci=").concat(campaign_id).concat("&").
					concat("ciu=").concat(new String(ci)).concat("&").
					concat("dm=").concat(str.get(proper.get("device"))).concat("&").
					concat("ei=").concat(str.get(proper.get("ad_exchange"))).concat("&").
					concat("fi=").concat(lineitem_id).concat("&").
					concat("os=").concat(str.get(proper.get("os"))).concat("&").
					concat("s=").concat(urlEncoder).concat("&").
					concat("scres=").concat(str.get(proper.get("size"))).concat("&").
					concat("sic=").concat(str.get(proper.get("site_category"))).concat("&").
					concat("upuid=").concat(str.get(proper.get("bilin_user_id"))).concat("&").
					concat("v=").concat(str.get(proper.get("adslot_visibility"))).concat("&").
					concat("wp_exchange=").concat(price).concat("&").
					concat("xid=").concat(str.get(proper.get("exchange_user_id"))).concat("&").
					concat("c=").concat("CN").concat("&").
					concat("r=").concat(r).concat("&").
					concat("m=").concat(m).
					concat(" HTTP/1.1\"");
			String unknown = " - - \"\" \"\" ";
			String wfivefivec = "\"\" ";
			String end = "\"-\"";
			
			String result = ip.concat(time).concat(url).concat(unknown).concat(wfivefivec).concat(end);
//			StringBuilder sb = new StringBuilder();
//			for(String tmp : items){
//				sb.append(tmp);
//				sb.append("\t");
//			}
			
			Text keyy = new Text(result);
			context.write(keyy, NullWritable.get());
		}
	} 

	@Override
	protected void setup(
			Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		String type = conf.get("TYPE");
		if(type.equals("imp"))
			dom = "a.gif";
		else if(type.equals("clk"))
			dom = "cl";
		else{
			System.out.println("properties error");
			System.exit(0);
		}
		
		Config.getInstance().LoadConf(conf.get("ConfPath"),type);
		proper = Config.getProper();
		
		URI[] localCacheFile = context.getCacheFiles();                //get ip to geo code file china.csv and loading
        FileSystem fs = FileSystem.get(localCacheFile[0], new Configuration());
        IpToGeo.loadGeoFile(localCacheFile[0].toString(),fs);
        
        FileSystem fs_geo_city = FileSystem.get(localCacheFile[1], new Configuration());
        BufferedReader br=null;
        try{
        	InputStreamReader in = new InputStreamReader(fs_geo_city.open(new Path(localCacheFile[1].toString())),Charsets.UTF_8);
        
        	br = new BufferedReader(in);
        	String line = null;
        	while ((line = br.readLine())!= null) {
        		StringTokenizer token = new StringTokenizer(line,"\t");
        		if(token.countTokens()==3){
        			long code = Long.parseLong(token.nextToken());
        			String city = token.nextToken()+"_"+token.nextToken();
        			ip_code_city.put(code, city);
        		}
        	}
        }finally{
        	br.close();
        }
        
	}
	
}
