import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.pestricOptionsParser;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapreduce.Cluster;


public class Part3 {

	public static class pestMapper
		extends Mapper<Object, Text, Text, Text> { 
		private Text pestId = new Text();
        private Text pestInfo = new Text();


	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(),",");
      		int entryNumber = 0;
             
      		String plantNumber= "";
            String[] plantInfo; 
      		while (itr.hasMoreTokens()) {
        		if (entryNumber == 0) {
                    plantInfo = itr.nextToken().toString().split("_");
                    plantNumber = plantInfo[1];
                    entryNumber++;
                	continue;
        		}
                double currExpr =  Double.parseDouble(itr.nextToken());
                if (currExpr != 0) {
                    pestId.set("pest_" + entryNumber);
                    pestInfo.set(plantNumber+ "," + currExpr);
                    context.write(pestId, pestInfo);
                }
                entryNumber++;
                
            } 
              
       	}
            
    }
    

    public static class pestReducer extends Reducer<Text, Text,Text, Text> {
        private Text result = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> vals = new ArrayList<String>(); 
            StringBuilder sb = new StringBuilder(); 
           
            for (Text val: values) {
                vals.add(val.toString());

            }
            for(int i = 0; i < vals.size(); i++) {
                String[] info1  = vals.get(i).split(",");
                int s1 = Integer.parseInt(info1[0]);
                double v1 = Double.parseDouble(info1[1]); 
                for (int j = 0; j < vals.size(); j++) {
                    String[] info2 = vals.get(j).split(","); 
                    int s2 = Integer.parseInt(info2[0]);
                    double v2 = Double.parseDouble(info2[1]); 
                    double product = v1 * v2;
                    if (s1 < s2) {
                        result.set(s1 + "," + s2 + "," + product);
                        context.write(key, result);
                    }
                             
                }      
            }
     
        }
    }

    public static class plantMapper
        extends Mapper<Object, Text, Text, DoubleWritable> { 
        private Text plants = new Text();
        private DoubleWritable pestProduct = new DoubleWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(),",");
        
            while (itr.hasMoreTokens()) { 
                plants.set("plant_" + itr.nextToken()+ ",plant_" + itr.nextToken());
                pestProduct.set(Double.parseDouble(itr.nextToken()));
                context.write(plants, pestProduct);
            }             
        }
    }

    public static class plantReducer extends Reducer<Text, DoubleWritable,Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0.0;
            for (DoubleWritable val : values) {
                sum += val.get();    
            }  
            result.set(sum); 
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
    	Configuration conf1 = new Configuration();
    	conf1.set("mapreduce.output.textoutputformat.separator", "\t");
    	String[] otherArgs = new pestricOptionsParser(conf1, args).getRemainingArgs();
    	if (otherArgs.length != 2) {
      		System.err.println("Usage: plant expr value <in> <out>");
      		System.exit(2);
    	}
    	Job job1 = new Job(conf1, "pestInfo");
    	job1.setJarByClass(Part3.class);
    	job1.setMapperClass(pestMapper.class);
        job1.setReducerClass(pestReducer.class);
        
       
    	job1.setOutputKeyClass(Text.class);
    	job1.setOutputValueClass(Text.class);
    	FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
    	FileOutputFormat.setOutputPath(job1,  new Path("/user/ysalmasi/introutput"));
      
    	job1.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        conf2.set("mapreduce.output.textoutputformat.separator", ",");
        Job job2 = new Job(conf2, "plantInfo");
        job2.setJarByClass(Part3.class);
        job2.setMapperClass(plantMapper.class);
        job2.setCombinerClass(plantReducer.class);
        job2.setReducerClass(plantReducer.class);
        job2.setInputFormatClass(KeyValueTextInputFormat.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job2, new Path("/user/ysalmasi/introutput"));
        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]));
      
        System.exit(job2.waitForCompletion(true) ? 0 : 1);

  	}
}



