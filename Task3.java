import java.io.IOException;
import java.io.StringReader;

import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

import javax.xml.parsers.*;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.*;
import org.xml.sax.SAXException;
import org.xml.sax.InputSource;

public class Task3 {

    public static class TokenizerMapper extends Mapper < Object, Text, Text, VersionNr > {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            VersionNr result = new VersionNr();
            Text outputKey = new Text();
            String elementName = "";

            String line = value.toString();
            if(line.contains("version=\"") && ( line.contains("node") || line.contains("relation") || line.contains("way") )){
                elementName = line.substring(line.indexOf("<")+1, line.indexOf(" ", 2));
                outputKey.set(elementName);
                result.setNodeName(elementName);
                result.setUpdateAmount(Float.parseFloat(line.substring(line.indexOf("version=\"")+9, line.indexOf("\" ", line.indexOf("version=\"")))));
                result.setNodeId(Long.parseLong(line.substring(line.indexOf("id=\"")+4, line.indexOf("\" ", line.indexOf("id=\"")))));
                context.write(outputKey, result);
            }
        }
    }

    public static class GetHighestVersionNrNode extends Reducer < Text, VersionNr, Text, VersionNr > {

        private VersionNr result = new VersionNr();
        private final Text outputText = new Text("Most updated element:");

        // Keeps track of the highest amount of times a node was updated
        float highestVersionValue = 0;

        public void reduce(Text key, Iterable<VersionNr> values, Context context) throws IOException, InterruptedException {

            for (VersionNr version : values) {
                float vNr = version.getUpdateAmount();

                // Compares the node update nrs & updates result fields/vars
                if(highestVersionValue < vNr) {
                    highestVersionValue = vNr;
                    result = new VersionNr(version.getNodeName(), vNr, version.getNodeId());
                }
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            context.write(outputText, result);
        }
    }

    public static class VersionNr implements Writable {
        private String nodeName;
        private float updateAmount;
        private long nodeId;

        public VersionNr(){
            nodeName = "";
            updateAmount = 0;
            nodeId = 0;
        }
        public VersionNr(String nodeName, float updateAmount, long nodeId){
            this.nodeName = nodeName;
            this.updateAmount = updateAmount;
            this.nodeId = nodeId;
        }
        public float getUpdateAmount() {
            return updateAmount;
        }
        public void setUpdateAmount(float updateAmount) {
            this.updateAmount = updateAmount;
        }
        public String getNodeName() {
            return nodeName;
        }
        public void setNodeName(String nodeName) {
            this.nodeName = nodeName;
        }
        public void setNodeId(long nodeId) {
            this.nodeId = nodeId;
        }
        public long getNodeId() {
            return nodeId;
        }
        public void readFields(DataInput in) throws IOException {
            updateAmount = in.readFloat();
            nodeId = in.readLong();
            nodeName = in.readUTF();
        }
        public void write(DataOutput out) throws IOException {
            out.writeFloat(updateAmount);
            out.writeLong(nodeId);
            out.writeUTF(nodeName);
        }
        public String toString() {
            return "Nodename: " + nodeName + " with id: " + nodeId + " was updated: " + updateAmount + " times.";
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.addResource("hdfs-site.xml");

        Job job = Job.getInstance(conf, "element with biggest version");

        job.setJarByClass(Task3.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(GetHighestVersionNrNode.class);
        job.setReducerClass(GetHighestVersionNrNode.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(VersionNr.class);

        FileInputFormat.addInputPath(job, new Path("/map.osm"));
        FileOutputFormat.setOutputPath(job, new Path("/xmlhadoop/result/opg3"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
