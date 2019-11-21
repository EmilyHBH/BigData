import java.io.IOException;
import java.util.ArrayList;
import java.io.StringReader;

import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.*;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

import javax.xml.parsers.*;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.*;
import org.xml.sax.SAXException;
import org.xml.sax.InputSource;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;

public class Task8 extends Configured implements Tool {

    public static class WayInputFormat extends FileInputFormat < LongWritable, Text > {

        @Override
        public RecordReader < LongWritable, Text > createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException,InterruptedException {

            StartEndRecordReader reader = new StartEndRecordReader();
            reader.initialize(split, context);

            return reader;
        }

    }

    public static class StartEndRecordReader extends RecordReader < LongWritable, Text > {

        private long start;
        private long pos;
        private long end;
        private FSDataInputStream fsin;
        private byte[] startTag;
        private byte[] endTag;
        private LongWritable key = new LongWritable();
        private Text value = new Text();
        private final DataOutputBuffer buffer = new DataOutputBuffer();

        @Override
        public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
            FileSplit split = (FileSplit) genericSplit;


            Configuration job = context.getConfiguration();
            this.startTag = job.get("no.healey.startend.startTag").getBytes("utf-8");
            this.endTag = job.get("no.healey.startend.endTag").getBytes("utf-8");


            start = split.getStart();
            end = start + split.getLength();

            final Path file = split.getPath();
            FileSystem fs = file.getFileSystem(job);
            this.fsin = fs.open(split.getPath());
            fsin.seek(start);
        }

        @Override
        public boolean nextKeyValue() throws IOException {
            if (fsin.getPos() < end) {
                if (readUntilMatch(startTag, false)) {
                    try {
                        buffer.write(startTag);
                        if (readUntilMatch(endTag, true)) {
                            key.set(fsin.getPos());
                            value.set(buffer.getData(), 0, buffer.getLength());
                            return true;
                        }
                    } finally {
                        buffer.reset();
                    }
                }
            }
            return false;
        }

        private boolean readUntilMatch(byte[] match, boolean withinBlock) throws IOException {
            int i = 0;
            while (true) {
                int b = fsin.read();
                // end of file:
                if (b == -1) return false;
                // save to buffer:
                if (withinBlock) buffer.write(b);

                // check if we're matching:
                if (b == match[i]) {
                    i++;
                    if (i >= match.length) return true;
                } else i = 0;
                // see if we've passed the stop point:
                if (!withinBlock && i == 0 && fsin.getPos() >= end) return false;
            }
        }

        @Override
        public LongWritable getCurrentKey() throws IOException,
                InterruptedException {
            return key;
        }

        @Override
        public Text getCurrentValue() throws IOException,
                InterruptedException {
            return value;
        }

        @Override
        public float getProgress() throws IOException,
                InterruptedException {
            if (start == end) {
                return 0.0f;
            } else {
                return Math.min(1.0f, (pos - start) / (float)(end - start));
            }
        }

        @Override
        public void close() throws IOException {
            if (fsin != null) {
                fsin.close();
            }
        }
    }

    public static class NodeLatMapper extends Mapper < Object, Text, Text, Text > {

        private Text nodeId = new Text(), nodeLat = new Text();
        private String stringValue;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            stringValue = value.toString();

            if(stringValue.substring(2,6).equals("node")){

                int latOffset = stringValue.indexOf("lat=\"") + 5;

                nodeLat.set("node:" + stringValue.substring(latOffset, stringValue.indexOf("\" ", latOffset)));
                nodeId.set(stringValue.substring(11, stringValue.indexOf("\" ", 12)));

                context.write(nodeId, nodeLat);

            }
        }
    }

    public static class BuildingsMapper extends Mapper < Object, Text, Text, Text > {

        private Text wayId = new Text(), nodeRef = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            try {
                DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
                DocumentBuilder builder = factory.newDocumentBuilder();
                InputSource is = new InputSource(new StringReader(value.toString()));
                Document document = builder.parse(is);
                document.getDocumentElement().normalize();
                Element way = document.getDocumentElement();

                NodeList tags = way.getElementsByTagName("tag");
                int tagsLength = tags.getLength();
                boolean isBuilding = false;

                for(int i = 0; i < tagsLength; i++){

                    Node tag = tags.item(i);

                    if (tag.getNodeType() == Node.ELEMENT_NODE) {

                        Element tagElem = (Element) tag;

                        if(tagElem.getAttribute("k").equals("building")){
                            isBuilding = true;
                            break;
                        }
                    }
                }

                if(isBuilding){
                    wayId.set("way:" + way.getAttribute("id"));
                    NodeList nds = way.getElementsByTagName("nd");
                    int ndsLength = nds.getLength();

                    for(int i = 0; i < ndsLength; i++){

                        Node nd = nds.item(i);

                        if (nd.getNodeType() == Node.ELEMENT_NODE) {

                            Element ndElem = (Element) nd;

                            nodeRef.set(ndElem.getAttribute("ref"));

                            context.write(nodeRef, wayId);
                        }
                    }
                }

            } catch (SAXException exception) {
                // ignore
            } catch (ParserConfigurationException exception) {

            }
        }
    }

    public static class WayIdNodeRefReducer extends Reducer < Text, Text, Text, Text > {

        private Text result = new Text(), outputKey = new Text();

        public void reduce(Text key, Iterable < Text > values, Context context) throws IOException, InterruptedException {

            ArrayList<String> wayIDs = new ArrayList<String>();
            String nodeLat = "";

            for(Text val : values){

                String[] valuesSplit = val.toString().split(":");

                if(valuesSplit[0].equals("node"))
                    nodeLat = valuesSplit[1];
                else
                    wayIDs.add(valuesSplit[1]);
            }

            if(!nodeLat.isEmpty()){
                result.set(nodeLat);
                for(String wayID : wayIDs){
                    outputKey.set(wayID);
                    context.write(outputKey, result);
                }
            }
        }
    }

    public static class WayMapper extends Mapper < Object, Text, Text, FloatWritable > {

        private Text wayID = new Text();
        private FloatWritable nodeLat = new FloatWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] val = value.toString().split("\t");
            wayID.set(val[0]);
            nodeLat.set(Float.parseFloat(val[1]));
            context.write(wayID, nodeLat);

        }
    }

    public static class FinalReducer extends Reducer < Text, FloatWritable, Text, FloatWritable > {

        private FloatWritable result = new FloatWritable(0);
        private Text wayID = new Text();

        public void reduce(Text key, Iterable < FloatWritable > values, Context context) throws IOException, InterruptedException {

            float latMax = 0, latMin = Float.MAX_VALUE, temp;

            for(FloatWritable val : values){

                temp = val.get();
                if(latMax < temp) latMax = temp;
                else if(latMin > temp) latMin = temp;

            }

            float diff = latMax - latMin;

            if(diff > result.get()){
                result.set(diff);
                wayID.set(key);
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            context.write(wayID, result);
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(getConf(), "Get Way Nd Latitudes");

        job.setJarByClass(Task8.class);
        job.setReducerClass(WayIdNodeRefReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path("/map.osm"), WayInputFormat.class, BuildingsMapper.class);
        MultipleInputs.addInputPath(job, new Path("/xmlhadoop/map.osm"), TextInputFormat.class, NodeLatMapper.class);

        Path subOutputPath = new Path("/xmlhadoop/sub-result/opg8");
        FileSystem fs = FileSystem.get(getConf());
        fs.delete(subOutputPath, true);
        FileOutputFormat.setOutputPath(job, subOutputPath);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {

        Configuration  conf = new Configuration();
        conf.addResource("hdfs-site.xml");

        conf.set("no.healey.startend.startTag", "<way");
        conf.set("no.healey.startend.endTag", "</way>");

        int resultFromJob1 = ToolRunner.run(conf, new Task8(), args);

        if(resultFromJob1 == 0){

            Configuration conf2 = new Configuration();
            Job j2 = Job.getInstance(conf2);
            j2.setJarByClass(Task8.class);
            j2.setInputFormatClass(TextInputFormat.class);
            j2.setMapperClass(WayMapper.class);
            j2.setReducerClass(FinalReducer.class);
            j2.setOutputKeyClass(Text.class);
            j2.setOutputValueClass(FloatWritable.class);

            FileInputFormat.addInputPath(j2, new Path("/xmlhadoop/sub-result/opg8"));
            FileOutputFormat.setOutputPath(j2, new Path("/xmlhadoop/result/opg8"));

            System.exit(j2.waitForCompletion(true) ? 0:1);

        }
    }
}
