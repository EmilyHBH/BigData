import java.util.TreeMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.io.IOException;
import java.io.StringReader;

import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
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

public class Task7 extends Configured implements Tool {

    public static class NodeInputFormat extends FileInputFormat < LongWritable, Text > {

        @Override
        public RecordReader < LongWritable, Text > createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException,InterruptedException {
            NodeReader reader = new NodeReader();
            reader.setCreateParentNode(true);
            reader.initialize(split, context);
            return reader;
        }

    }

    public static class WayInputFormat extends FileInputFormat < LongWritable, Text > {

        @Override
        public RecordReader < LongWritable, Text > createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException,InterruptedException {
            WayReader reader = new WayReader();
            reader.initialize(split, context);
            return reader;
        }

    }

    public static class StartEndRecordReader extends RecordReader < LongWritable, Text > {

        private long start;
        private long pos;
        private long end;
        private boolean createParentNode = false;
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
                        if(createParentNode)
                            buffer.write("<nodes>".getBytes());
                        buffer.write(startTag);
                        if (readUntilMatch(endTag, true)) {
                            if(createParentNode)
                                buffer.write("</nodes>".getBytes());
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
                if(withinBlock) buffer.write(b);

                // check if we're matching:
                if (b == match[i]) {
                    i++;
                    if (i >= match.length) return true;
                } else i = 0;
                // see if we've passed the stop point:
                if (!withinBlock && i == 0 && fsin.getPos() >= end) return false;
            }
        }

        public void setCreateParentNode(boolean createParentNode){
            this.createParentNode = createParentNode;
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

        public void setStartTagAndEndTag(byte[] start, byte[] end){
            startTag = start;
            endTag = end;
        }
    }

    public static class NodeReader extends StartEndRecordReader {
        @Override
        public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
            Configuration job = context.getConfiguration();
            setStartTagAndEndTag(job.get("no.healey.startend.startTagNode").getBytes("utf-8"), job.get("no.healey.startend.endTagNode").getBytes("utf-8"));
            super.initialize(genericSplit, context);
        }
    }

    public static class WayReader extends StartEndRecordReader {
        @Override
        public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
            Configuration job = context.getConfiguration();
            setStartTagAndEndTag(job.get("no.healey.startend.startTagWay").getBytes("utf-8"), job.get("no.healey.startend.endTagWay").getBytes("utf-8"));
            super.initialize(genericSplit, context);
        }
    }

    public static class NodeRefMapper extends Mapper<Object, Text, Text, Text> {

        private Text isFromNodeRefMapper = new Text("NodeIsValid");
        private Text ndRef = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {

                DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
                DocumentBuilder builder = factory.newDocumentBuilder();
                InputSource is = new InputSource(new StringReader(value.toString()));
                Document document = builder.parse(is);
                document.getDocumentElement().normalize();
                Node nodeParent = (Node)document.getDocumentElement();
                Node node = nodeParent.getLastChild();

                if(node.getNodeType() == Node.ELEMENT_NODE){

                    Element nodeElem = (Element)node;
                    NodeList tags = nodeElem.getElementsByTagName("tag");
                    int nrOfTags = tags.getLength();

                    for(int x = 0; x < nrOfTags; x++) {

                        Node tag = tags.item(x);

                        if (tag.getNodeType() == Node.ELEMENT_NODE) {

                            Element tagElem = (Element)tag;

                            if(tagElem.getAttribute("k").equals("traffic_calming") && tagElem.getAttribute("v").equals("hump")){
                                ndRef.set(nodeElem.getAttribute("id"));
                                context.write(ndRef, isFromNodeRefMapper);
                            }
                        }
                    }
                }
            } catch (SAXException exception) {
                // ignore
            } catch (ParserConfigurationException exception) {

            }
        }
    }

    public static class HighwaysMapper extends Mapper<Object, Text, Text, Text> {

        private Text isFromNodeRefMapper = new Text();
        private Text nodeId = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            try {

                DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
                DocumentBuilder builder = factory.newDocumentBuilder();
                InputSource is = new InputSource(new StringReader(value.toString()));
                Document document = builder.parse(is);
                document.getDocumentElement().normalize();
                Element way = document.getDocumentElement();
                NodeList tags = way.getElementsByTagName("tag");
                int nrOfTags = tags.getLength();

                for(int i = 0; i < nrOfTags; i++){

                    Node tag = tags.item(i);

                    if (tag.getNodeType() == Node.ELEMENT_NODE) {

                        Element tagElem = (Element) tag;

                        if(tagElem.getAttribute("k").equals("highway")) {

                            NodeList ndTags = way.getElementsByTagName("nd");

                            int nrNdTags = ndTags.getLength();

                            for(int x = 0; x < nrNdTags; x++) {

                                Node ndNode = ndTags.item(x);

                                if (ndNode.getNodeType() == Node.ELEMENT_NODE) {
                                    Element ndElem = (Element) ndNode;
                                    nodeId.set(ndElem.getAttribute("ref"));
                                    isFromNodeRefMapper.set(way.getAttribute("id"));
                                    context.write(nodeId, isFromNodeRefMapper);
                                }
                            }
                        }
                    }
                }

            } catch (SAXException exception) {
                // ignore
            } catch (ParserConfigurationException exception) {

            }
        }
    }

    public static class CountWaysMapper extends Mapper<Object, Text, IntWritable, NullWritable> {

        private NullWritable nothing;
        private IntWritable outputKey = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            outputKey.set(Integer.parseInt(value.toString()));
            context.write(outputKey, nothing.get());
        }
    }

    public static class WaysThatReferToNode extends Reducer < Text, Text, Text, NullWritable > {

        private NullWritable result;
        private Text wayID = new Text();

        public void reduce(Text key, Iterable < Text > values, Context context) throws IOException, InterruptedException {

            boolean nodeReferredDoesHaveTrafficCalmingHump = false;
            int nrOfHighwaysReferToNode = 0;

            ArrayList<String> wayIDs = new ArrayList<>();

            for (Text val : values) {
                String value = val.toString();
                if(value.equals("NodeIsValid"))
                    nodeReferredDoesHaveTrafficCalmingHump = true;
                else
                    wayIDs.add(value);
            }

            if(nodeReferredDoesHaveTrafficCalmingHump){
                for(int i = 0; i < wayIDs.size(); i++){
                    wayID.set(wayIDs.get(i));
                    context.write(wayID, result.get());
                }
            }
        }
    }

    public static class FinalReducer extends Reducer < IntWritable, NullWritable, IntWritable, IntWritable > {

        private TreeMap<IntWritable, List<IntWritable>> tmap;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            tmap = new TreeMap<IntWritable, List<IntWritable>>();
        }

        public void reduce(IntWritable key, Iterable < NullWritable > values, Context context) throws IOException, InterruptedException {

            int count = 0;

            for (NullWritable val : values)
                count++;

            IntWritable tmapKey = new IntWritable(count), tmapValue = new IntWritable(key.get());
            List<IntWritable> valuesInTmap = tmap.get(tmapKey);

            if (valuesInTmap == null) {
                valuesInTmap  = new ArrayList<>();
                valuesInTmap.add(tmapValue);
                tmap.put(tmapKey, valuesInTmap);
            }
            else
                valuesInTmap.add(tmapValue);
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {

            int allWaysCount = 0;
            for(Map.Entry<IntWritable, List<IntWritable>> entry : tmap.entrySet())
                allWaysCount += entry.getValue().size();

            while (allWaysCount > 15) {
                if(tmap.get(tmap.firstKey()).size() > 1)
                    tmap.get(tmap.firstKey()).remove(0);
                else
                    tmap.remove(tmap.firstKey());

                allWaysCount--;
            }

            for (Map.Entry<IntWritable, List<IntWritable>> entry : tmap.entrySet()) {
                List<IntWritable> list = entry.getValue();
                for(IntWritable value : list)
                    context.write(entry.getKey(), value);
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(getConf(), "Get node id's and way id's of highways that refer to traffic_calming hump");
        job.setJarByClass(Task7.class);
        job.setReducerClass(WaysThatReferToNode.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        MultipleInputs.addInputPath(job, new Path("/map.osm"), WayInputFormat.class, HighwaysMapper.class);
        MultipleInputs.addInputPath(job, new Path("/xmlhadoop/map.osm"), NodeInputFormat.class, NodeRefMapper.class);

        Path subOutputPath = new Path("/xmlhadoop/sub-result/opg7");
        FileSystem fs = FileSystem.get(getConf());
        fs.delete(subOutputPath, true);
        FileOutputFormat.setOutputPath(job, subOutputPath);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.addResource("hdfs-site.xml");

        conf.set("no.healey.startend.startTagNode", "<node");
        conf.set("no.healey.startend.endTagNode", "</node>");

        conf.set("no.healey.startend.startTagWay", "<way");
        conf.set("no.healey.startend.endTagWay", "</way>");

        int res = ToolRunner.run(conf, new Task7(), args);

        if(res == 0){
            Configuration conf2 = new Configuration();
            Job j2 = Job.getInstance(conf2, "get 15 highways that refer to most nodes");
            j2.setJarByClass(Task7.class);
            j2.setInputFormatClass(TextInputFormat.class);
            j2.setMapperClass(CountWaysMapper.class);
            j2.setReducerClass(FinalReducer.class);
            j2.setMapOutputValueClass(NullWritable.class);
            j2.setOutputKeyClass(IntWritable.class);
            j2.setOutputValueClass(IntWritable.class);

            FileInputFormat.addInputPath(j2, new Path("/xmlhadoop/sub-result/opg7"));
            FileOutputFormat.setOutputPath(j2, new Path("/xmlhadoop/result/opg7"));

            System.exit(j2.waitForCompletion(true) ? 0:1);
        }

        System.exit(res);
    }
}
