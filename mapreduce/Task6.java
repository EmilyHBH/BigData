import java.io.IOException;
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

public class Task6 extends Configured implements Tool {

    public static class NodeRefInputFormat extends FileInputFormat < LongWritable, Text > {

        @Override
        public RecordReader < LongWritable, Text > createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException,InterruptedException {
            NodeRefReader reader = new NodeRefReader();
            reader.setCreateParentNode(true);
            reader.initialize(split, context);
            return reader;
        }

    }

    public static class HighwayInputFormat extends FileInputFormat < LongWritable, Text > {

        @Override
        public RecordReader < LongWritable, Text > createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException,InterruptedException {
            HighwayReader reader = new HighwayReader();
            reader.initialize(split, context);
            return reader;
        }

    }

    public static class StartEndRecordReader extends RecordReader < LongWritable, Text > {

        private long start;
        private long pos;
        private long end;
        private FSDataInputStream fsin;
        private boolean createParentNode = false;
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

        public void setStartTagAndEndTag(byte[] start, byte[] end){
            startTag = start;
            endTag = end;
        }

        public void setCreateParentNode(boolean createParentNode){
            this.createParentNode = createParentNode;
        }
    }

    public static class NodeRefReader extends StartEndRecordReader {
        @Override
        public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
            Configuration job = context.getConfiguration();
            setStartTagAndEndTag(job.get("no.healey.startend.startTagNodeRef").getBytes("utf-8"), job.get("no.healey.startend.endTagNodeRef").getBytes("utf-8"));
            super.initialize(genericSplit, context);
        }
    }

    public static class HighwayReader extends StartEndRecordReader {
        @Override
        public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
            Configuration job = context.getConfiguration();
            setStartTagAndEndTag(job.get("no.healey.startend.startTagHighway").getBytes("utf-8"), job.get("no.healey.startend.endTagHighway").getBytes("utf-8"));
            super.initialize(genericSplit, context);
        }
    }

    public static class NodeRefMapper extends Mapper<Object, Text, Text, BooleanWritable> {

        private final BooleanWritable isFromNodeRefMapper = new BooleanWritable(true);
        private Text elementId = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {

                DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
                DocumentBuilder builder = factory.newDocumentBuilder();
                InputSource is = new InputSource(new StringReader(value.toString()));
                Document document = builder.parse(is);
                document.getDocumentElement().normalize();
                Node nodeParent = (Node)document.getDocumentElement();
                Node node = nodeParent.getLastChild();

                if (node.getNodeType() == Node.ELEMENT_NODE) {

                    Element nodeElem = (Element) node;

                    NodeList tags = nodeElem.getElementsByTagName("tag");

                    int nrOfTags = tags.getLength();

                    for(int x = 0; x < nrOfTags; x++) {

                        Node tag = tags.item(x);

                        if (tag.getNodeType() == Node.ELEMENT_NODE) {

                            Element tagElem = (Element) tag;

                            if(tagElem.getAttribute("k").equals("barrier") && tagElem.getAttribute("v").equals("lift_gate")){
                                elementId.set(nodeElem.getAttribute("id"));
                                context.write(elementId, isFromNodeRefMapper);
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

    public static class HighwayWithSpecificValuesMapper extends Mapper<Object, Text, Text, BooleanWritable> {

        private final BooleanWritable isFromNodeRefMapper = new BooleanWritable(false);
        private Text nodeRef = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            try {

                DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
                DocumentBuilder builder = factory.newDocumentBuilder();
                InputSource is = new InputSource(new StringReader(value.toString()));
                Document document = builder.parse(is);
                document.getDocumentElement().normalize();
                Element root = document.getDocumentElement();
                NodeList tags = root.getElementsByTagName("tag");
                int nrOfTags = tags.getLength();

                for(int i = 0; i < nrOfTags; i++){

                    Node tag = tags.item(i);

                    if (tag.getNodeType() == Node.ELEMENT_NODE) {

                        Element tagElem = (Element) tag;

                        if(tagElem.getAttribute("k").equals("highway")) {

                            String k_val = tagElem.getAttribute("v");

                            if(k_val.equals("path") || k_val.equals("service") || k_val.equals("road") || k_val.equals("unclassified")) {

                                Element parentElement = (Element)tag.getParentNode();
                                NodeList ndTags = parentElement.getElementsByTagName("nd");

                                int nrNdTags = ndTags.getLength();

                                for(int x = 0; x < nrNdTags; x++) {

                                    Node ndNode = ndTags.item(x);

                                    if (ndNode.getNodeType() == Node.ELEMENT_NODE) {
                                        Element ndElem = (Element) ndNode;
                                        nodeRef.set(ndElem.getAttribute("ref"));
                                        context.write(nodeRef, isFromNodeRefMapper);
                                    }
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

    public static class CountOccurences extends Reducer < Text, BooleanWritable, Text, IntWritable > {

        private int totalOccurencesOfHighwaysThatReferToNode;
        private final Text outputText = new Text("Total Nr of Highways that refer to Node with barrier lift_gate");
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable < BooleanWritable > values, Context context) throws IOException, InterruptedException {

            boolean nodeReferredDoesHaveBarrierLiftGate = false;
            int nrOfHighwaysReferToNode = 0;

            for (BooleanWritable val : values) {
                if(val.get())
                    nodeReferredDoesHaveBarrierLiftGate = true;
                else
                    nrOfHighwaysReferToNode++;
            }

            if(nodeReferredDoesHaveBarrierLiftGate && nrOfHighwaysReferToNode > 0)
                totalOccurencesOfHighwaysThatReferToNode += nrOfHighwaysReferToNode;

        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            result.set(totalOccurencesOfHighwaysThatReferToNode);
            context.write(outputText, result);
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(getConf(), "types of highway with barrier=lift_gate");
        job.setJarByClass(Task6.class);
        job.setReducerClass(CountOccurences.class);
        job.setMapOutputValueClass(BooleanWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        MultipleInputs.addInputPath(job, new Path("/map.osm"), HighwayInputFormat.class, HighwayWithSpecificValuesMapper.class);
        MultipleInputs.addInputPath(job, new Path("/xmlhadoop/map.osm"), NodeRefInputFormat.class, NodeRefMapper.class);

        FileOutputFormat.setOutputPath(job, new Path("/xmlhadoop/result/opg6"));

        return job.waitForCompletion(true) ? 0 : 1;

    }

    public static void main(String[] args) throws Exception {

        Configuration  conf = new Configuration();
        conf.addResource("hdfs-site.xml");

        conf.set("no.healey.startend.startTagHighway", "<way");
        conf.set("no.healey.startend.endTagHighway", "</way>");

        conf.set("no.healey.startend.startTagNodeRef", "<node");
        conf.set("no.healey.startend.endTagNodeRef", "</node>");

        int res = ToolRunner.run(conf, new Task6(), args);

        System.exit(res);
    }
}
