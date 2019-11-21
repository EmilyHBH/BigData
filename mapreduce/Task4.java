import java.io.*;
import java.util.*;
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
import org.apache.hadoop.mapreduce.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

import javax.xml.parsers.*;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.*;
import org.xml.sax.SAXException;
import org.xml.sax.InputSource;

public class Task4 {

    public static class StartEndFileInputFormat extends FileInputFormat < LongWritable, Text > {

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

    public static class TokenizerMapper extends Mapper < Object, Text, IntWritable, Highway > {

        private TreeMap<IntWritable, List<Highway>> tmap;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            tmap = new TreeMap<IntWritable, List<Highway>>();
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            try {

                DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
                DocumentBuilder builder = factory.newDocumentBuilder();
                InputSource is = new InputSource(new StringReader(value.toString()));
                Document document = builder.parse(is);
                document.getDocumentElement().normalize();
                Element root = document.getDocumentElement();
                NodeList tags = root.getElementsByTagName("tag");

                // Check for all tag k=highway in <way>
                for(int i = 0; i < tags.getLength(); i++){

                    Node tag = tags.item(i);

                    if (tag.getNodeType() == Node.ELEMENT_NODE) {

                        Element tagElem = (Element) tag;

                        if(tagElem.getAttribute("k").equals("highway")) {

                            Node wayNode = tagElem.getParentNode();
                            Element wayElem = (Element)wayNode;
                            NodeList parentNodesNdTags = wayElem.getElementsByTagName("nd");
                            int ndTagsToWay = parentNodesNdTags.getLength();

                            // Populate the highway with correct info/data
                            Highway highway = new Highway(wayElem.getAttribute("id"), tagElem.getAttribute("v"), ndTagsToWay);

                            // Get right place/key to write to tmap (treemap)
                            IntWritable inputKey = new IntWritable(ndTagsToWay);
                            List<Highway> list = tmap.get(inputKey);

                            // If the list doesn't already exist it shall put(), else just add the highway
                            if (list == null) {
                                list  = new ArrayList<>();
                                list.add(highway);
                                tmap.put(inputKey, list);
                            }
                            else
                                list.add(highway);
                        }

                    }

                }

            } catch (SAXException exception) {
                // ignore
            } catch (ParserConfigurationException exception) {

            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {

            // Counts the length of all the lists in the treemap
            int allHighwaysCount = 0;
            for(Map.Entry<IntWritable, List<Highway>> entry : tmap.entrySet())
                allHighwaysCount += entry.getValue().size();

            // removes highways from lists to the point that only 20 highways are stored in the treemap
            while (allHighwaysCount > 20) {
                if(tmap.get(tmap.firstKey()).size() > 1)
                    tmap.get(tmap.firstKey()).remove(0);
                else
                    tmap.remove(tmap.firstKey());

                allHighwaysCount--;
            }

            // Write to reducer the top 20 highways with most nodes
            for (Map.Entry<IntWritable, List<Highway>> entry : tmap.entrySet()) {

                List<Highway> list = entry.getValue();

                for(Highway highwayFromList : list)
                    context.write(entry.getKey(), highwayFromList);
            }
        }
    }

    public static class TwentyHighwaysWithMostNodes extends Reducer < IntWritable, Highway, IntWritable, Highway > {

        private TreeMap<IntWritable, List<Highway>> tmap;
        private IntWritable allHighwaysCountIntWritable = new IntWritable();

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            tmap = new TreeMap<IntWritable, List<Highway>>();
        }

        @Override
        public void reduce(IntWritable key, Iterable<Highway> values, Context context) throws IOException, InterruptedException {

            List<Highway> list = new ArrayList<>();
            IntWritable inputKey = new IntWritable(key.get());

            for(Highway highway : values) {
                Highway h = new Highway(highway.getWayId(), highway.getHighwayType(), highway.getNrNdNodes());
                list.add(h);
            }

            tmap.put(inputKey, list);
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {

            // Counts the length of all the lists in the treemap
            int allHighwaysCount = 0;
            for(Map.Entry<IntWritable, List<Highway>> entry : tmap.entrySet())
                allHighwaysCount += entry.getValue().size();

            // removes highways from lists to the point that only 20 highways are stored in the treemap
            while (allHighwaysCount > 20) {
                if(tmap.get(tmap.firstKey()).size() > 1)
                    tmap.get(tmap.firstKey()).remove(0);
                else
                    tmap.remove(tmap.firstKey());

                allHighwaysCount--;
            }

            // Writes out the tmap (which now is 20 top highways with most nodes)
            for (Map.Entry<IntWritable, List<Highway>> entry : tmap.entrySet()) {

                List<Highway> list = entry.getValue();

                for(Highway highwayFromList : list) {
                    allHighwaysCountIntWritable.set(allHighwaysCount);
                    context.write(allHighwaysCountIntWritable, highwayFromList);
                    allHighwaysCount--;
                }
            }
        }
    }

    public static class Highway implements Writable {
        private String wayId, highwayType;
        private int nrNdNodes;

        public Highway(String wayId, String highwayType, int nrNdNodes){
            this.wayId = wayId;
            this.highwayType = highwayType;
            this.nrNdNodes = nrNdNodes;
        }
        public Highway(){
            this.wayId = "";
            this.highwayType = "";
            this.nrNdNodes = 0;
        }

        public int getNrNdNodes() {
            return nrNdNodes;
        }
        public String getWayId() {
            return wayId;
        }
        public String getHighwayType() {
            return highwayType;
        }
        public void readFields(DataInput in) throws IOException {
            nrNdNodes = in.readInt();
            wayId = in.readUTF();
            highwayType = in.readUTF();
        }
        public void write(DataOutput out) throws IOException {
            out.writeInt(nrNdNodes);
            out.writeUTF(wayId);
            out.writeUTF(highwayType);
        }
        public String toString() {
            return "Way ID:\t\t" + wayId + "\n\tIs of type:\t" + highwayType + "\n\tNr of Nd Nodes:\t" + nrNdNodes;
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.addResource("hdfs-site.xml");

        conf.set("no.healey.startend.startTag", "<way");
        conf.set("no.healey.startend.endTag", "</way>");

        Job job = Job.getInstance(conf, "20 highways with most nodes");

        job.setJarByClass(Task4.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(TwentyHighwaysWithMostNodes.class);
        job.setInputFormatClass(StartEndFileInputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Highway.class);

        FileInputFormat.addInputPath(job, new Path("/map.osm"));
        FileOutputFormat.setOutputPath(job, new Path("/xmlhadoop/result/opg4"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
