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

public class CreativePart2 extends Configured implements Tool {

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
                        buffer.write("<nodes>".getBytes());
                        buffer.write(startTag);
                        if (readUntilMatch(endTag, true)) {
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
    }

    public static class CSVReaderMapper extends Mapper<Object, Text, Text, BooleanWritable> {

        private BooleanWritable fromCSV = new BooleanWritable(true);
        private Text distanceMeasured = new Text();
        private String[] lineFromCSV = new String[13];
        private String startLat = new String(), startLon = new String();
        private Double startLatDouble, startLonDouble, gridSize = 1000.0, xDistance, yDistance;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

           lineFromCSV = value.toString().split(",");
           startLat = lineFromCSV[6];
           startLon = lineFromCSV[7];

           if (!Character.isLetter(startLat.charAt(1))){
              startLatDouble = Double.parseDouble(startLat.substring(1, startLat.length()-1));
              startLonDouble = Double.parseDouble(startLon.substring(1, startLon.length()-1));

              xDistance = distVincenty(startLatDouble, 59.898, 10.662, 59.898);
              yDistance = distVincenty(10.662, startLonDouble, 10.662, 59.898);

              xDistance = Math.floor(xDistance/gridSize);
              yDistance = Math.floor(yDistance/gridSize);

              distanceMeasured.set(xDistance+":"+yDistance);
              context.write(distanceMeasured, fromCSV);
           }

        }
    }

    public static class OsloOsmMapper extends Mapper<Object, Text, Text, BooleanWritable> {

        private final BooleanWritable fromCSV = new BooleanWritable(false);
        private Text distanceMeasured = new Text();
        private Double startLatDouble, startLonDouble, gridSize = 1000.0, xDistance, yDistance;

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

                            if(tagElem.getAttribute("k").equals("addr:street")){
                                startLatDouble = Double.parseDouble(nodeElem.getAttribute("lat"));
                                startLonDouble = Double.parseDouble(nodeElem.getAttribute("lon"));

                                xDistance = distVincenty(startLatDouble, 59.898, 10.662, 59.898);
                                yDistance = distVincenty(10.662, startLonDouble, 10.662, 59.898);

                                xDistance = Math.floor(xDistance/gridSize);
                                yDistance = Math.floor(yDistance/gridSize);

                                distanceMeasured.set(xDistance+":"+yDistance);
                                context.write(distanceMeasured, fromCSV);
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

    public static class CountOccurences extends Reducer < Text, BooleanWritable, Text, FloatWritable > {

        private FloatWritable result = new FloatWritable();

        public void reduce(Text key, Iterable < BooleanWritable > values, Context context) throws IOException, InterruptedException {

            float streetsInOsm = 0, stationsInCSV = 0, ratio;

            for (BooleanWritable val : values) {
                if(val.get())
                    stationsInCSV++;
                else
                    streetsInOsm++;
            }
            if(stationsInCSV > 0 && streetsInOsm > 0){
                ratio = stationsInCSV/streetsInOsm;
                result.set(ratio);
                context.write(key, result);
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(getConf(), "creative part 2");
        job.setJarByClass(CreativePart2.class);
        job.setReducerClass(CountOccurences.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BooleanWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        MultipleInputs.addInputPath(job, new Path("/osloMap.osm"), StartEndFileInputFormat.class, OsloOsmMapper.class);
        MultipleInputs.addInputPath(job, new Path("/creativePart/turer.csv"), TextInputFormat.class, CSVReaderMapper.class);

        FileOutputFormat.setOutputPath(job, new Path("/creativePart/result/opg2"));

        return job.waitForCompletion(true) ? 0 : 1;

    }

    public static void main(String[] args) throws Exception {

        Configuration  conf = new Configuration();
        conf.addResource("hdfs-site.xml");

        conf.set("no.healey.startend.startTag", "<node");
        conf.set("no.healey.startend.endTag", "</node>");

        int res = ToolRunner.run(conf, new CreativePart2(), args);

        System.exit(res);
    }

    public static double distVincenty(double lat1, double lon1, double lat2, double lon2) {
       double a = 6378137, b = 6356752.314245, f = 1 / 298.257223563; // WGS-84 ellipsoid params
       double L = Math.toRadians(lon2 - lon1);
       double U1 = Math.atan((1 - f) * Math.tan(Math.toRadians(lat1)));
       double U2 = Math.atan((1 - f) * Math.tan(Math.toRadians(lat2)));
       double sinU1 = Math.sin(U1), cosU1 = Math.cos(U1);
       double sinU2 = Math.sin(U2), cosU2 = Math.cos(U2);

       double sinLambda, cosLambda, sinSigma, cosSigma, sigma, sinAlpha, cosSqAlpha, cos2SigmaM;
       double lambda = L, lambdaP, iterLimit = 100;
       do {
        sinLambda = Math.sin(lambda);
        cosLambda = Math.cos(lambda);
        sinSigma = Math.sqrt((cosU2 * sinLambda) * (cosU2 * sinLambda) +
         (cosU1 * sinU2 - sinU1 * cosU2 * cosLambda) * (cosU1 * sinU2 - sinU1 * cosU2 * cosLambda));
        if (sinSigma == 0)
         return 0; // co-incident points
        cosSigma = sinU1 * sinU2 + cosU1 * cosU2 * cosLambda;
        sigma = Math.atan2(sinSigma, cosSigma);
        sinAlpha = cosU1 * cosU2 * sinLambda / sinSigma;
        cosSqAlpha = 1 - sinAlpha * sinAlpha;
        cos2SigmaM = cosSigma - 2 * sinU1 * sinU2 / cosSqAlpha;
        if (Double.isNaN(cos2SigmaM))
         cos2SigmaM = 0; // equatorial line: cosSqAlpha=0 (§6)
        double C = f / 16 * cosSqAlpha * (4 + f * (4 - 3 * cosSqAlpha));
        lambdaP = lambda;
        lambda = L + (1 - C) * f * sinAlpha *
         (sigma + C * sinSigma * (cos2SigmaM + C * cosSigma * (-1 + 2 * cos2SigmaM * cos2SigmaM)));
       } while (Math.abs(lambda - lambdaP) > 1e-12 && --iterLimit > 0);

       if (iterLimit == 0)
        return Double.NaN; // formula failed to converge

       double uSq = cosSqAlpha * (a * a - b * b) / (b * b);
       double A = 1 + uSq / 16384 * (4096 + uSq * (-768 + uSq * (320 - 175 * uSq)));
       double B = uSq / 1024 * (256 + uSq * (-128 + uSq * (74 - 47 * uSq)));
       double deltaSigma = B *
        sinSigma *
        (cos2SigmaM + B /
         4 *
         (cosSigma * (-1 + 2 * cos2SigmaM * cos2SigmaM) - B / 6 * cos2SigmaM *
          (-3 + 4 * sinSigma * sinSigma) * (-3 + 4 * cos2SigmaM * cos2SigmaM)));
       double dist = b * A * (sigma - deltaSigma);

       return dist;
   }
}
