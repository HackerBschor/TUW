import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.*;


public class ChiSquareCalculator {
    final static int NUMBER_REDUCERS = 18;

    final static char KEY_TYPE_DOCUMENT = '!';
    final static char KEY_TYPE_CATEGORY = 'c';
    final static char KEY_TYPE_TERM = 't';
    final static char KEY_TYPE_CATEGORY_X_TERM = 'x';

    /*
     * Parsing a list out of a StringTokenizer
     */
    public static List<String> asList(StringTokenizer itr) {
        List<String> l = new ArrayList<>();

        while (itr.hasMoreTokens())
            l.add(itr.nextToken());

        return l;
    }


    /* ------------------------- JOB 1 ------------------------- */

    /*
     * Counts the total number of documents (JSON-Files), the appearance of every category and terms
     * (performs case folding & stop word filtering on the review text).
     * Also, it counts the appearance of every combination category x terms (> 0)
     *
     * Keys-Value Pairs:
     * * Documents: ! 1
     * * Category:  c_<category> 1
     * * Category&Term:  x_<category>_<term> 1
     * * Term:  t_<term> 1
     */
    public static class CounterMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable ONE = new IntWritable(1);

        private final Set<String> stopWords = new HashSet<>();

        /*
         * parsing the stopwords.txt-file from args[0] -> Set<String> stopwords
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String stopWordsFile = conf.get("stopWordsFile");

            FileSystem fs = FileSystem.get(conf);
            FSDataInputStream reader = fs.open(new Path(stopWordsFile));
            BufferedReader br = new BufferedReader(new InputStreamReader(reader));
            String line = br.readLine();

            while (line != null) {
                stopWords.add(line);
                line = br.readLine();
            }

            super.setup(context);
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            JsonObject jsonObject;

            try {
                jsonObject = new JsonParser().parse(value.toString()).getAsJsonObject();
            } catch (Exception ignored) {
                return;
            }

            // Count total number of documents (Json Object) -> to every node
            context.write(new Text(""+KEY_TYPE_DOCUMENT), ONE);

            String reviewTextJson = jsonObject.get("reviewText").getAsString();
            String categoryJson = jsonObject.get("category").getAsString();

            // Transforms the words in categories (case folding & but NO stop word filtering)
            List<String> categoryTokens = new ArrayList<>();
            StringTokenizer itr = new StringTokenizer(categoryJson, " \t0123456798()[]{}.!?,;:+=-_\"'`~#@&*%€$§\\/");
            while (itr.hasMoreTokens())
                categoryTokens.add(itr.nextToken().toLowerCase());

            String category = String.join("-", categoryTokens);

            // Count documents with the category
            //c_<category> -> 1
            context.write(new Text(KEY_TYPE_CATEGORY+"_"+category), ONE);

            //  Count documents with term & term x category
            itr = new StringTokenizer(reviewTextJson, " \t0123456798()[]{}.!?,;:+=-_\"'`~#@&*%€$§\\/");
            Set<String> set = new HashSet<>();
            while(itr.hasMoreTokens()) {
                String term = itr.nextToken().toLowerCase(); // case folding

                if (stopWords.contains(term)) // stopword filtering
                    continue;

                if (set.contains(term)) // duplicate elimination
                    continue;

                set.add(term);

                //x_<category>_<term> -> 1
                context.write(new Text(KEY_TYPE_CATEGORY_X_TERM+"_"+ category+"_"+term), ONE);
                //t_<term> -> 1
                context.write(new Text(KEY_TYPE_TERM+"_" + term), ONE);
            }

        }
    }


    /*
     * Aggregates the results from the Mapper
     * E.g. for terms: <key-type-term>_<node>_<term>, [1, 1, 1, ..., 1] ->  <key-type-term>_<node>_<term>, 1+1+1+...+1
     *
     * Values:
     * * Documents: ! -> sum(documents)
     * * Category:  c_<category> -> sum(documents wit <category>)
     * * Term:  t_<term> -> sum(documents wit <term>)
     * * Category&Term:  c_<category>_<term> -> sum(documents wit <term> & <category>)
     */
    public static class CountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    /*
     * JOB 2
     */

    /*
     * Copies the values: Documents, Category, Term
     *
     * Remaps all combined values to the corresponding term:
     * * Old: c_<category>_<term> -> sum(documents wit <term> & <category>)
     * * New: t_<term> -> <category>_sum(documents wit <term> & <category>)
     */
    public static class SumCatTermToTermMapper extends Mapper<Object, Text, Text, Text> {
        private final Text keyOut = new Text();
        private final Text valueOut = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            List<String> values = asList(new StringTokenizer(value.toString(), " \t_"));

            switch (values.get(0)) {
                /* Key/ Value Copy */
                case ""+KEY_TYPE_DOCUMENT:
                    //! -> sum(documents)
                    keyOut.set(""+KEY_TYPE_DOCUMENT);
                    valueOut.set(values.get(1));
                    break;
                case ""+KEY_TYPE_CATEGORY:
                    //c_<category> -> sum(documents wit <category>)
                    keyOut.set(KEY_TYPE_CATEGORY + "_" + values.get(1));
                    valueOut.set(values.get(2));
                    break;
                case ""+KEY_TYPE_TERM:
                    //t_<term> -> sum(documents wit <term>)
                    keyOut.set(KEY_TYPE_TERM + "_" + values.get(1));
                    valueOut.set(KEY_TYPE_TERM + "_"+values.get(2));
                    break;

                /* Re-Map */
                case ""+KEY_TYPE_CATEGORY_X_TERM:
                    // Old: c_<category>_<term> -> sum(documents wit <term> & <category>)
                    // New: t_<term> -> c_<category>_sum(documents wit <term> & <category>)
                    keyOut.set(KEY_TYPE_TERM +  "_" +values.get(2));
                    valueOut.set(KEY_TYPE_CATEGORY + "_" + values.get(1) + "_" + values.get(3));
                    break;
            }

            context.write(keyOut, valueOut);
        }
    }

    /*
     * Builds a string (seperated through whitespace) of the sums of documents with <category>X<terms> (Key <term>)
     *
     * Values:
     * * Documents: !               ->      sum(documents)
     * * Category:  c_<category>    ->      sum(documents wit <category>)
     * * Term:      t_<term>        ->  [   t_sum(documents wit <term>) |
     *                                      c_<category_1>_sum(documents wit <term> & <category_1>)
     *                                      c_<category_2>_sum(documents wit <term> & <category_2>)
     *                                      ...
     *                                      c_<category_N>_sum(documents wit <term> & <category_N>) ]
     */
    public static class SumCatTermToTermReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder s = new StringBuilder();
            for (Text val : values) {
                s.append(val).append(" ");
            }
            context.write(key, new Text(s.toString()));
        }
    }


    /*
     * Copies the Key->Value pairs:
     * * Documents: !            ->     sum(documents)
     * * Category:  c_<category> ->     sum(documents wit <category>)
     *
     * Remaps Key->Value pairs:
     * * Term:      t_<term>     ->  [  t_sum(documents wit <term>) |
     *                                  c_<category_1>_sum(documents wit <term> & <category_1>)
     *                                  c_<category_2>_sum(documents wit <term> & <category_2>)
     *                                  ...
     *                                  c_<category_N>_sum(documents wit <term> & <category_N>) ]
     *
     * * TO:
     * * Category:  c_<category_1> ->  <term>_sum(documents wit <term> & <category_1>)_sum(documents wit <term>)
     * * Category:  c_<category_2> ->  <term>_sum(documents wit <term> & <category_2>)_sum(documents wit <term>)
     * * ...
     * * Category:  c_<category_N> ->  <term>_sum(documents wit <term> & <category_N>)_sum(documents wit <term>)
     */
    public static class TermListToCategoryMapper extends Mapper<Object, Text, Text, Text> {
        private final Text keyOut = new Text();
        private final Text valueOut = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            List<String> values = asList(new StringTokenizer(value.toString(), " \t_"));
            /* Key/ Value Copy */
            switch (values.get(0)) {
                case ""+KEY_TYPE_DOCUMENT:
                    //! -> sum(documents) (!!TO ALL NODES !!)
                    for (int i = 0; i < NUMBER_REDUCERS; i++) {
                        keyOut.set(KEY_TYPE_DOCUMENT+"_"+i);
                        valueOut.set(values.get(1));
                        context.write(keyOut, valueOut);
                    }
                    return;
                case ""+KEY_TYPE_CATEGORY:
                    //c_<category> -> sum(documents wit <category>)
                    keyOut.set(KEY_TYPE_CATEGORY + "_" + values.get(1));
                    valueOut.set(KEY_TYPE_CATEGORY+"_"+values.get(2));
                    context.write(keyOut, valueOut);
                    return;
            }

            /* ReMap */

            /*
             * For each key (c_<term>)
             * Distinguish between two values:
             * * (t_sum(documents wit <term>))
             * * (c_<category>_sum(documents wit <term> & <category>))
             * and map them to the category:
             * c_<node>_<category> -> <term>_sum(documents wit <term> & <category>)_sum(documents wit <term>)
             */
            String token = values.get(1);
            String totalDocsWithToken = null;

            List<String> categories = new ArrayList<>();
            List<String> docsWithCategoryAndToken = new ArrayList<>();
            for(int i = 2; i < values.size(); i++) {
                if("c".equals(values.get(i))) {
                    categories.add(values.get(i+1));
                    docsWithCategoryAndToken.add(values.get(i+2));
                    i=i+2;
                }
                else {
                    totalDocsWithToken = values.get(i+1);
                    i++;
                }

            }


            for(int i = 0; i < categories.size(); i++) {
                // New Key/Value Pair:
                // c_<node>_<category> -> <term>_sum(documents wit <term> & <category>)_sum(documents wit <term>)
                keyOut.set(KEY_TYPE_CATEGORY + "_" + categories.get(i));
                valueOut.set("t_"+token+"_"+docsWithCategoryAndToken.get(i)+"_"+totalDocsWithToken);
                context.write(keyOut, valueOut);
            }
        }
    }

    /*
     * Every node has to have access on the total number of documents
     * -> mapper emitted !_0, !_1, ..., !_R with this info (R number of reducers)
     * ChiSquarePartitioner::getPartition ensures that !_0 goes to node 1, !_1 goes to node 2...
     *
     * The categories are distributed via hash
     */
    public static class ChiSquarePartitioner extends Partitioner<Text, Object> {
        @Override
        public int getPartition(Text key, Object value, int i) {
            if(key.toString().startsWith("!"))
                return Integer.parseInt(key.toString().split("_")[1]);
            else
                return Math.abs(key.toString().hashCode() % NUMBER_REDUCERS);
        }
    }

    /*
     * Calculates the final ChiSquare Values
     *
     * Values:
     * * Documents: !_<node>        ->      sum(documents)
     * * Category:  c_<category>    ->  [   c_sum(documents wit <category>) |
     *                                      <term>_sum(documents wit <term> & <category>)_sum(documents wit <term>) ]
     */
    public static class TermListToCategoryReducer extends Reducer<Text, Text, Text, Text> {
        private final Text keyOut = new Text();
        private final Text valueOut = new Text();
        Integer totalNumberDocuments = 0;

        static class TermInfo {
            String term;
            int docsTermCategory;
            int docsTerm;

            protected TermInfo(String term, int docsTermCategory, int docsTerm) {
                this.term = term;
                this.docsTermCategory = docsTermCategory;
                this.docsTerm = docsTerm;
            }
        }

        static class ChiSquareTerms {
            String term;
            BigDecimal chiSquareValue;

            protected ChiSquareTerms(String term, BigDecimal chiSquareValue) {
                this.term = term;
                this.chiSquareValue = chiSquareValue;
            }

            public String toString() {
                return term+":"+chiSquareValue;
            }
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //Reading the total number of documents
            if(key.toString().startsWith("!")) {
                totalNumberDocuments = Integer.parseInt(values.iterator().next().toString());
                return;
            }

            String category = key.toString().split("_")[1];

            /*
             * Reading the total number of documents for the category:
             * * c_sum(documents wit <category>)
             * Or the terms with the total number of documents and the number of documents with the term and the category:
             * * <term>_sum(documents wit <term> & <category>)_sum(documents wit <term>)
             */

            List<TermInfo> termInfos = new ArrayList<>();
            int docsCategory = 0;

            for(Text val: values) {
                String[] valueSplit = val.toString().split("_");
                if(valueSplit[0].equals(""+KEY_TYPE_CATEGORY))
                    docsCategory = Integer.parseInt(valueSplit[1]);
                else
                    termInfos.add(new TermInfo(valueSplit[1], Integer.parseInt(valueSplit[2]), Integer.parseInt(valueSplit[3])));
            }

            //Calculating for every term & category the chi-square values
            List<ChiSquareTerms> chiSquareTerms = new ArrayList<>();
            for(TermInfo termInfo : termInfos) {
                BigInteger a = BigInteger.valueOf(termInfo.docsTermCategory);
                BigInteger b = BigInteger.valueOf(termInfo.docsTerm - termInfo.docsTermCategory);
                BigInteger c = BigInteger.valueOf(docsCategory - termInfo.docsTermCategory);
                BigInteger d = BigInteger.valueOf(totalNumberDocuments - docsCategory - termInfo.docsTerm + termInfo.docsTermCategory);

                BigDecimal quotient = new BigDecimal(((a.multiply(d)).subtract(b.multiply(c))).pow(2));
                BigDecimal dividend = new BigDecimal((a.add(b)).multiply((a.add(c)).multiply((b.add(d)).multiply(c.add(d)))));

                BigDecimal chiSquare = quotient.divide(dividend, 10, RoundingMode.HALF_UP);

                chiSquareTerms.add(new ChiSquareTerms(termInfo.term, chiSquare));
            }

            //Sorting for every category every term with the chi-square value (ref. chiSquareValue DESC)
            chiSquareTerms.sort((c1, c2) -> c2.chiSquareValue.compareTo(c1.chiSquareValue));
            chiSquareTerms = chiSquareTerms.subList(0, 75);

            //Sorting for every category every term with the chi-square value (ref. term ASC)
            chiSquareTerms.sort(Comparator.comparing(c -> c.term));

            StringBuilder valueOutput = new StringBuilder();
            for (ChiSquareTerms chiSquareTerm : chiSquareTerms)
                valueOutput.append(chiSquareTerm.toString()).append(" ");


            keyOut.set(category);
            valueOut.set(valueOutput.toString());
            context.write(keyOut, valueOut);
        }
    }



    public static void main(String[] args) throws Exception {
        String stopWordsFile = args[0];
        String inputFile = args[1];
        String outputFile = args[2];

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        conf.set("stopWordsFile", stopWordsFile);
        Path inputPath = new Path(inputFile);
        Path outputPath = new Path(outputFile + "_0");

        //JOB 1
        Job job_0 = Job.getInstance(conf, "count categories, terms, categories x terms");

        job_0.setNumReduceTasks(NUMBER_REDUCERS);

        job_0.setJarByClass(ChiSquareCalculator.class);
        job_0.setMapperClass(CounterMapper.class);
        job_0.setCombinerClass(CountReducer.class);
        job_0.setReducerClass(CountReducer.class);
        //job_0.setPartitionerClass(ChiSquarePartitioner.class);


        job_0.setOutputKeyClass(Text.class);
        job_0.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job_0, inputPath);
        FileOutputFormat.setOutputPath(job_0, outputPath);

        if ( fs.exists( outputPath ))
            fs.delete( outputPath, true );

        if(!job_0.waitForCompletion(true))
            System.exit(-1);



        //JOB 2
        inputPath = new Path(outputFile + "_0");
        outputPath = new Path(outputFile + "_1");

        Job job_1 = Job.getInstance(conf, "word count 1");

        job_1.setNumReduceTasks(ChiSquareCalculator.NUMBER_REDUCERS);

        job_1.setJarByClass(ChiSquareCalculator.class);
        job_1.setMapperClass(SumCatTermToTermMapper.class);
        job_1.setReducerClass(SumCatTermToTermReducer.class);
        //job_1.setPartitionerClass(ChiSquarePartitioner.class);

        job_1.setOutputKeyClass(Text.class);
        job_1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job_1, inputPath);
        FileOutputFormat.setOutputPath(job_1, outputPath);

        if ( fs.exists( outputPath ))
            fs.delete( outputPath, true );

        if(!job_1.waitForCompletion(true))
            System.exit(-1);

        //JOB 3
        inputPath = new Path(outputFile + "_1");
        outputPath = new Path(outputFile);

        Job job_2 = Job.getInstance(conf, "word count 2");

        job_2.setNumReduceTasks(ChiSquareCalculator.NUMBER_REDUCERS);
        job_2.setJarByClass(ChiSquareCalculator.class);

        job_2.setMapperClass(TermListToCategoryMapper.class);
        job_2.setReducerClass(TermListToCategoryReducer.class);
        job_2.setPartitionerClass(ChiSquarePartitioner.class);

        job_2.setOutputKeyClass(Text.class);
        job_2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job_2, inputPath);
        FileOutputFormat.setOutputPath(job_2, outputPath);

        if ( fs.exists( outputPath ))
            fs.delete( outputPath, true );

        if(!job_2.waitForCompletion(true))
            System.exit(-1);

        System.exit(job_2.waitForCompletion(true) ? 0 : 1);

    }

}
