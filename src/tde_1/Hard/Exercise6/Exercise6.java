package tde_1.Hard.Exercise6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;
import services.DirectoryManage;

import java.io.IOException;

public class Exercise6 {
    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {

        DirectoryManage.deleteResultFold();
        BasicConfigurator.configure();
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);
        // arquivo de saida
        Path output = new Path(files[1]);
        // criacao do job e seu nome
        Job j = new Job(c, "countryLargest");
        // Registrar as classes
        j.setJarByClass(Exercise6.class);
        j.setMapperClass(MapForTransactionsPerFlowAndYear.class);
        j.setReducerClass(ReduceForCombineForMapForTransactionsPerFlowAndYear.class);
        j.setCombinerClass(CombineForMapForTransactionsPerFlowAndYear.class);
        // Definir os tipos de saida
        // MAP
        j.setMapOutputKeyClass(Exercise6KeyWritable.class);
        j.setMapOutputValueClass(Exercise6ValueWritable.class);
        // REDUCE
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(Text.class);
        // Definir arquivos de entrada e de saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);
        // rodar
        j.waitForCompletion(false);
    }
    public static class MapForTransactionsPerFlowAndYear extends Mapper<LongWritable, Text,
            Exercise6KeyWritable, Exercise6ValueWritable> {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {

            String linha = value.toString();
            String colunas[] = linha.split(";");
            String keyString = key.toString();
        }
    }
    public static class CombineForMapForTransactionsPerFlowAndYear extends Reducer<Exercise6KeyWritable, Exercise6ValueWritable,
            Exercise6KeyWritable, Exercise6ValueWritable>{
        public void reduce(Exercise6KeyWritable key, Iterable<Exercise6ValueWritable> values, Context con)
                throws IOException, InterruptedException {

        }
    }
    public static class ReduceForCombineForMapForTransactionsPerFlowAndYear extends Reducer<Exercise6KeyWritable, Exercise6ValueWritable,
            Text, Text> {
        public void reduce(Exercise6KeyWritable key, Iterable<Exercise6ValueWritable> values, Context con)
                throws IOException, InterruptedException {

        }
    }
}