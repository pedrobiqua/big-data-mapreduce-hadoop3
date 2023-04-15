package tde_1.Hard.Exercise7;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class Exercise7 {
    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {

        DirectoryManage.deleteResultFold();
        DirectoryManage.deleteIntermedieteFold();
        BasicConfigurator.configure();
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("in/transactions_amostra.csv");
        // intermediate
        Path intermediate = new Path("./output/intermediate.tmp");
        // arquivo de saida
        Path output = new Path("output/result");
        // criacao do job e seu nome
        Job j = new Job(c, "maxCommperFlowType");
        // Registrar as classes
        j.setJarByClass(Exercise7.class);
        j.setMapperClass(MapForTransactionsPerFlowAndYear.class);
        j.setReducerClass(ReduceForCombineForMapForTransactionsPerFlowAndYear.class);
        j.setCombinerClass(CombineForMapForTransactionsPerFlowAndYear.class);
        // Definir os tipos de saida
        // MAP
        j.setMapOutputKeyClass(Exercise7KeyWritable.class);
        j.setMapOutputValueClass(FloatWritable.class);
        // REDUCE
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(FloatWritable.class);
        // Definir arquivos de entrada e de saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, intermediate);
        // rodar
        j.waitForCompletion(false);

        Job j2 = new Job(c, "entropia");
        j2.setJarByClass(Exercise7.class);
        j2.setMapperClass(MapEtapaB.class);
        j2.setReducerClass(ReduceEtapaB.class);
        j2.setMapOutputKeyClass(Text.class);
        j2.setMapOutputValueClass(Exercise7ValueWritable.class);
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(j2, intermediate);
        FileOutputFormat.setOutputPath(j2, output);
        j2.waitForCompletion(false);
    }

    public static class MapForTransactionsPerFlowAndYear
            extends Mapper<LongWritable, Text, Exercise7KeyWritable, FloatWritable> {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {

            String linha = value.toString();
            String colunas[] = linha.split(";");

            String keyString = key.toString();

            if (!keyString.equals("0")) {

                String year = colunas[1];

                if (year.equals("2016")) {
                    String commName = colunas[3];
                    String flow = colunas[4];
                    float qtdyComm = Float.parseFloat(colunas[8]);

                    con.write(new Exercise7KeyWritable(flow, commName), new FloatWritable(qtdyComm));
                }
            }
        }
    }

    public static class CombineForMapForTransactionsPerFlowAndYear extends
            Reducer<Exercise7KeyWritable, FloatWritable, Exercise7KeyWritable, FloatWritable> {
        public void reduce(Exercise7KeyWritable key, Iterable<FloatWritable> values, Context con)
                throws IOException, InterruptedException {
            float somaQtds = 0.0f;

            for (FloatWritable o : values) {
                somaQtds += o.get();
            }
            // passando para o reduce valores pre-somados
            con.write(key, new FloatWritable(somaQtds));
        }
    }

    public static class ReduceForCombineForMapForTransactionsPerFlowAndYear
            extends Reducer<Exercise7KeyWritable, FloatWritable, Text, FloatWritable> {
        public void reduce(Exercise7KeyWritable key, Iterable<FloatWritable> values, Context con)
                throws IOException, InterruptedException {
            float somaQtds = 0.0f;
            for (FloatWritable o : values) {
                somaQtds += o.get();
            }
            con.write(new Text(key.getFlow() + "\t" + key.getCommName()), new FloatWritable(somaQtds));
        }
    }

    public static class MapEtapaB extends Mapper<LongWritable, Text, Text, Exercise7ValueWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            // Pegando uma linha
            String linha = value.toString();
            String campos[] = linha.split("\t");
            String flow = campos[0];
            String nameComm = campos[1];

            float sum = Float.parseFloat(campos[2].trim());

            con.write(new Text(flow), new Exercise7ValueWritable(nameComm, flow, sum));

        }
    }

    public static class ReduceEtapaB extends Reducer<Text, Exercise7ValueWritable, Text, FloatWritable> {
        public void reduce(Text key, Iterable<Exercise7ValueWritable> values, Context con)
                throws IOException, InterruptedException {

            float max = 0.0f;
            String name = "";
            String flow = "";
            for (Exercise7ValueWritable o: values) {
                if (o.getQtdValue() > max){
                    max = o.getQtdValue();
                    name = o.getNameComm();
                    flow = o.getFlow();
                }
            }

            // passando para o reduce valores pre-somados
            con.write(new Text(flow + " | " + name + " | "), new FloatWritable(max));
        }
    }
}