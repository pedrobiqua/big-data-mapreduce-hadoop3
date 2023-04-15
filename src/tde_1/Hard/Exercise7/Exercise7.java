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

public class Exercise7 {
    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {

        DirectoryManage.deleteResultFold();
        BasicConfigurator.configure();
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("in/transactions_amostra.csv");
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
        FileOutputFormat.setOutputPath(j, output);
        // rodar
        j.waitForCompletion(false);
    }

    public static class MapForTransactionsPerFlowAndYear
            extends Mapper<LongWritable, Text, Exercise7KeyWritable, FloatWritable> {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {

            String linha = value.toString();
            String colunas[] = linha.split(";");

            String keyString = key.toString();

            if (keyString.equals("0")) {

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

            con.write(new Text(key.getFlow() + " " + key.getCommName()), new FloatWritable(somaQtds));
        }
    }
}