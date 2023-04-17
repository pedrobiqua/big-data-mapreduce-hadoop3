package tde_1.Easy.Exercise2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

public class Exercise2 {
    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {

        // DirectoryManage.deleteResultFold();
        BasicConfigurator.configure();
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // Metodo estático que apaga a pasta result.
        DirectoryManage.deleteResultFold(files[1]);
        // arquivo de entrada
        Path input = new Path(files[0]);
        // arquivo de saida
        Path output = new Path(files[1]);
        // criacao do job e seu nome
        Job j = new Job(c, "transactionsPerFlowAndYear");
        // Registrar as classes
        j.setJarByClass(Exercise2.class);
        j.setMapperClass(MapForExercise2.class);
        j.setReducerClass(ReduceForExercise2.class);
        j.setCombinerClass(CombineForExercise2.class);
        // Definir os tipos de saida
        // MAP
        j.setMapOutputKeyClass(Exercise2KeyWritable.class);
        j.setMapOutputValueClass(IntWritable.class);
        // REDUCE
        j.setOutputKeyClass(Exercise2KeyWritable.class);
        j.setOutputValueClass(IntWritable.class);
        // Definir arquivos de entrada e de saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);
        // rodar
        j.waitForCompletion(false);
    }

    public static class MapForExercise2
            extends Mapper<LongWritable, Text, Exercise2KeyWritable, IntWritable> {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {

            String linha = value.toString();
            String colunas[] = linha.split(";");

            String keyString = key.toString();
            // Se nãp for a primeira linha
            if (!keyString.equals("0")) {
                String flow = colunas[4];
                String year = colunas[1];
                int yearInt = Integer.parseInt(year);
                con.write(new Exercise2KeyWritable(flow, yearInt), new IntWritable(1));
            }
        }
    }

    public static class CombineForExercise2 extends
            Reducer<Exercise2KeyWritable, IntWritable, Exercise2KeyWritable, IntWritable> {
        public void reduce(Exercise2KeyWritable key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {
            int contagem = 0;
            // Varendo o values
            for (IntWritable v : values) {
                contagem += v.get();
            }
            // salvando os resultados em disco
            con.write(key, new IntWritable(contagem));
        }
    }

    public static class ReduceForExercise2
            extends Reducer<Exercise2KeyWritable, IntWritable, Text, IntWritable> {
        public void reduce(Exercise2KeyWritable key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {
            int contagem = 0;
            // Varendo o values
            for (IntWritable v : values) {
                contagem += v.get();
            }
            String resultFlowKey = key.getFlow();
            int resultYearKey = key.getQtd();
            String result = resultFlowKey + " " + resultYearKey;
            // salvando os resultados em disco
            con.write(new Text(result), new IntWritable(contagem));
        }
    }
}