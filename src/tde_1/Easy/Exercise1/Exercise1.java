package tde_1.Easy.Exercise1;

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

public class Exercise1 {

    public static void main(String[] args) throws Exception {
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
        Job j = new Job(c, "brazilTransactions");

        // Registro de classes
        j.setJarByClass(Exercise1.class);
        j.setMapperClass(MapForExercise1.class);
        j.setReducerClass(ReduceForExercise1.class);
        j.setCombinerClass(CombineForExercise1.class);

        // Definir os tipos
        j.setOutputKeyClass(Text.class);
        j.setMapOutputValueClass(IntWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setMapOutputValueClass(IntWritable.class);

        // Definindo arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // Executando a rotina
        j.waitForCompletion(false);

    }

    public static class MapForExercise1 extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            String linha = value.toString();
            String coluns[] = linha.split(";");

            String keyString = key.toString();
            // Se nãp for a primeira linha
            if (!keyString.equals("0")) {
                IntWritable val = new IntWritable(1);
                con.write(new Text(coluns[0]), val);
            }
        }
    }

    public static class CombineForExercise1 extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context con)
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

    public static class ReduceForExercise1 extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {

            String country = key.toString();
            if (country.equals("Brazil")) {
                // Criando variavel de contagem
                int contagem = 0;
                // Varendo o values
                for (IntWritable v : values) {
                    contagem += v.get();
                }
                // salvando os resultados em disco
                con.write(key, new IntWritable(contagem));
            }
        }
    }

}
