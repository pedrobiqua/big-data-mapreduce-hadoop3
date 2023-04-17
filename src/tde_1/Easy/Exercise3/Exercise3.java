package tde_1.Easy.Exercise3;

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

public class Exercise3 {
    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
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
        Job j = new Job(c, "TransactionsAveragePerYear");
        // Registrar as classes
        j.setJarByClass(Exercise3.class);
        j.setMapperClass(MapForExercise3.class);
        j.setReducerClass(ReduceForExercise3.class);
        j.setCombinerClass(CombineForExercise3.class);
        // Definir os tipos de saida
        // MAP
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(Exercise3ValueWritable.class);
        // REDUCE
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(FloatWritable.class);
        // Definir arquivos de entrada e de saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);
        // rodar
        j.waitForCompletion(false);
    }

    public static class MapForExercise3 extends Mapper<LongWritable, Text, Text, Exercise3ValueWritable> {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {

            String linha = value.toString();
            String colunas[] = linha.split(";");

            String keyString = key.toString();
            // Se não for a primeira linha
            if (!keyString.equals("0")) {
                float commValue = Float.parseFloat(colunas[5]);
                String year = colunas[1];
                int yearInt = Integer.parseInt(year);
                con.write(new Text("media"), new Exercise3ValueWritable(commValue, 1));
                con.write(new Text(year), new Exercise3ValueWritable(commValue, 1));
            }
        }
    }

    public static class CombineForExercise3 extends Reducer<Text, Exercise3ValueWritable, Text, Exercise3ValueWritable> {
        public void reduce(Text key, Iterable<Exercise3ValueWritable> values, Context con) throws IOException, InterruptedException {
            float somaComm = 0.0f;
            int somaQtds = 0;
            for (Exercise3ValueWritable o : values) {
                somaComm += o.getcommValue();
                somaQtds += o.getQtd();
            }
            // passando para o reduce valores pre-somados
            con.write(key, new Exercise3ValueWritable(somaComm, somaQtds));
        }
    }

    public static class ReduceForExercise3 extends Reducer<Text, Exercise3ValueWritable, Text, FloatWritable> {
        public void reduce(Text key, Iterable<Exercise3ValueWritable> values, Context con) throws IOException, InterruptedException {
            float somaComm = 0.0f;
            int somaQtds = 0;
            for (Exercise3ValueWritable o : values) {
                somaComm += o.getcommValue();
                somaQtds += o.getQtd();
            }

            // calcular a media
            float media = somaComm / somaQtds;
            // salvando o resultado
            con.write(key, new FloatWritable(media));
        }
    }
}