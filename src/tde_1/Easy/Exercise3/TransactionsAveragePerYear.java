package tde_1.Easy.Exercise3;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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

import java.io.File;
import java.io.IOException;

public class TransactionsAveragePerYear {
    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        // Para não precisar ficar deletando o diret처rio em cada teste fiz essa pequena l처gica
        File diretorio = new File("../big-data-mapreduce-hadoop3-student-master/output/result");
        if (diretorio.exists()) {
            FileUtils.deleteDirectory(diretorio);
        }
        BasicConfigurator.configure();
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("in/transactions_amostra.csv");
        // arquivo de saida
        Path output = new Path("output/result");
        // criacao do job e seu nome
        Job j = new Job(c, "TransactionsAveragePerYear");
        // Registrar as classes
        j.setJarByClass(TransactionsAveragePerYear.class);
        j.setMapperClass(MapForTransactionsAveragePerYear.class);
        j.setReducerClass(ReduceForCombineForMapForTransactionsAveragePerYear.class);
        j.setCombinerClass(CombineForMapForTransactionsAveragePerYear.class);
        // Definir os tipos de saida
        // MAP
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(TransactionsAveragePerYearWritable.class);
        // REDUCE
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(FloatWritable.class);
        // Definir arquivos de entrada e de saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);
        // rodar
        j.waitForCompletion(false);
    }

    public static class MapForTransactionsAveragePerYear extends Mapper<LongWritable, Text, Text, TransactionsAveragePerYearWritable> {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {

            String linha = value.toString();
            String colunas[] = linha.split(";");

            String keyString = key.toString();
            // Se n찾p for a primeira linha
            if (!keyString.equals("0")) {
                float commValue = Float.parseFloat(colunas[5]);
                String year = colunas[1];
                int yearInt = Integer.parseInt(year);
                con.write(new Text("media"), new TransactionsAveragePerYearWritable(commValue, 1));
                con.write(new Text(year), new TransactionsAveragePerYearWritable(commValue, 1));
            }
        }
    }

    public static class CombineForMapForTransactionsAveragePerYear extends Reducer<Text, TransactionsAveragePerYearWritable, Text, TransactionsAveragePerYearWritable> {
        public void reduce(Text key, Iterable<TransactionsAveragePerYearWritable> values, Context con) throws IOException, InterruptedException {
            float somaComm = 0.0f;
            int somaQtds = 0;
            for (TransactionsAveragePerYearWritable o : values) {
                somaComm += o.getcommValue();
                somaQtds += o.getQtd();
            }
            // passando para o reduce valores pre-somados
            con.write(key, new TransactionsAveragePerYearWritable(somaComm, somaQtds));
        }
    }

    public static class ReduceForCombineForMapForTransactionsAveragePerYear extends Reducer<Text, TransactionsAveragePerYearWritable, Text, FloatWritable> {
        public void reduce(Text key, Iterable<TransactionsAveragePerYearWritable> values, Context con) throws IOException, InterruptedException {
            float somaComm = 0.0f;
            int somaQtds = 0;
            for (TransactionsAveragePerYearWritable o : values) {
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