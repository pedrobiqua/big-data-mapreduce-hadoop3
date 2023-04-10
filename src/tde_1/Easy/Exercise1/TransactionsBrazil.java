package tde_1.Easy.Exercise1;

import org.apache.commons.io.FileUtils;
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
import java.io.*;
import java.io.IOException;


public class TransactionsBrazil {

    public static void main(String[] args) throws Exception {
        // /home/pedro/Downloads/big-data-mapreduce-hadoop3-student-master/output/result
        // Para não precisar ficar deletando o diretório em cada teste fiz essa pequena lógica
        File diretorio = new File("../big-data-mapreduce-hadoop3-student-master/output/result");
        if(diretorio.exists()){
            FileUtils.deleteDirectory(diretorio);
        }

        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "brazilTransactions");

        // Registro de classes
        j.setJarByClass(TransactionsBrazil.class); // Classe que contem o metodo MAIN
        j.setMapperClass(MapForTransactionsBrazil.class); // Classe que contem o MAP
        j.setReducerClass(ReduceForTransactionsBrazil.class); // Classe que contem o metodo REDUCE

        // j.setCombinerClass(ReduceForWordCount.class);

        // Definir os tipos
        j.setOutputKeyClass(Text.class); // Tipo da chave de saida do MAP
        j.setMapOutputValueClass(IntWritable.class); // Tipo do valor de saida do MAP
        j.setOutputKeyClass(Text.class); // Tipo da chave de saida do REDUCE
        j.setMapOutputValueClass(IntWritable.class); // Tipo do valor de saida do REDUCE

        // Definindo arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // Executando a rotina
        j.waitForCompletion(false);

    }

    public static class MapForTransactionsBrazil extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            // Coverte para string
            String linha = value.toString();
            // Quebrando a linha em palavras
            String coluns[] = linha.split(";");

            // O if serve para não pegar a primeira linha
            if (!(coluns[0].equals("country_or_area"))){
                IntWritable val = new IntWritable(1);
                con.write(new Text(coluns[0]), val);
            }
        }
    }

    public static class ReduceForTransactionsBrazil extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {

                String country = key.toString();
                if (country.equals("Brazil")) {
                    // Criando variavel de contagem
                    int contagem = 0;
                    // Varendo o values
                    for (IntWritable v: values) {
                        contagem+= v.get();
                    }
                    // salvando os resultados em disco
                    con.write(key, new IntWritable(contagem));
                }
        }
    }

}
