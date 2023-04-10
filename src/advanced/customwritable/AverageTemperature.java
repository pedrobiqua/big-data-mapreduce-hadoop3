package advanced.customwritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;
import services.DirectoryManage;

import java.io.IOException;

public class AverageTemperature {
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
        Job j = new Job(c, "media");
        // Registrar as classes
        j.setJarByClass(AverageTemperature.class);
        j.setMapperClass(MapForAverage.class);
        j.setReducerClass(ReduceForAverage.class);
        j.setCombinerClass(CombineForAverage.class);
        // Definir os tipos de saida
        // MAP
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(FireAvgTempWritable.class);
        // REDUCE
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(FloatWritable.class);
        // Definir arquivos de entrada e de saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);
        // rodar
        j.waitForCompletion(false);
    }
    public static class MapForAverage extends Mapper<LongWritable, Text, Text, FireAvgTempWritable> {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            // funcao chamada automaticamente por linha do arquivo
            // obtendo a linha
            String linha = value.toString();
            // quebrando em colunas
            String colunas[] = linha.split(",");
            String mes = colunas[2];
            float temperatura = Float.parseFloat(colunas[8]);
            int qtd = 1;
            String chave = "media";
            // enviando dados no formato (chave,valor) para o reduce
            con.write(new Text(chave), new FireAvgTempWritable(temperatura, qtd));
            con.write(new Text(mes), new FireAvgTempWritable(temperatura, qtd));
        }
    }
    public static class CombineForAverage extends Reducer<Text, FireAvgTempWritable, Text, FireAvgTempWritable>{
        public void reduce(Text key, Iterable<FireAvgTempWritable> values, Context con) throws IOException, InterruptedException {
            // somar as temperaturas e as qtds para cada chave
            float somaTemp = 0.0f;
            int somaQtds = 0;
            for(FireAvgTempWritable o : values){
                somaTemp += o.getSomaTemperaturas();
                somaQtds += o.getQtd();
            }
            // passando para o reduce valores pre-somados
            con.write(key, new FireAvgTempWritable(somaTemp, somaQtds));
        }
    }
    public static class ReduceForAverage extends Reducer<Text, FireAvgTempWritable, Text, FloatWritable> {
        public void reduce(Text key, Iterable<FireAvgTempWritable> values, Context con) throws IOException, InterruptedException {
            // logica do reduce:
            // recebe diferentes objetos compostos (temperatura, qtd)
            // somar as temperaturas e somar as qtds
            float somaTemps = 0.0f;
            int somaQtds = 0;
            for (FireAvgTempWritable o : values){
                somaTemps += o.getSomaTemperaturas();
                somaQtds += o.getQtd();
            }
            // calcular a media

            float media = somaTemps / somaQtds;
            // salvando o resultado
            con.write(key, new FloatWritable(media));
        }
    }
}