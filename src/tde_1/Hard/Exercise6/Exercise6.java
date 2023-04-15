package tde_1.Hard.Exercise6;

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
import tde_1.Medium.Exercise5.MaxMinMeanTransactionValueWritable;

import java.io.IOException;

public class Exercise6 {
    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {

        // TODO: Fazer o segundo MapReduce Lembre: Usar o FASTA como exemplo.
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
        Job j = new Job(c, "countryLargest");
        // Registrar as classes
        j.setJarByClass(Exercise6.class);
        j.setMapperClass(MapForTransactionsPerFlowAndYear.class);
        j.setReducerClass(ReduceForCombineForMapForTransactionsPerFlowAndYear.class);
        j.setCombinerClass(CombineForMapForTransactionsPerFlowAndYear.class);
        // Definir os tipos de saida
        // MAP
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(Exercise6ValueWritable.class);
        // REDUCE
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(Exercise6ValueWritable.class);
        // Definir arquivos de entrada e de saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, intermediate);
        // rodar
        j.waitForCompletion(false);

        Job j2 = new Job(c, "entropia");
        j2.setJarByClass(Exercise6.class);
        j2.setMapperClass(MapEtapaB.class);
        j2.setReducerClass(ReduceEtapaB.class);
        j2.setMapOutputKeyClass(Text.class);
        j2.setMapOutputValueClass(Exercise6EtapaBValueWritable.class);
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(j2, intermediate);
        FileOutputFormat.setOutputPath(j2, output);
        j2.waitForCompletion(false);
    }

    public static class MapForTransactionsPerFlowAndYear
            extends Mapper<LongWritable, Text, Text, Exercise6ValueWritable> {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {

            String linha = value.toString();
            String colunas[] = linha.split(";");
            String keyLine = key.toString();

            if (!keyLine.equals("0")) {
                // Obtendo os valores do csv/base de dados
                String country = colunas[0];
                String flow = colunas[4];
                float priceComm = Float.parseFloat(colunas[5]);

                // Fazendo o map
                if (flow.equals("Export")) {
                    int qtd = 1;
                    con.write(new Text(country), new Exercise6ValueWritable(priceComm, qtd));
                }
            }
        }
    }

    public static class CombineForMapForTransactionsPerFlowAndYear
            extends Reducer<Text, Exercise6ValueWritable, Text, Exercise6ValueWritable> {
        public void reduce(Text key, Iterable<Exercise6ValueWritable> values, Context con)
                throws IOException, InterruptedException {
            float max = 0.0f;
            float somaPrice = 0;
            int somaQtds = 0;
            for (Exercise6ValueWritable o : values) {
                somaPrice += o.getPriceComm();
                somaQtds += o.getQtd();
                // Procura o valor maximo
                if (o.getPriceComm() > max) {
                    max = o.getPriceComm();
                }
            }
            // passando para o reduce valores pre-somados
            con.write(key, new Exercise6ValueWritable(somaPrice, somaQtds, max));
        }
    }

    public static class ReduceForCombineForMapForTransactionsPerFlowAndYear
            extends Reducer<Text, Exercise6ValueWritable, Text, FloatWritable> {
        public void reduce(Text key, Iterable<Exercise6ValueWritable> values, Context con)
                throws IOException, InterruptedException {

            String country = "";
            float max = 0.0f;
            float somaPrice = 0.0f;
            int somaQtds = 0;
            float avarage = 0.0f;

            for (Exercise6ValueWritable o : values) {
                somaPrice += o.getPriceComm();
                somaQtds += o.getQtd();
                // Procura o valor maximo
                if (o.getPriceComm() > max) {
                    max = o.getPriceComm();
                    country = key.toString();
                }
            }
            avarage = somaPrice / somaQtds;
            con.write(new Text(country), new FloatWritable(avarage));
        }
    }

    public static class MapEtapaB extends Mapper<LongWritable, Text, Text, Exercise6EtapaBValueWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            // Pegando uma linha
            String linha = value.toString();
            String campos[] = linha.split("\t");
            String country = campos[0];
            float average = Float.parseFloat(campos[1]);

            Text chaveGenerica = new Text("maximo");
            con.write(chaveGenerica, new Exercise6EtapaBValueWritable(average, country));

        }
    }

    public static class ReduceEtapaB extends Reducer<Text, Exercise6EtapaBValueWritable, Text, Text> {
        public void reduce(Text key, Iterable<Exercise6EtapaBValueWritable> values, Context con)
                throws IOException, InterruptedException {

            float max = 0.0f;
            String country = "";
            for(Exercise6EtapaBValueWritable o : values){
                // Procura o valor maximo
                if (o.getPriceComm() > max){
                    max = o.getPriceComm();
                    country = o.getCountry();
                }
            }
            // passando para o reduce valores pre-somados
            con.write(key, new Text(country));
        }
    }
}