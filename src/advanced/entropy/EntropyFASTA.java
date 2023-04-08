package advanced.entropy;

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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;

public class EntropyFASTA {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);
        Path intermediate = new Path("./output/intermediate.tmp");
        // arquivo de saida
        Path output = new Path(files[1]);

        // Criando o primeiro job
        Job j1 = new Job(c, "parte1");
        j1.setJarByClass(EntropyFASTA.class);
        j1.setMapperClass(MapEtapaA.class);
        j1.setReducerClass(ReduceEtapaA.class);
        j1.setMapOutputKeyClass(Text.class);
        j1.setMapOutputValueClass(LongWritable.class);
        j1.setOutputKeyClass(Text.class);
        j1.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(j1, input);
        FileOutputFormat.setOutputPath(j1, intermediate);

        // Rodo o job 1
        // argumentos: in/JY157487.1.fasta output/entropia.txt
        j1.waitForCompletion(false);

        // Configuracao do job 2
        Job j2 = new Job(c, "entropia");
        j2.setJarByClass(EntropyFASTA.class);
        j2.setMapperClass(MapEtapaB.class);
        j2.setReducerClass(ReduceEtapaB.class);
        j2.setMapOutputKeyClass(Text.class);
        j2.setMapOutputValueClass(BaseQtdWritable.class);
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(j2, intermediate);
        FileOutputFormat.setOutputPath(j2, output);
        j2.waitForCompletion(false);


    }

    public static class MapEtapaA extends Mapper<LongWritable, Text, Text, LongWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            // Pegando a linha e quebrando em caracteres
            String linha = value.toString();
            // condicao para ignorar o cabecalho
            if (!linha.startsWith(">")) {
                // para cada caracter, gerando (caracter, 1)
                String caracteres[] = linha.split("");
                for (String c : caracteres) {
                    con.write(new Text(c), new LongWritable(1));
                    con.write(new Text("total"), new LongWritable(1));
                }
            }
        }
    }

    public static class ReduceEtapaA extends Reducer<Text, LongWritable, Text, LongWritable> {
        public void reduce(Text key, Iterable<LongWritable> values, Context con)
                throws IOException, InterruptedException {
            // somando todas as ocorrencias
            long contagem = 0;
            for(LongWritable v : values){
                contagem += v.get();
            }
            // escrevendo o arquivo de resultados
            con.write(key, new LongWritable(contagem));
        }
    }


    public static class MapEtapaB extends Mapper<LongWritable, Text, Text, BaseQtdWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            // Pegando uma linha
            String linha = value.toString();
            String campos[] = linha.split("\t");
            String caracter = campos[0];
            long qtd = Long.parseLong(campos[1]);

            Text chaveGenerica = new Text("entropia");
            BaseQtdWritable valor = new BaseQtdWritable(caracter, qtd);

            con.write(chaveGenerica, valor);

        }
    }

    public static class ReduceEtapaB extends Reducer<Text, BaseQtdWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<BaseQtdWritable> values, Context con)
                throws IOException, InterruptedException {

            double qtdTotal = 0.0;
            ArrayList<BaseQtdWritable> listagem = new ArrayList<>();
            for (BaseQtdWritable v : values){
                if(v.getCaracter().equals("total")){
                    qtdTotal = v.getContagem();
                }else{
                    listagem.add(
                            new BaseQtdWritable(v.getCaracter(), v.getContagem()));
                }
            }

            // percorrer os caracteres e calcular as probabilidades e as entropias
            for (BaseQtdWritable v : listagem){
                double prob = v.getContagem() / qtdTotal;
                double entropia = -prob * (Math.log(prob) / Math.log(2.0));
                con.write(new Text(v.getCaracter()), new DoubleWritable(entropia));
            }

        }
    }
}
