package basic;

import java.io.IOException;

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


public class WordCount {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "wordcount");

        // Registro de classes
        j.setJarByClass(WordCount.class); // Classe que contem o metodo MAIN
        j.setMapperClass(MapForWordCount.class); // Classe que contem o MAP
        j.setReducerClass(ReduceForWordCount.class); // Classe que contem o metodo REDUCE

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

    public static class MapForWordCount extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            // Coverte para string
            String linha = value.toString();

            // Quebrando a linha em palavras
            String palavras[] = linha.split(" ");

            // Iterando pelas palavras e criando (chave, valor)
            for (String p: palavras) {

                String palavra = p;
                // remove espaços em branco
                String palavraSemEspacos = palavra.replaceAll("\\s+", "");
                // remove caracteres não alfabéticos
                String apenasLetras = palavraSemEspacos.replaceAll("[^a-zA-Z]", "");
                // remove o caractere ':'
                String semDoisPontos = apenasLetras.replaceAll(":", "");

                Text chave = new Text(semDoisPontos);
                IntWritable valor = new IntWritable(1);

                // mandando a (chave, valor) pro reduce
                con.write(chave, valor);
            }
        }
    }

    public static class ReduceForWordCount extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {
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
