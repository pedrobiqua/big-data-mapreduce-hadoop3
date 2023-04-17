package tde_1.Medium.Exercise5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

public class Exercise5 {
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
        Job j = new Job(c, "maxMinMean");
        // Registrar as classes
        j.setJarByClass(Exercise5.class);
        j.setMapperClass(MapForExercise5.class);
        j.setReducerClass(ReduceForExercise5.class);
        j.setCombinerClass(CombineForExercise5.class);
        // Definir os tipos de saida
        // MAP
        j.setMapOutputKeyClass(Exercise5KeyWritable.class);
        j.setMapOutputValueClass(Exercise5ValueWritable.class);
        // REDUCE
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(Text.class);
        // Definir arquivos de entrada e de saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);
        // rodar
        j.waitForCompletion(false);
    }
    public static class MapForExercise5 extends Mapper<LongWritable, Text,
            Exercise5KeyWritable, Exercise5ValueWritable> {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {

            String linha = value.toString();
            String colunas[] = linha.split(";");
            String keyString = key.toString();

            // Se não for a primeira linha
            if (!keyString.equals("0")){
                String unityType = colunas[7].trim();
                String year = colunas[1].trim();
                String price = colunas[5].trim();
                float priceFloat = Float.parseFloat(price);
                // float quantidade = Float.parseFloat(colunas[8]);
                // float priceByUnit = priceFloat/quantidade;
                con.write(new Exercise5KeyWritable(unityType, year), new Exercise5ValueWritable(priceFloat, 1));
            }
        }
    }
    public static class CombineForExercise5 extends Reducer<Exercise5KeyWritable, Exercise5ValueWritable,
            Exercise5KeyWritable, Exercise5ValueWritable>{
        public void reduce(Exercise5KeyWritable key, Iterable<Exercise5ValueWritable> values, Context con)
                throws IOException, InterruptedException {
            float max = 0.0f;
            float min = 0.0f;
            float somaPrice = 0;
            int somaQtds = 0;
            for(Exercise5ValueWritable o : values){
                somaPrice += o.getPrice();
                somaQtds += o.getQtd();
                // Procura o valor maximo
                if (o.getPrice() > max){
                    max = o.getPrice();
                }

                // Procura o valor minimo
                if ((min == 0.0f) || o.getPrice() < min){
                    min = o.getPrice();
                }
            }
            // passando para o reduce valores pre-somados
            con.write(key, new Exercise5ValueWritable(somaPrice, somaQtds, max, min));
        }
    }
    public static class ReduceForExercise5 extends Reducer<Exercise5KeyWritable, Exercise5ValueWritable,
            Text, Text> {
        public void reduce(Exercise5KeyWritable key, Iterable<Exercise5ValueWritable> values, Context con)
                throws IOException, InterruptedException {

            float max = 0.0f;
            float min = 0.0f;
            float somaCommValues = 0;
            int somaQtds = 0;
            for (Exercise5ValueWritable o : values){
                somaCommValues += o.getPrice();
                somaQtds += o.getQtd();

                // Procura o valor maximo
                if (o.getMax() > max){
                    max = o.getMax();
                }

                // Procura o valor minimo
                if ((min == 0.0f) || o.getMin() < min){
                    min = o.getMin();
                }
            }
            // calcular a media
            float media = somaCommValues / somaQtds;
            
            // salvando o resultado
            con.write(new Text(key.getUnityType() + " " + key.getYear()), new Text("| média: " + media + " | max: " + max + " | min: " + min + " |"));
        }
    }
}