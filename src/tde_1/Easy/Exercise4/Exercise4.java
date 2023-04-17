package tde_1.Easy.Exercise4;

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

public class Exercise4 {
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
        j.setJarByClass(Exercise4.class);
        j.setMapperClass(MapForExercise4.class);
        j.setReducerClass(ReduceForExercise4.class);
        j.setCombinerClass(CombineForExercise4.class);
        // Definir os tipos de saida
        // MAP
        j.setMapOutputKeyClass(Exercise4KeyWritable.class);
        j.setMapOutputValueClass(Exercise4ValueWritable.class);
        // REDUCE
        j.setOutputKeyClass(Exercise4KeyWritable.class);
        j.setOutputValueClass(FloatWritable.class);
        // Definir arquivos de entrada e de saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);
        // rodar
        j.waitForCompletion(false);
    }

    public static class MapForExercise4
            extends
            Mapper<LongWritable, Text, Exercise4KeyWritable, Exercise4ValueWritable> {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {

            String linha = value.toString();
            String colunas[] = linha.split(";");

            String keyString = key.toString();
            // Se nãp for a primeira linha
            if (!keyString.equals("0")) {
                String country = colunas[0];
                String flow = colunas[4];
                String year = colunas[1];
                String unitType = colunas[7];
                String category = colunas[9];
                float priceComm = Float.parseFloat(colunas[5]);
                int yearInt = Integer.parseInt(year);
                if (country.equals("Brazil") && flow.equals("Export")) {
                    con.write(new Exercise4KeyWritable(country, flow, yearInt, unitType, category),
                            new Exercise4ValueWritable(priceComm, 1));
                }
            }
        }
    }

    public static class CombineForExercise4 extends
            Reducer<Exercise4KeyWritable, Exercise4ValueWritable, Exercise4KeyWritable, Exercise4ValueWritable> {
        public void reduce(Exercise4KeyWritable key,
                           Iterable<Exercise4ValueWritable> values, Context con)
                throws IOException, InterruptedException {
            int contagem = 0;
            float sum = 0.0f;

            // Varendo o values
            for (Exercise4ValueWritable v : values) {
                contagem += v.getQntd();
                sum += v.getPriceComm();
            }
            // salvando os resultados em disco
            con.write(key, new Exercise4ValueWritable(sum, contagem));
        }
    }

    public static class ReduceForExercise4
            extends
            Reducer<Exercise4KeyWritable, Exercise4ValueWritable, Text, FloatWritable> {
        public void reduce(Exercise4KeyWritable key,
                           Iterable<Exercise4ValueWritable> values, Context con)
                throws IOException, InterruptedException {
            int contagem = 0;
            float sum = 0.0f;
            float average = 0.0f;
            String result = key.getCountry() + " " + key.getFlow() + " | " + key.getYear() + " " + key.getUnitType() + " " + key.getCategory() + " | Média : ";
            // Varendo o values
            for (Exercise4ValueWritable v : values) {
                contagem += v.getQntd();
                sum += v.getPriceComm();
                // salvando os resultados em disco

            }
            average = sum / contagem;
            con.write(new Text(result), new FloatWritable(average));
        }
    }
}