package tde_1.Easy.Exercise4;

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
import services.DirectoryManage;

import java.io.IOException;

public class TransactionsPerFlowAndYear {
    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {

        DirectoryManage.deleteResultFold();
        BasicConfigurator.configure();
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("in/transactions_amostra.csv");
        // arquivo de saida
        Path output = new Path("output/result");
        // criacao do job e seu nome
        Job j = new Job(c, "transactionsPerFlowAndYear");
        // Registrar as classes
        j.setJarByClass(TransactionsPerFlowAndYear.class);
        j.setMapperClass(MapForTransactionsPerFlowAndYear.class);
        j.setReducerClass(ReduceForCombineForMapForTransactionsPerFlowAndYear.class);
        j.setCombinerClass(CombineForMapForTransactionsPerFlowAndYear.class);
        // Definir os tipos de saida
        // MAP
        j.setMapOutputKeyClass(TransactionsFlowAndYearTempWritable.class);
        j.setMapOutputValueClass(TransactionsFlowAndYearTempValueWritable.class);
        // REDUCE
        j.setOutputKeyClass(TransactionsFlowAndYearTempWritable.class);
        j.setOutputValueClass(FloatWritable.class);
        // Definir arquivos de entrada e de saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);
        // rodar
        j.waitForCompletion(false);
    }

    public static class MapForTransactionsPerFlowAndYear
            extends
            Mapper<LongWritable, Text, TransactionsFlowAndYearTempWritable, TransactionsFlowAndYearTempValueWritable> {
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
                    con.write(new TransactionsFlowAndYearTempWritable(country, flow, yearInt, unitType, category),
                            new TransactionsFlowAndYearTempValueWritable(priceComm, 1));
                }
            }
        }
    }

    public static class CombineForMapForTransactionsPerFlowAndYear extends
            Reducer<TransactionsFlowAndYearTempWritable, TransactionsFlowAndYearTempValueWritable, TransactionsFlowAndYearTempWritable, TransactionsFlowAndYearTempValueWritable> {
        public void reduce(TransactionsFlowAndYearTempWritable key,
                Iterable<TransactionsFlowAndYearTempValueWritable> values, Context con)
                throws IOException, InterruptedException {
            int contagem = 0;
            float sum = 0.0f;

            // Varendo o values
            for (TransactionsFlowAndYearTempValueWritable v : values) {
                contagem += v.getQntd();
                sum += v.getPriceComm();
            }
            // salvando os resultados em disco
            con.write(key, new TransactionsFlowAndYearTempValueWritable(sum, contagem));
        }
    }

    public static class ReduceForCombineForMapForTransactionsPerFlowAndYear
            extends
            Reducer<TransactionsFlowAndYearTempWritable, TransactionsFlowAndYearTempValueWritable, Text, FloatWritable> {
        public void reduce(TransactionsFlowAndYearTempWritable key,
                Iterable<TransactionsFlowAndYearTempValueWritable> values, Context con)
                throws IOException, InterruptedException {
            int contagem = 0;
            float sum = 0.0f;
            float average = 0.0f;
            String result = key.getFlow() + " " + key.getYear() + " " + key.getUnitType() + " " + key.getCategory();
            // Varendo o values
            for (TransactionsFlowAndYearTempValueWritable v : values) {
                contagem += v.getQntd();
                sum += v.getPriceComm();
                // salvando os resultados em disco

            }
            average = sum / contagem;
            con.write(new Text(result), new FloatWritable(average));
        }
    }
}