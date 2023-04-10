package tde_1.Medium.Exercise5;

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

public class MaxMinMeanTransaction {
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
        Job j = new Job(c, "maxMinMean");
        // Registrar as classes
        j.setJarByClass(MaxMinMeanTransaction.class);
        j.setMapperClass(MapForTransactionsPerFlowAndYear.class);
        j.setReducerClass(ReduceForCombineForMapForTransactionsPerFlowAndYear.class);
        j.setCombinerClass(CombineForMapForTransactionsPerFlowAndYear.class);
        // Definir os tipos de saida
        // MAP
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(MaxMinMeanTransactionWritable.class);
        // REDUCE
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(FloatWritable.class);
        // Definir arquivos de entrada e de saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);
        // rodar
        j.waitForCompletion(false);
    }
    public static class MapForTransactionsPerFlowAndYear extends Mapper<LongWritable, Text, Text, MaxMinMeanTransactionWritable> {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {

            String linha = value.toString();
            String colunas[] = linha.split(";");
            String keyString = key.toString();

            // Se nãp for a primeira linha
            if (!keyString.equals("0")){
                String unityType = colunas[7];
                String year = colunas[1];
                String price = colunas[5];
                float quantidade = Float.parseFloat(colunas[8]);
                float priceFloat = Float.parseFloat(price);
                float priceByUnit = priceFloat/quantidade;
                String chave = unityType + " " + year;
                // con.write(new Text("Max"), new MaxMinMeanTransactionWritable(priceFloat, 1));
                // con.write(new Text("Min"), new MaxMinMeanTransactionWritable(priceFloat, 1));
                con.write(new Text(chave), new MaxMinMeanTransactionWritable(priceByUnit, 1));
            }
        }
    }
    public static class CombineForMapForTransactionsPerFlowAndYear extends Reducer<Text, MaxMinMeanTransactionWritable, Text, MaxMinMeanTransactionWritable>{
        public void reduce(Text key, Iterable<MaxMinMeanTransactionWritable> values, Context con) throws IOException, InterruptedException {
            float temp = 0.0f;
            if (key.toString().equals("Max")){
                for (MaxMinMeanTransactionWritable o :
                        values) {
                    float price = o.getPrice();
                    if (temp < price) temp = price;
                }

                con.write(key, new MaxMinMeanTransactionWritable(temp, 1));
            }
            else
            {
                float somaPrice = 0;
                int somaQtds = 0;
                for(MaxMinMeanTransactionWritable o : values){
                    somaPrice += o.getPrice();
                    somaQtds += o.getQtd();
                }
                // passando para o reduce valores pre-somados
                con.write(key, new MaxMinMeanTransactionWritable(somaPrice, somaQtds));
            }
        }
    }
    public static class ReduceForCombineForMapForTransactionsPerFlowAndYear extends Reducer<Text, MaxMinMeanTransactionWritable, Text, FloatWritable> {
        public void reduce(Text key, Iterable<MaxMinMeanTransactionWritable> values, Context con) throws IOException, InterruptedException {

            // logica do reduce:
            // recebe diferentes objetos compostos (temperatura, qtd)
            // somar as temperaturas e somar as qtds
            float somaTemps = 0;
            int somaQtds = 0;
            for (MaxMinMeanTransactionWritable o : values){
                somaTemps += o.getPrice();
                somaQtds += o.getQtd();
            }
            // calcular a media

            float media = somaTemps / somaQtds;
            // salvando o resultado
            con.write(key, new FloatWritable(media));
        }
    }
}