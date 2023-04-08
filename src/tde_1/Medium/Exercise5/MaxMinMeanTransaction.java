package tde_1.Medium.Exercise5;

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

import java.io.File;
import java.io.IOException;

public class MaxMinMeanTransaction {
    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
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
        Job j = new Job(c, "transactionsPerFlowAndYear");
        // Registrar as classes
        j.setJarByClass(MaxMinMeanTransaction.class);
        j.setMapperClass(MapForTransactionsPerFlowAndYear.class);
        j.setReducerClass(ReduceForCombineForMapForTransactionsPerFlowAndYear.class);
        j.setCombinerClass(CombineForMapForTransactionsPerFlowAndYear.class);
        // Definir os tipos de saida
        // MAP
        j.setMapOutputKeyClass(MaxMinMeanTransactionWritable.class);
        j.setMapOutputValueClass(IntWritable.class);
        // REDUCE
        j.setOutputKeyClass(MaxMinMeanTransactionWritable.class);
        j.setOutputValueClass(IntWritable.class);
        // Definir arquivos de entrada e de saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);
        // rodar
        j.waitForCompletion(false);
    }
    public static class MapForTransactionsPerFlowAndYear extends Mapper<LongWritable, Text, MaxMinMeanTransactionWritable, IntWritable> {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {

            // TODO: Montar o map
        }
    }
    public static class CombineForMapForTransactionsPerFlowAndYear extends Reducer<MaxMinMeanTransactionWritable, IntWritable, MaxMinMeanTransactionWritable, IntWritable>{
        public void reduce(MaxMinMeanTransactionWritable key, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException {

            // TODO: Montar o Combine
        }
    }
    public static class ReduceForCombineForMapForTransactionsPerFlowAndYear extends Reducer<MaxMinMeanTransactionWritable, IntWritable, Text, IntWritable> {
        public void reduce(MaxMinMeanTransactionWritable key, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException {

            // TODO: Montar o Reduce
        }
    }
}