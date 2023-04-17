package services;

import org.apache.commons.io.FileUtils;
import java.io.File;
import java.io.IOException;

public class DirectoryManage {
    public DirectoryManage() {

    }
    public static void deleteResultFold(String pathResult) throws IOException {
        // Para não precisar ficar deletando o diretório em cada teste fiz essa pequena lógica
        File diretorio = new File("../big-data-mapreduce-hadoop3-student-master/" + pathResult);
        if(diretorio.exists()){
            FileUtils.deleteDirectory(diretorio);
        }
    }

    public static void deleteIntermedieteFold(String pathResult) throws IOException {
        // Para não precisar ficar deletando o diretório em cada teste fiz essa pequena lógica
        File diretorio = new File("../big-data-mapreduce-hadoop3-student-master/output/" + pathResult);
        if(diretorio.exists()){
            FileUtils.deleteDirectory(diretorio);
        }
    }
}
