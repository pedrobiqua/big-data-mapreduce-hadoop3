package services;

import org.apache.commons.io.FileUtils;
import java.io.File;
import java.io.IOException;

public class DirectoryManage {
    public DirectoryManage() {

    }
    public static void deleteResultFold() throws IOException {
        // Para não precisar ficar deletando o diretório em cada teste fiz essa pequena lógica
        File diretorio = new File("../big-data-mapreduce-hadoop3-student-master/output/result");
        if(diretorio.exists()){
            FileUtils.deleteDirectory(diretorio);
        }
    }
}
