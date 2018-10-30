package readFileFromHDFS;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;


public class FileReadFromHdfs {

    public static void main(String[] args) throws Exception {
        try {
            System.setProperty("hadoop.home.dir", "D:\\installed\\hadoop-common-2.2.0-bin-master");
            String dsf = "hdfs://10.10.0.104:9000/hello.txt";
            Configuration conf = new Configuration();

            FileSystem fs = FileSystem.get(URI.create(dsf),conf);

            FSDataInputStream hdfsInStream = fs.open(new Path(dsf));

            byte[] ioBuffer = new byte[1024];
            int readLen = hdfsInStream.read(ioBuffer);
            while(readLen!=-1)
            {
                System.out.write(ioBuffer, 0, readLen);
                readLen = hdfsInStream.read(ioBuffer);
            }
            hdfsInStream.close();
            fs.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }



        downloadFile("C:\\Users\\cafeyqian\\Desktop\\","hdfs://10.10.0.104:9000/picture_pix.txt");


    }



    /**
     * 下载hdfs文件到本地目录
     *
     * @param dstPath
     * @param srcPath
     * @throws Exception
     */
    public static void downloadFile(String dstPath, String srcPath) throws Exception {
        Path path = new Path(srcPath);
        Configuration conf = new Configuration();
        FileSystem hdfs = path.getFileSystem(conf);

        File rootfile = new File(dstPath);
        if (!rootfile.exists()) {
            rootfile.mkdirs();
        }

        if (hdfs.isFile(path)) {
            //只下载非txt文件
            String fileName = path.getName();
            if (fileName.toLowerCase().endsWith("txt")) {
                FSDataInputStream in = null;
                FileOutputStream out = null;
                try {
                    in = hdfs.open(path);
                    File srcfile = new File(rootfile, path.getName());
                    if (!srcfile.exists()) srcfile.createNewFile();
                    out = new FileOutputStream(srcfile);
                    IOUtils.copyBytes(in, out, 4096, false);
                } finally {
                    IOUtils.closeStream(in);
                    IOUtils.closeStream(out);
                }
                //下载完后，在hdfs上将原文件删除
                // delete(path.toString());
            }
        } else if (hdfs.isDirectory(path)) {
            File dstDir = new File(dstPath);
            if (!dstDir.exists()) {
                dstDir.mkdirs();
            }
            //在本地目录上加一层子目录
            String filePath = path.toString();//目录
            String subPath[] = filePath.split("/");
            String newdstPath = dstPath + subPath[subPath.length - 1] + "/";
            System.out.println("newdstPath=======" + newdstPath);
            if (hdfs.exists(path) && hdfs.isDirectory(path)) {
                FileStatus[] srcFileStatus = hdfs.listStatus(path);
                if (srcFileStatus != null) {
                    for (FileStatus status : hdfs.listStatus(path)) {
                        //下载子目录下文件
                        downloadFile(newdstPath, status.getPath().toString());
                    }
                }
            }
        }
    }


    /**
     * 删除hdfs文件
     *
     * @param filePath
     * @throws IOException
     */
    public static void delete(String filePath) throws IOException {
        Configuration conf = new Configuration();
        Path path = new Path(filePath);
        FileSystem fs = path.getFileSystem(conf);

        boolean isok = fs.deleteOnExit(path);
        if (isok) {
            System.out.println("delete file " + filePath + " success!");
        } else {
            System.out.println("delete file " + filePath + " failure");
        }
        //fs.close();
    }

}  