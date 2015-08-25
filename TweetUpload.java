/*
Copyright 2015 Aalto University, Hussnain Ahmed
 
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
 
    http://www.apache.org/licenses/LICENSE-2.0
 
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 
*/

import java.io.*;
import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.blob.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
 
public class BlobSample {
        public static final String storageConnectionString = "DefaultEndpointsProtocol=http;"
                        + "AccountName=account_name_here;"
                        + "AccountKey=account_key_here";
 
        public static void main(String[] args) {
                try {
 
                        String filePath = "hdfs://localhost.localdomain:8020/tmp/hive-mapred/"
                                        + args[0] + "/000000_0"; // File location
 
                        Configuration configuration = new Configuration();
 
                        Path path = new Path(filePath);
                        Path newFilePath = new Path("temp_" + args[0]);
                        FileSystem fs = path.getFileSystem(configuration);
                       
                        fs.copyToLocalFile(path, newFilePath);
                        // Copy temporary to local directory
 
                        CloudStorageAccount account = CloudStorageAccount
                                        .parse(storageConnectionString);
                        CloudBlobClient serviceClient = account.createCloudBlobClient();
 
                        CloudBlobContainer container = serviceClient
                                        .getContainerReference("container_name_here"); // Container name (must be lower case)
                        container.createIfNotExists();
 
                        // Upload file
                        CloudBlockBlob blob = container
                                        .getBlockBlobReference("user/rdp_username_here/analysisFiles/"
                                                        + args[0] + ".tsv");
                        File sourceFile = new File(newFilePath.toString());
                        blob.upload(new FileInputStream(sourceFile), sourceFile.length());
 
                        File tmpFile = new File(newFilePath.toString());
                        tmpFile.delete(); // Delete the temporary file
                       
                       
                        // In case of errors
                } catch (Exception e) {
                        System.exit(-1);
                }
        }
