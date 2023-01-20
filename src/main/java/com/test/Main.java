package com.test;

import com.azure.core.util.BinaryData;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClientBuilder;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;

import java.util.UUID;

public class Main {

    public static void main(String args[]) {
        if (args.length < 2) {
            System.out.println("Usage: java -jar target/azure-datalake-demo.jar <storage account name> <storage account key>");
            System.exit(1);
        }

        String endpoint = String.format("https://%s.dfs.core.windows.net", args[0]);
        String fileSystemName = UUID.randomUUID().toString();

        StorageSharedKeyCredential credentials = new StorageSharedKeyCredential(args[0], args[1]);
        DataLakeServiceClient dataLakeServiceClient = new DataLakeServiceClientBuilder()
                .endpoint(endpoint)
                .credential(credentials)
                .buildClient();

        DataLakeFileSystemClient dataLakeFileSystemClient = new DataLakeFileSystemClientBuilder()
                .endpoint(endpoint)
                .credential(credentials)
                .fileSystemName(fileSystemName)
                .buildClient();

        try {
            dataLakeServiceClient.createFileSystem(fileSystemName);

            for (int i = 1; i <=5; i++) {
                String fileName = "test-" + i + ".txt";
                System.out.println("Creating file: " + fileName);
                dataLakeFileSystemClient.getFileClient(fileName).upload(BinaryData.fromString("Main " + i));
            }

            System.out.println("Listing paths...");
            dataLakeFileSystemClient.listPaths().forEach(pathItem -> System.out.println("Path Item Name: " + pathItem.getName()));
        } finally {
            dataLakeServiceClient.deleteFileSystem(fileSystemName);
        }
    }
}
