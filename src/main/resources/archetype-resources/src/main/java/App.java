import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import com.fasterxml.jackson.databind.ObjectMapper;

public class FileProcessor {
    private static final String INPUT_TOPIC = "input_files";
    private static final String OUTPUT_TOPIC = "processed_files";
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "file-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> inputStream = builder.stream(INPUT_TOPIC);

        KStream<String, String> outputStream = inputStream
                .mapValues(value -> {
                    String fileExtension = getFileExtension(value);
                    switch (fileExtension) {
                        case "xlsx":
                            return processExcelFile(value);
                        case "txt":
                            return processTxtFile(value);
                        case "cnab240":
                            return processCnab240File(value);
                        case "cnab400":
                            return processCnab400File(value);
                        default:
                            return "Unsupported file type";
                    }
                });

        outputStream.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        while (true) {
            Thread.sleep(1000);
            System.out.println("Processed files: " + outputStream.count().get());
        }
    }

    private static String processExcelFile(String filePath) {
        try {
            Workbook workbook = new XSSFWorkbook(new File(filePath));
            Sheet sheet = workbook.getSheetAt(0);
            StringBuilder output = new StringBuilder();
            for (Row row : sheet) {
                for (Cell cell : row) {
                    output.append(cell.toString());
                }
            }
            return output.toString();
        } catch (IOException e) {
            return "Error processing Excel file: " + e.getMessage();
        }
    }

    private static String processTxtFile(String filePath) {
        try {
            byte[] bytes = Files.readAllBytes(Paths.get(filePath));
            return new String(bytes);
        } catch (IOException e) {
            return "Error processing TXT file: " + e.getMessage();
        }
    }

    private static String processCnab240File(String filePath) {
        // TODO: implement CNAB240 processing logic
        return "CNAB240 file processing not implemented yet";
    }

    private static String processCnab400File(String filePath) {
        // TODO: implement CNAB400 processing logic
        return "CNAB400 file processing not implemented yet";
    }

    private static String getFileExtension(String filePath) {
        int lastIndex = filePath.lastIndexOf('.');
        if (lastIndex == -1) {
            return "";
        } else {
            return filePath.substring(lastIndex + 1);
        }
    }
}
