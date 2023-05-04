import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class DocumentProcessor {
    public static void main(String[] args) {
        // Configurações do Kafka Streams
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("application.id", "document-processor");

        // Cria um StreamsBuilder
        StreamsBuilder builder = new StreamsBuilder();

        // Define o tópico de entrada
        KStream<String, byte[]> documents = builder.stream("input-topic");

        // Filtra apenas os arquivos com extensão .xlsx e processa-os
        KStream<String, byte[]> excelDocuments = documents.filter((key, value) -> key.endsWith(".xlsx"));
        excelDocuments.foreach((key, value) -> {
            // Processa o arquivo Excel
            // ...

            // Envia o resultado para o tópico de saída
            // ...
        });

        // Filtra apenas os arquivos com extensão .txt e processa-os
        KStream<String, byte[]> txtDocuments = documents.filter((key, value) -> key.endsWith(".txt"));
        txtDocuments.foreach((key, value) -> {
            // Processa o arquivo TXT
            // ...

            // Envia o resultado para o tópico de saída
            // ...
        });

        // Filtra apenas os arquivos com extensão .cnab240 e processa-os
        KStream<String, byte[]> cnab240Documents = documents.filter((key, value) -> key.endsWith(".cnab240"));
        cnab240Documents.foreach((key, value) -> {
            // Processa o arquivo CNAB240
            // ...

            // Envia o resultado para o tópico de saída
            // ...
        });

        // Filtra apenas os arquivos com extensão .cnab400 e processa-os
        KStream<String, byte[]> cnab400Documents = documents.filter((key, value) -> key.endsWith(".cnab400"));
        cnab400Documents.foreach((key, value) -> {
            // Processa o arquivo CNAB400
            // ...

            // Envia o resultado para o tópico de saída
            // ...
        });

        // Define o tópico de saída para cada tipo de documento processado
        excelDocuments.to("output-excel-topic", Produced.with(org.apache.kafka.common.serialization.Serdes.String(), org.apache.kafka.common.serialization.Serdes.ByteArray()));
        txtDocuments.to("output-txt-topic", Produced.with(org.apache.kafka.common.serialization.Serdes.String(), org.apache.kafka.common.serialization.Serdes.ByteArray()));
        cnab240Documents.to("output-cnab240-topic", Produced.with(org.apache.kafka.common.serialization.Serdes.String(), org.apache.kafka.common.serialization.Serdes.ByteArray()));
        cnab400Documents.to("output-cnab400-topic", Produced.with(org.apache.kafka.common.serialization.Serdes.String(), org.apache.kafka.common.serialization.Serdes.ByteArray()));

        // Cria o KafkaStreams e inicia o processamento
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }
}
