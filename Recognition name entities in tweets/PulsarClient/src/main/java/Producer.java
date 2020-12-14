import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.pulsar.client.api.PulsarClient;

import java.io.*;
import java.util.concurrent.TimeUnit;


public class Producer{
    public static void main(String[] args) throws IOException{

        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://<ipaddress>:6650")
                .build();


        org.apache.pulsar.client.api.Producer<byte[]> ner = client.newProducer()
                .topic("persistent://public/default/ner_in")
                .batchingMaxPublishDelay(10, TimeUnit.MILLISECONDS)
                .sendTimeout(10, TimeUnit.MILLISECONDS)
                .maxPendingMessages(10)
                .blockIfQueueFull(true)
                .create();



        Reader in = new FileReader(String.valueOf(Producer.class.getResource("/tweets.csv").getFile()));
        Iterable<CSVRecord> records = CSVFormat.EXCEL.withHeader().withSkipHeaderRecord().parse(in);


        for(CSVRecord record : records){
            String ner_mes = record.get(17);
            ner.sendAsync(ner_mes.getBytes());
        }

        ner.closeAsync();


        System.out.println("end");

    }
}
