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


        org.apache.pulsar.client.api.Producer<byte[]> translate = client.newProducer()
                .topic("persistent://public/default/tr_in")
                .batchingMaxPublishDelay(10, TimeUnit.MILLISECONDS)
                .sendTimeout(10, TimeUnit.MILLISECONDS)
                .maxPendingMessages(1)
                .blockIfQueueFull(true)
                .create();


        Reader in = new FileReader(String.valueOf(Producer.class.getResource("/tweets.csv").getFile()));
        Iterable<CSVRecord> records = CSVFormat.EXCEL.withHeader().withSkipHeaderRecord().parse(in);


        for(CSVRecord record : records){
            String trans_mes = record.get(0) + "!|!" + record.get(17);
            translate.sendAsync(trans_mes.getBytes());
        }

        translate.closeAsync();


        System.out.println("end");

    }
}
