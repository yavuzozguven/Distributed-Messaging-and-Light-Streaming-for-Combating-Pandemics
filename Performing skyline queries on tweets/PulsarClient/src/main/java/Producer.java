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


        org.apache.pulsar.client.api.Producer<byte[]> sq = client.newProducer()
                .topic("persistent://public/default/sq_in")
                .batchingMaxPublishDelay(10, TimeUnit.MILLISECONDS)
                .sendTimeout(10, TimeUnit.MILLISECONDS)
                .maxPendingMessages(10)
                .blockIfQueueFull(true)
                .create();



        Reader in = new FileReader(String.valueOf(Producer.class.getResource("/tweets.csv").getFile()));
        Iterable<CSVRecord> records = CSVFormat.EXCEL.withHeader().withSkipHeaderRecord().parse(in);

        boolean flag = true;
        for(CSVRecord record : records){
            if(flag){
                String sq_mes = "start";
                sq.sendAsync(sq_mes.getBytes());
                flag = false;
            }
            else{
                String sq_mes = record.get(5) + "!|!" + record.get(24);
                sq.sendAsync(sq_mes.getBytes());
            }
        }

        String ms = "end";
        sq.sendAsync(ms.getBytes());
        sq.closeAsync();


        System.out.println("end");

    }
}
