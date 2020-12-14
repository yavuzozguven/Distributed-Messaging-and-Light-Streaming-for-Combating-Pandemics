import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Scanner;


public class Consumer {
    public static void main(String[] args) throws IOException {
        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://<ipaddress>:6650")
                .build();


        org.apache.pulsar.client.api.Consumer<byte[]> ner = client.newConsumer()
                .topic("persistent://public/default/ner_out")
                .subscriptionName("subscription")
                .subscribe();


        Scanner sc = new Scanner(System.in);
        System.out.print("Enter val:");
        int val = sc.nextInt();
        if(val == 0) {
            new PrintWriter(System.getProperty("user.home") + "/Desktop/highner1.txt").close();
            new PrintWriter(System.getProperty("user.home") + "/Desktop/highner2.txt").close();
        }


        while(true){
            Message msg = ner.receive();
            String str_ner = new String(msg.getData());
            if(str_ner.contains("!_!")) {
                String splitBy = "!_!";
                String[] arr_ner = str_ner.split(splitBy);
                Files.write(Paths.get(System.getProperty("user.home") + "/Desktop/highner1.txt"), arr_ner[0].concat("\n").getBytes(), StandardOpenOption.APPEND);
                Files.write(Paths.get(System.getProperty("user.home") + "/Desktop/highner2.txt"), arr_ner[1].concat("\n").getBytes(), StandardOpenOption.APPEND);
                System.out.printf("Message recieved: %s \n", arr_ner[1]);
            }
            ner.acknowledge(msg);
        }
    }
}
