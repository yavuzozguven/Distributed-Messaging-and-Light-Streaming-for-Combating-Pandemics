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


        //Pattern allTopicsInNamespace = Pattern.compile("persistent://public/default/tr_.*");
        org.apache.pulsar.client.api.Consumer<byte[]> translate = client.newConsumer()
                .topic("persistent://public/default/tr_UK")
                //.topicsPattern(allTopicsInNamespace)
                .subscriptionName("subscription")
                .subscribe();


        Scanner sc = new Scanner(System.in);
        System.out.print("Enter val:");
        int val = sc.nextInt();
        if(val == 0) {
            new PrintWriter(System.getProperty("user.home") + "/Desktop/hightr1.txt").close();
            new PrintWriter(System.getProperty("user.home") + "/Desktop/hightr2.txt").close();
        }


        while(true){
             Message msg = translate.receive();
             String tr = new String(msg.getData());
             if(tr.contains("!_!")) {
             String splitBy = "!_!";
             String[] arr_tr = tr.split(splitBy);
             Files.write(Paths.get(System.getProperty("user.home") + "/Desktop/hightr1.txt"), arr_tr[0].concat("\n").getBytes(), StandardOpenOption.APPEND);
             Files.write(Paths.get(System.getProperty("user.home") + "/Desktop/hightr2.txt"), arr_tr[1].concat("\n").getBytes(), StandardOpenOption.APPEND);
             System.out.printf("%s\n", tr);
             }
             translate.acknowledge(msg);
        }
    }
}
