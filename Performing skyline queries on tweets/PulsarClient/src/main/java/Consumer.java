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


        org.apache.pulsar.client.api.Consumer<byte[]> sq = client.newConsumer()
                .topic("persistent://public/default/sq_out")
                .subscriptionName("subscription")
                .subscribe();


        Scanner sc = new Scanner(System.in);
        System.out.print("Enter val:");
        int val = sc.nextInt();
        if(val == 0) {
            new PrintWriter(System.getProperty("user.home") + "/Desktop/highsq1.txt").close();
            new PrintWriter(System.getProperty("user.home") + "/Desktop/highsq2.txt").close();
            new PrintWriter(System.getProperty("user.home") + "/Desktop/highsq3.txt").close();
            new PrintWriter(System.getProperty("user.home") + "/Desktop/highsq4.txt").close();
        }


        while(true){
            Message msg = sq.receive();
            String str_sq = new String(msg.getData());
            if(str_sq.contains("!_!")) {
                if(str_sq.contains("end")){
                    String splitBy = "!_!";
                    String[] arr_sq = str_sq.split(splitBy);
                    arr_sq[0] = arr_sq[0].substring(3);
                    Files.write(Paths.get(System.getProperty("user.home") + "/Desktop/highsq3.txt"), arr_sq[0].concat("\n").getBytes(), StandardOpenOption.APPEND);
                    Files.write(Paths.get(System.getProperty("user.home") + "/Desktop/highsq4.txt"), arr_sq[1].concat("\n").getBytes(), StandardOpenOption.APPEND);
                }
                else{
                    String splitBy = "!_!";
                    String[] arr_sq = str_sq.split(splitBy);
                    Files.write(Paths.get(System.getProperty("user.home") + "/Desktop/highsq1.txt"), arr_sq[0].concat("\n").getBytes(), StandardOpenOption.APPEND);
                    Files.write(Paths.get(System.getProperty("user.home") + "/Desktop/highsq2.txt"), arr_sq[1].concat("\n").getBytes(), StandardOpenOption.APPEND);
                    System.out.printf("Message recieved: %s \n", arr_sq[1]);
                }
            }
            sq.acknowledge(msg);
        }
    }
}
