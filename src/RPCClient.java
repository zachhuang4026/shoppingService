import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.*;

public class RPCClient implements AutoCloseable {

    private Connection connection;
    private Channel channel;
    private String requestQueueName = "rpc_queue";

    public RPCClient(String hostIp) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(hostIp);

        connection = factory.newConnection();
        channel = connection.createChannel();
    }

    public static void main(String[] argv) {
        try (RPCClient rpc = new RPCClient(argv[0])) {
            
            String getMsg = "{\"type\": \"getShoppingCart\", \"userId\": \"1234\"}";
            String checkoutMsg = "{\"type\": \"checkout\", \"userId\": \"1234\"}";
            String addMsg = "{\"type\": \"addToShoppingCart\", \"userId\": \"1234\", \"itemId\": \"5678\"}";
            String deleteMsg = "{\"type\": \"deleteFromShoppingCart\", \"userId\": \"1234\", \"itemId\": \"5678\"}";

            String[] messages = {addMsg, getMsg, deleteMsg, getMsg, addMsg, checkoutMsg, getMsg};
            for (String message : messages) {
                System.out.println(" [x] Requesting " + message);
                String response = rpc.call(message);
                System.out.println(" [.] Got '" + response + "'");
            }
        } catch (IOException | TimeoutException | InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    public String call(String message) throws IOException, InterruptedException, ExecutionException {
        final String corrId = UUID.randomUUID().toString();

        String replyQueueName = channel.queueDeclare().getQueue();
        AMQP.BasicProperties props = new AMQP.BasicProperties
                .Builder()
                .correlationId(corrId)
                .replyTo(replyQueueName)
                .build();

        channel.basicPublish("", requestQueueName, props, message.getBytes("UTF-8"));

        final CompletableFuture<String> response = new CompletableFuture<>();

        String ctag = channel.basicConsume(replyQueueName, true, (consumerTag, delivery) -> {
            if (delivery.getProperties().getCorrelationId().equals(corrId)) {
                response.complete(new String(delivery.getBody(), "UTF-8"));
            }
        }, consumerTag -> {
        });

        String result = response.get();
        channel.basicCancel(ctag);
        return result;
    }

    public void close() throws IOException {
        connection.close();
    }
}

