import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.rabbitmq.client.*;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class RPCServer {

    private static final String RPC_QUEUE_NAME = "rpc_queue";

    private static java.sql.Connection createConn() {
		java.sql.Connection conn = null;
		try {
			System.out.println("calling Class.forName()");
			Class.forName("org.postgresql.Driver");
			System.out.println("building connection string");
			String connectionUrl = "jdbc:postgresql://172.17.0.3:5432/shopping";
			String connectionUser = "postgres";
			String connectionPassword = "mysecret";
			System.out.println("calling getConnection");
			conn = DriverManager.getConnection(connectionUrl, connectionUser, connectionPassword);
			return conn;
		} catch (Exception e) {
			e.printStackTrace();
		} 
        return null;
    }

    private static String process(String request) {
        // Parse JSON request
        JSONParser parser = new JSONParser();
        String type = "";
        String userId = "";
        String itemId = "";
        try {
            JSONObject obj = (JSONObject) parser.parse(request);
            type = (String) obj.get("type");
            userId = (String) obj.get("userId");
            itemId = (String) obj.get("itemId");
        } catch (ParseException e) {
            e.printStackTrace();
        }

        // Determine which table to query in
        String tableName;
        if (type.contains("WatchList")) {
            tableName = "watchlist";
        } else {
            tableName = "shoppingcart";
        }

        // Execute query
        java.sql.Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            System.out.println("executing query");
            conn = createConn();
            stmt = conn.createStatement();
            String query;
    
            if (type.contains("get")) {
                query = "SELECT itemid FROM "+tableName+" WHERE userid='"+userId+"';";
                rs = stmt.executeQuery(query);
                List<String> list = new ArrayList<String>();
                while (rs.next()) {
                    String res = rs.getString("itemid");
                    list.add(res);
                }
                return "{\"success\": true, \"itemId\": "+JSONArray.toJSONString(list)+"}";

            } else if (type.contains("add")) {
                String id = UUID.randomUUID().toString();
                query = "INSERT INTO "+tableName+" (id, userid, itemid) VALUES ('"+id+"','"+ userId +"','"+ itemId +"');";
                try {
                    stmt.executeUpdate(query);
                    return "{\"success\": true}";
                } catch (Exception e) {
                    e.printStackTrace();
                    return "{\"success\": false}";
                }
            } else if (type.contains("delete")) {
                query = "DELETE FROM "+tableName+" WHERE userid='"+userId+"' AND itemid='"+itemId+"';";
                try {
                    stmt.executeUpdate(query);
                    return "{\"success\": true}";
                } catch (Exception e) {
                    e.printStackTrace();
                    return "{\"success\": false}";
                }
            } else {  // checkout
                query = "DELETE FROM "+tableName+" WHERE userid='"+userId+"';";
                try {
                    stmt.executeUpdate(query);
                    return "{\"success\": true}";
                } catch (Exception e) {
                    e.printStackTrace();
                    return "{\"success\": false}";
                }
            }
        } catch (Exception e) {
			e.printStackTrace();
		} finally {
            try { if (rs != null) rs.close(); } catch (SQLException e) { e.printStackTrace(); }
            try { if (stmt != null) stmt.close(); } catch (SQLException e) { e.printStackTrace(); }
            try { if (conn != null) conn.close(); } catch (SQLException e) { e.printStackTrace(); }
        }
        return "";
    }

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("172.17.0.2");

        com.rabbitmq.client.Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare(RPC_QUEUE_NAME, false, false, false, null);
        channel.queuePurge(RPC_QUEUE_NAME);

        channel.basicQos(1);

        System.out.println(" [x] Awaiting RPC requests");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            AMQP.BasicProperties replyProps = new AMQP.BasicProperties
                    .Builder()
                    .correlationId(delivery.getProperties().getCorrelationId())
                    .build();

            String response = "";
            try {
                String message = new String(delivery.getBody(), "UTF-8");
                System.out.println(" [x] Receiving " + message);
                response += process(message);
            } catch (RuntimeException e) {
                System.out.println(" [.] " + e);
            } finally {
                channel.basicPublish("", delivery.getProperties().getReplyTo(), replyProps, response.getBytes("UTF-8"));
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }
        };

        channel.basicConsume(RPC_QUEUE_NAME, false, deliverCallback, (consumerTag -> {}));
    }
}