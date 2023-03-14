package com.face.script;

import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.simple.JSONObject;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;

import io.github.cdimascio.dotenv.Dotenv;

public class ETLThread implements Runnable {

    static Dotenv dotenv = Dotenv.load();
    static long startTime = System.currentTimeMillis();
    private Connection con;
    private Connection postCon;
    static Map<String, double[]> customerDataArray = Collections.synchronizedMap(new HashMap<>());
    static List<String> nullData = Collections.synchronizedList(new ArrayList<>());
    static List<String> cannotEncode = Collections.synchronizedList(new ArrayList<>());
    public int offset;
    public int fetch;
    int count;

    public ETLThread() {
    }

    public ETLThread(int offset, int fetch) {
        this.offset = offset;
        this.fetch = fetch;
    }

    static final String authToken = "Basic " + Base64.getEncoder().encodeToString(
            (dotenv.get("elastic_user") + ":" + dotenv.get("elastic_password")).getBytes());

    @Override
    public void run() {
        try {
            executeProgram();
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                this.con.close();
                this.postCon.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }

    public void executeProgram() throws ClassNotFoundException, SQLException {
        Class.forName("oracle.jdbc.OracleDriver");
        this.con = DriverManager.getConnection(
                dotenv.get("db_url"), dotenv.get("db_user"), dotenv.get("db_password"));

        this.postCon = DriverManager.getConnection(
                dotenv.get("postgres_url"), dotenv.get("postgres_user"), dotenv.get("postgres_password"));

        Map<String, String> customerList = new HashMap<>();
        customerList = getCustNumFromDatabase(offset, fetch);
        List<String> custNumbers = new ArrayList<>(customerList.keySet());

        for (int i = 0; i < custNumbers.size(); i++) {
            PreparedStatement pCount = postCon
                    .prepareStatement("select count(*) from migration_logs where customer_number =?");
            pCount.setString(1, custNumbers.get(i));
            ResultSet rsCount = pCount.executeQuery();
            rsCount.next();
            count = rsCount.getInt(1);

            if (count < 1) {
                encodeAndInsertToES(custNumbers, i, customerList);
            } else {
                System.out.println("[INFO] Data Exists for: " + custNumbers.get(i));
            }
        }

    }

    static double[] convertToArray(JSONArray jsonArray) {

        double[] fData = new double[jsonArray.length()];
        for (int i = 0; i < jsonArray.length(); i++) {
            try {
                fData[i] = jsonArray.getDouble(i);
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }
        return fData;
    }

    public byte[] getByteDataFromBlob(Blob blob) {
        if (blob != null) {
            try {
                return blob.getBytes(1, (int) blob.length());
            } catch (SQLException ex) {
                System.out.println(ex);
            }
        }
        return null;
    }

    public Map<String, String> getCustNumFromDatabase(int offset, int fetch)
            throws SQLException, ClassNotFoundException {
        Map<String, String> custNumberList = new HashMap<>();
        java.sql.Statement stmt = this.con.createStatement();
        ResultSet rs = ((java.sql.Statement) stmt).executeQuery(
                dotenv.get("db_query") + " OFFSET " + offset + " ROWS FETCH FIRST " + fetch + " ROWS ONLY");

        while (rs.next()) {
            custNumberList.put(rs.getString("CUST_NO"), rs.getString("CUST_NAME"));

        }
        return custNumberList;

    }

    public void encodeAndInsertToES(List<String> custNumbers, int i, Map<String, String> customerList)
            throws SQLException, ClassNotFoundException {
        PreparedStatement smt = (PreparedStatement) con
                .prepareStatement("select CUST_IMAGE from MEMBER_IMAGE where CUST_NO= ? ");
        ((PreparedStatement) smt).setString(1, custNumbers.get(i));
        ResultSet rs = ((PreparedStatement) smt).executeQuery();
        PreparedStatement pstmt;

        String pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
        String date = simpleDateFormat.format(new Date());

        ZonedDateTime dateTime = ZonedDateTime.now();
        String currentDateTime = dateTime.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);

        while (rs.next()) {
            try {
                String base64 = Base64.getEncoder().encodeToString(getByteDataFromBlob(rs.getBlob("CUST_IMAGE")));
                JSONObject obj = new JSONObject();
                obj.put("image", base64);

                try {
                    HttpResponse<JsonNode> response = Unirest.post(dotenv.get("api_url"))
                            .header("Content-Type", "application/json")
                            .body(obj.toJSONString())
                            .asJson();

                    JSONArray encodings = new JSONArray();
                    encodings = response.getBody().getObject().getJSONObject("data").getJSONArray("encodings");
                    double imageData[];
                    imageData = convertToArray(encodings);
                    customerDataArray.put(custNumbers.get(i), imageData);

                    JSONObject payload = new JSONObject();
                    payload.put("name", customerList.get(custNumbers.get(i)));
                    JSONObject custObj = new JSONObject();
                    custObj.put("customerNumber", custNumbers.get(i));
                    custObj.put("faceEncodings", new JSONArray(imageData));
                    custObj.put("payload", payload);
                    custObj.put("createdAt", currentDateTime);
                    custObj.put("createdBy", "Admin");
                    try {
                        kong.unirest.Unirest.config().verifySsl(false);
                        kong.unirest.Unirest.post(dotenv.get("elasitc_api"))
                                .header("Authorization", authToken)
                                .header("Content-Type", "application/json")
                                .body(custObj.toJSONString())
                                .asString();

                        pstmt = postCon.prepareStatement(
                                "insert into migration_logs (customer_number, status, created_at, message) VALUES (?,  ?, ?, ?)");
                        pstmt.setString(1, custNumbers.get(i));
                        pstmt.setString(2, "SUCCESS");
                        pstmt.setString(3, date);
                        pstmt.setString(4, " ");
                        pstmt.execute();
                        pstmt.close();
                        System.out.println("[INFO] Data inserted for: " + custNumbers.get(i));

                    } catch (Exception ex) {
                        try {
                            pstmt = postCon.prepareStatement(
                                    "insert into migration_logs (customer_number, status, created_at, message) VALUES (?, ?, ?, ?)");
                            pstmt.setString(1, custNumbers.get(i));
                            pstmt.setString(2, "FAILURE");
                            pstmt.setString(3, date);
                            pstmt.setString(4, ex.toString());
                            pstmt.execute();
                            pstmt.close();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        System.out.println("[INFO] Can not insert data for: " + custNumbers.get(i));
                    }

                } catch (Exception ex) {
                    System.out.println("[ERROR] Can not encode data for: " + custNumbers.get(i));
                    cannotEncode.add(custNumbers.get(i));
                    try {
                        pstmt = postCon.prepareStatement(
                                "insert into migration_logs (customer_number, status, created_at, message) VALUES (?,  ?, ?, ?)");
                        pstmt.setString(1, custNumbers.get(i));
                        pstmt.setString(2, "FAILURE");
                        pstmt.setString(3, date);
                        pstmt.setString(4, ex.toString());
                        pstmt.execute();
                        pstmt.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

            } catch (Exception ex) {
                System.out.println("[ERROR] No data found for: " + custNumbers.get(i));
                nullData.add(custNumbers.get(i));
                try {
                    pstmt = postCon.prepareStatement(
                            "insert into migration_logs (customer_number, status, created_at, message) VALUES (?,  ?, ?, ?)");
                    pstmt.setString(1, custNumbers.get(i));
                    pstmt.setString(2, "EMPTY");
                    pstmt.setString(3, date);
                    pstmt.setString(4, ex.toString());
                    pstmt.execute();
                    pstmt.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        rs.close();
        smt.close();

    }

}
