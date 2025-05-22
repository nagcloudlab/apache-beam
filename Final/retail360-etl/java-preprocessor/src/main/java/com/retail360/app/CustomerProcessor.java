package com.retail360.app;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVReader;
import com.retail360.model.Customer;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

public class CustomerProcessor {

    private static final Logger logger = LoggerFactory.getLogger(CustomerProcessor.class);


    // Add inside class CustomerProcessor
    public static boolean isValidEmail(String email) {
        return email != null && email.matches("^[\\w.-]+@[\\w.-]+\\.[a-zA-Z]{2,6}$");
    }

    public static void postToOracleAPI(Customer customer) {
        String endpoint = "https://webhook.site/a5449117-e5b6-47c2-97f4-2c622a76b7e3"; // Replace with your actual link
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpPost post = new HttpPost(endpoint);
            post.setHeader("Content-Type", "application/json");

            ObjectMapper mapper = new ObjectMapper();
            String json = mapper.writeValueAsString(customer);
            post.setEntity(new StringEntity(json));

            var response = client.execute(post);
            logger.info("📤 Posted to API. Status: {}", response.getCode());
        } catch (Exception e) {
            logger.error("❌ Failed to POST: {}", e.getMessage());
        }
    }

    public static void writeCleanedCSV(List<Customer> customers, String outputPath) {
        try (PrintWriter writer = new PrintWriter(new FileWriter(outputPath))) {
            writer.println("customer_id,first_name,last_name,email,country,status"); // header
            for (Customer c : customers) {
                writer.println(c.toString()); // uses Customer.toString()
            }
            logger.info("📁 Cleaned CSV written to: {}", outputPath);
        } catch (Exception e) {
            logger.error("❌ Failed to write cleaned CSV: {}", e.getMessage(), e);
        }
    }

    public static List<Customer> loadCustomersFromCSV(String fileName) {
        List<Customer> customers = new ArrayList<>();

        try (
                InputStream is = CustomerProcessor.class.getClassLoader().getResourceAsStream(fileName);
                InputStreamReader isr = new InputStreamReader(is);
                CSVReader reader = new CSVReader(isr)
        ) {
            String[] nextLine;
            reader.readNext(); // skip header
            while ((nextLine = reader.readNext()) != null) {
                Customer customer = new Customer(
                        nextLine[0], nextLine[1], nextLine[2],
                        nextLine[3], nextLine[4], nextLine[5]
                );
                customers.add(customer);
            }
        } catch (Exception e) {
            logger.error("Error reading CSV: {}", e.getMessage(), e);
        }

        return customers;
    }

    public static void main(String[] args) {


        List<Customer> customers = loadCustomersFromCSV("customers.csv");

        //
        List<Customer> cleaned = customers.stream()
                .filter(c -> "ACTIVE".equalsIgnoreCase(c.getStatus()))
                .filter(c -> isValidEmail(c.getEmail()))
                .toList();

        //cleaned.forEach(c -> logger.info("✅ Valid: {}", c));
        String outputPath = "output/cleaned_customers.csv"; // relative path

        cleaned.forEach(c -> {
            logger.info("✅ Valid: {}", c);
            postToOracleAPI(c);  // 💥 Send to API
        });

        writeCleanedCSV(cleaned, outputPath); // ✅ Save to file

    }


}
