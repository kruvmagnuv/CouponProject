package coupon_project.db_dao;

import coupon_project.beans.Customer;
import coupon_project.dao.CustomersDAO;
import coupon_project.db_util.DatabaseUtils;
import coupon_project.db_util.Factory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class CustomersDBDAO implements CustomersDAO {

    private final String IS_CUSTOMER_EXISTS = "SELECT COUNT(*) AS total " +
            "FROM coupon_project.customers " +
            "WHERE email=? AND password=?";

    private final String IS_CUSTOMER_EXISTS_BY_ID = "SELECT COUNT(*) AS total " +
            "FROM coupon_project.customers " +
            "WHERE id=?";

    private final String ADD_CUSTOMER = "INSERT " +
            "INTO coupon_project.customers " +
            "(`email`,`password`,`first_name`, `last_name`) " +
            "VALUES (?,?,?,?)";

    private final String UPDATE_CUSTOMER = "UPDATE " +
            "coupon_project.customers " +
            "SET first_name=?, last_name=?, password=? " +
            "WHERE email=?";

    private final String DELETE_CUSTOMER = "DELETE " +
            "FROM coupon_project.customers " +
            "WHERE id=?";

    private final String GET_ALL_CUSTOMERS = "SELECT * " +
            "FROM coupon_project.customers";

    private final String GET_ONE_CUSTOMER = "SELECT * " +
            "FROM coupon_project.customers " +
            "WHERE id=?";

    private final String GET_CUSTOMER_ID_BY_EMAIL = "SELECT id " +
            "FROM coupon_project.customers " +
            "WHERE email=?";

    private final String IS_CUSTOMER_EXISTS_BY_EMAIL = "SELECT COUNT(*) AS total " +
            "FROM coupon_project.customers " +
            "WHERE email=?";

    @Override
    public boolean isCustomerExists(String email, String password) throws SQLException, InterruptedException {
        // Creates a map collection to replace "?" on the statement. Key--> number of "?", Value--> value (the replacement)
        Map<Integer, Object> params = new HashMap<>();
        // Adding all the replacement values and their order
        params.put(1, email);
        params.put(2, password);
        // Running the statement and getting a ResultSet
        ResultSet resultSet = DatabaseUtils.runQueryForResult(IS_CUSTOMER_EXISTS, params);
        // Moving for the first line of the ResultSet
        resultSet.next();
        // Returning whether it counts more than 0 matching values (=exist)
        return resultSet.getInt("total") > 0;
    }

    @Override
    public boolean isCustomerExistsByID(int customerID) throws SQLException, InterruptedException {
        // Creates a map collection to replace "?" on the statement. Key--> number of "?", Value--> value (the replacement)
        Map<Integer, Object> params = new HashMap<>();
        // Adding all the replacement values and their order
        params.put(1, customerID);
        // Running the statement and getting a ResultSet
        ResultSet resultSet = DatabaseUtils.runQueryForResult(IS_CUSTOMER_EXISTS_BY_ID, params);
        // Moving for the first line of the ResultSet
        resultSet.next();
        // Returning whether it counts more than 0 matching values (=exist)
        return resultSet.getInt("total") > 0;
    }

    @Override
    public void addCustomer(Customer customer) throws InterruptedException {
        // Creates a map collection to replace "?" on the statement. Key--> number of "?", Value--> value (the replacement)
        Map<Integer, Object> params = new HashMap<>();
        // Adding all the replacement values and their order
        params.put(1, customer.getEmail());
        params.put(2, customer.getPassword());
        params.put(3, customer.getFirstName());
        params.put(4, customer.getLastName());
        // Running the statement
        DatabaseUtils.runQuery(ADD_CUSTOMER, params);
    }

    @Override
    public void updateCustomer(Customer customer) throws InterruptedException {
        // Creates a map collection to replace "?" on the statement. Key--> number of "?", Value--> value (the replacement)
        Map<Integer, Object> params = new HashMap<>();
        // Adding all the replacement values and their order
        params.put(1, customer.getFirstName());
        params.put(2, customer.getLastName());
        params.put(3, customer.getPassword());
        params.put(4, customer.getEmail());
        // Running the statement
        DatabaseUtils.runQuery(UPDATE_CUSTOMER, params);
    }

    @Override
    public void deleteCustomer(int customerID) throws InterruptedException {
        // Creates a map collection to replace "?" on the statement. Key--> number of "?", Value--> value (the replacement)
        Map<Integer, Object> params = new HashMap<>();
        // Adding all the replacement values and their order
        params.put(1, customerID);
        // Running the statement
        DatabaseUtils.runQuery(DELETE_CUSTOMER, params);
    }

    @Override
    public ArrayList<Customer> getAllCustomers() throws SQLException, InterruptedException {
        // Running the statement and getting a ResultSet
        ResultSet resultSet = DatabaseUtils.runQueryForResult(GET_ALL_CUSTOMERS);
        // Creating a blank list to fill
        ArrayList<Customer> customerList = new ArrayList<>();
        // For every line on the ResultSet
        while (resultSet.next()) {
            // Creating a blank customer to fill
            Customer customer = new Customer();
            // Changing its email
            customer.setEmail(resultSet.getString("email"));
            // Changing its ID
            customer.setId(resultSet.getInt("id"));
            // Changing it's first name
            customer.setFirstName(resultSet.getString("first_name"));
            // Changing it's surname
            customer.setLastName(resultSet.getString("last_name"));
            // Changing its password
            customer.setPassword(resultSet.getString("password"));
            // Changing its coupons list
            customer.setCoupons(Factory.getCustomerVsCouponDAO("sql").getAllCustomerCoupons(resultSet.getInt("id")));
            // Adding the filled customer to the list above
            customerList.add(customer);
        }
        // Return the wanted list
        return customerList;
    }

    @Override
    public Customer getOneCustomer(int customerID) throws SQLException, InterruptedException {
        // Creates a map collection to replace "?" on the statement. Key--> number of "?", Value--> value (the replacement)
        Map<Integer, Object> params = new HashMap<>();
        // Adding all the replacement values and their order
        params.put(1, customerID);
        // Running the statement and getting a ResultSet
        ResultSet resultSet = DatabaseUtils.runQueryForResult(GET_ONE_CUSTOMER, params);
        // Moving for the first line of the ResultSet
        resultSet.next();
        // Creating a blank customer to fill
        Customer customer = new Customer();
        // Changing its email
        customer.setEmail(resultSet.getString("email"));
        // Changing its ID
        customer.setId(resultSet.getInt("id"));
        // Changing it's first name
        customer.setFirstName(resultSet.getString("first_name"));
        // Changing it's surname
        customer.setLastName(resultSet.getString("last_name"));
        // Changing its password
        customer.setPassword(resultSet.getString("password"));
        // Changing its coupon list
        customer.setCoupons(Factory.getCustomerVsCouponDAO("sql").getAllCustomerCoupons(resultSet.getInt("id")));
        // Return the wanted customer
        return customer;
    }

    @Override
    public int getCustomerIDbyEmail(String email) throws SQLException, InterruptedException {
        // Creates a map collection to replace "?" on the statement. Key--> number of "?", Value--> value (the replacement)
        Map<Integer, Object> params = new HashMap<>();
        // Adding all the replacement values and their order
        params.put(1, email);
        // Running the statement and getting a ResultSet
        ResultSet resultSet = DatabaseUtils.runQueryForResult(GET_CUSTOMER_ID_BY_EMAIL, params);
        // Moving for the first line of the ResultSet
        resultSet.next();
        // Return the wanted ID
        return resultSet.getInt("id");
    }

    @Override
    public boolean isCustomerExistsByEmail(String email) throws SQLException, InterruptedException {
        // Creates a map collection to replace "?" on the statement. Key--> number of "?", Value--> value (the replacement)
        Map<Integer, Object> params = new HashMap<>();
        // Adding all the replacement values and their order
        params.put(1, email);
        // Running the statement and getting a ResultSet
        ResultSet resultSet = DatabaseUtils.runQueryForResult(IS_CUSTOMER_EXISTS_BY_EMAIL, params);
        // Moving for the first line of the ResultSet
        resultSet.next();
        // Returning whether it counts more than 0 matching values (=exist)
        return resultSet.getInt("total") > 0;
    }
}
