package coupon_project.dao;

import coupon_project.beans.Customer;

import java.util.ArrayList;

public interface CustomersDAO {
    public boolean isCustomerExists(String email, String password);

    public void addCustomer(Customer customer);

    public void updateCustomer(Customer customer);

    public void deleteCustomer(int customerID);

    public ArrayList<Customer> getAllCustomers();

    public Customer getOneCustomer(int customerID);
}
