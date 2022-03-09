package coupon_project.facade;

import coupon_project.beans.Company;
import coupon_project.beans.Coupon;
import coupon_project.beans.Customer;
import coupon_project.exceptions.AdministrationException;

import java.sql.SQLException;
import java.util.ArrayList;

public class AdminFacade extends ClientFacade {

    // There is only one admin, so we can write his email and password here hardcoded
    // Admin email    - HARDCODED!!!
    private static final String ADMIN_USERNAME = "admin@admin.com";
    // Admin password - HARDCODED!!!
    private static final String ADMIN_PASSWORD = "admin";

    /**
     * Blank constructor to crate admin facade instance
     */
    public AdminFacade() {
    }

    /**
     * checks if the login arguments are correct.
     *
     * @param email    Admins email.
     * @param password admins password.
     * @return whether credentials are correct.
     */
    public boolean login(String email, String password) {
        // Checking if the given email and password matching the hardcoded email and password above
        return email.equals(ADMIN_USERNAME) && password.equals(ADMIN_PASSWORD);
    }

    /**
     * adds new company to the database.
     *
     * @param company A company object.
     * @throws SQLException            when sql raises an exception.
     * @throws InterruptedException    when thread is interrupted.
     * @throws AdministrationException when email or company name already exist.
     */
    public void addCompany(Company company) throws SQLException, InterruptedException, AdministrationException {
        // First, if the company name already used, you can't create that company
        if (companyActions.isCompanyExistsByName(company.getName())) {
            // An exception is thrown
            throw new AdministrationException("Company name already exists");
        }
        // Next, if the company email already used, you can't create that company
        if (companyActions.isCompanyExistsByEmail(company.getEmail())) {
            // An exception is thrown
            throw new AdministrationException("Company email already exists");
        }
        // If you pass all checks, the company is created
        companyActions.addCompany(company);
    }

    /**
     * updates company's data. Can only update company's email or password
     *
     * @param company Company object.
     * @throws SQLException         when sql raises SQLException.
     * @throws InterruptedException when sql raises InterruptedException.
     * @throws AdministrationException when attempting to update a non-existent company.
     */
    public void updateCompany(Company company) throws SQLException, InterruptedException, AdministrationException {
        // First, if the company name doesn't exist (=the company doesn't exist), you can't update
        if (!companyActions.isCompanyExistsByName(company.getName())) {
            // An exception is thrown
            throw new AdministrationException("Company doesn't exists");
        }
        // If you pass the check, you can update the company details (email and password)
        companyActions.updateCompany(company);
    }

    /**
     * delete company
     *
     * @param companyID company ID
     * @throws SQLException            when SQL raises SQLException
     * @throws InterruptedException    when SQL raises InterruptedException
     * @throws AdministrationException when we throw a custom exception
     */
    public void deleteCompany(int companyID) throws SQLException, InterruptedException, AdministrationException {
        // First, if the company doesn't exist (using ID number), you can't delete it
        if (!companyActions.isCompanyExistsByID(companyID)) {
            // An exception is thrown
            throw new AdministrationException("Company doesn't exists");
        }
        // Next, we make a list of all the company coupons, so we can delete their purchases
        ArrayList<Coupon> couponArrayList = couponActions.getAllCompanyCoupons(companyID);
        // For every coupon on the list, we delete all its purchases
        for (Coupon coupon : couponArrayList) {
            // Deleting purchases
            purchaseActions.deleteAllPurchasesByCoupon(coupon.getId());
        }
        // Next, we delete all the coupons
        couponActions.deleteAllCompanyCoupons(companyID);
        // And finally we delete the company
        companyActions.deleteCompany(companyID);
    }

    /**
     * returning a list of all the companies
     *
     * @return list of all companies
     * @throws SQLException         when SQL throw SQLException
     * @throws InterruptedException when SQL throw InterruptedException
     */
    public ArrayList<Company> getAllCompanies() throws SQLException, InterruptedException {
        // Return a list of all the companies
        return companyActions.getAllCompany();
    }

    /**
     * returning one company using its ID number
     *
     * @param companyID wanted company ID
     * @return wanted company
     * @throws SQLException            when SQL throw SQLException
     * @throws InterruptedException    when SQL throw InterruptedException
     * @throws AdministrationException when we throw custom exception
     */
    public Company getOneCompany(int companyID) throws SQLException, InterruptedException, AdministrationException {
        // First, we check if the company even exist (by ID number)
        if (!companyActions.isCompanyExistsByID(companyID)) {
            // An exception is thrown
            throw new AdministrationException("Company doesn't exists");
        }
        // If the company exists, it returns one company who match the ID (max 1 on the table)
        return companyActions.getOneCompany(companyID);
    }

    /**
     * adds new customer to the database
     *
     * @param customer a customer object
     * @throws SQLException            when SQL throw SQLException
     * @throws InterruptedException    when SQL throw InterruptedException
     * @throws AdministrationException when we throw custom exception
     */
    public void addCustomer(Customer customer) throws SQLException, InterruptedException, AdministrationException {
        // First, if the email is already used, you can't create that customer
        if (customerActions.isCustomerExistsByEmail(customer.getEmail())) {
            // An exception is thrown
            throw new AdministrationException("Customer email already exists");
        }
        // If the email isn't used, the customer is created
        customerActions.addCustomer(customer);
    }

    /**
     * updates customer data. can only update customer's first name, last name and password
     *
     * @param customer customer object
     * @throws SQLException            when SQL throw SQLException
     * @throws InterruptedException    when SQL throw InterruptedException
     * @throws AdministrationException when we throw custom exception
     */
    public void updateCustomer(Customer customer) throws SQLException, InterruptedException, AdministrationException {
        // First, if the customer email doesn't exist (=the customer doesn't exist), you can't update
        if (!customerActions.isCustomerExistsByEmail(customer.getEmail())) {
            // An exception is thrown
            throw new AdministrationException("This customer doesn't exists");
        }
        // If the customer exists, you can update his details (first name, last name, password)
        customerActions.updateCustomer(customer);
    }

    /**
     * delete customer
     *
     * @param customerID customer ID
     * @throws SQLException            when SQL throw SQLException
     * @throws InterruptedException    when SQL throw InterruptedException
     * @throws AdministrationException when we throw custom exception
     */
    public void deleteCustomer(int customerID) throws SQLException, InterruptedException, AdministrationException {
        // First, if the customer doesn't exist (using ID number), you can't delete him
        if (!customerActions.isCustomerExistsByID(customerID)) {
            // An exception is thrown
            throw new AdministrationException("This customer doesn't exists");
        }
        // Next, we create a list of all the coupons he bought
        ArrayList<Coupon> coupons = purchaseActions.getAllCustomerCoupons(customerID);
        // And for each one, we raise its amount by 1
        for (Coupon coupon : coupons) {
            // Raising its amount
            couponActions.increaseCouponAmount(coupon.getId());
        }
        // Next, we delete all his purchases
        purchaseActions.deleteAllPurchasesByCustomer(customerID);
        // And finally we delete the customer
        customerActions.deleteCustomer(customerID);
    }

    /**
     * returning the list of all the customers
     *
     * @return customers list
     * @throws SQLException         when SQL throw SQLException
     * @throws InterruptedException when SQL throw InterruptedException
     */
    public ArrayList<Customer> getAllCustomers() throws SQLException, InterruptedException {
        // Return the customers list
        return customerActions.getAllCustomers();
    }

    /**
     * returning one customer using his ID number
     *
     * @param customerID wanted customer ID
     * @return wanted customer
     * @throws SQLException            when SQL throw SQLException
     * @throws InterruptedException    when SQL throw InterruptedException
     * @throws AdministrationException when we throw custom exception
     */
    public Customer getOneCustomer(int customerID) throws SQLException, InterruptedException, AdministrationException {
        // First, we check if the customer even exist (by ID number)
        if (!customerActions.isCustomerExistsByID(customerID)) {
            // An exception is thrown
            throw new AdministrationException("This customer doesn't exists");
        }
        // If the customer exists, return him
        return customerActions.getOneCustomer(customerID);
    }
}
