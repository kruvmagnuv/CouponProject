package coupon_project.db_dao;

import coupon_project.beans.Company;
import coupon_project.dao.CompaniesDAO;
import coupon_project.db_util.DatabaseUtils;
import coupon_project.db_util.Factory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class CompaniesDBDAO implements CompaniesDAO {

    private final String IS_COMPANY_EXISTS = "SELECT COUNT(*) AS total " +
            "FROM coupon_project.companies " +
            "WHERE email=? AND password=?";

    private final String IS_COMPANY_EXISTS_BY_ID = "SELECT COUNT(*) AS total " +
            "FROM coupon_project.companies " +
            "WHERE id=?";

    private final String ADD_COMPANY = "INSERT " +
            "INTO coupon_project.companies " +
            "(`name`,`email`,`password`) " +
            "VALUES (?,?,?)";

    private final String UPDATE_COMPANY = "UPDATE " +
            "coupon_project.companies " +
            "SET email=?, password=? " +
            "WHERE name=?";

    private final String DELETE_COMPANY = "DELETE " +
            "FROM coupon_project.companies " +
            "WHERE id=?";

    private final String GET_ALL_COMPANY = "SELECT * " +
            "FROM coupon_project.companies";

    private final String GET_ONE_COMPANY = "SELECT * " +
            "FROM coupon_project.companies " +
            "WHERE id=?";

    private final String GET_COMPANY_ID_BY_EMAIL = "SELECT id " +
            "FROM coupon_project.companies " +
            "WHERE email=?";

    private final String IS_COMPANY_EXISTS_BY_NAME = "SELECT COUNT(*) AS total " +
            "FROM coupon_project.companies " +
            "WHERE name=?";

    private final String IS_COMPANY_EXISTS_BY_EMAIL = "SELECT COUNT(*) AS total " +
            "FROM coupon_project.companies " +
            "WHERE email=?";

    @Override
    public boolean isCompanyExists(String email, String password) throws SQLException, InterruptedException {
        // Creates a map collection to replace "?" on the statement. Key--> number of "?", Value--> value (the replacement)
        Map<Integer, Object> params = new HashMap<>();
        // Adding all the replacement values and their order
        params.put(1, email);
        params.put(2, password);
        // Running the statement and getting a ResultSet
        ResultSet resultSet = DatabaseUtils.runQueryForResult(IS_COMPANY_EXISTS, params);
        // Moving for the first line of the ResultSet
        resultSet.next();
        // Returning whether it counts more than 0 matching values (=exist)
        return resultSet.getInt("total") > 0;
    }

    @Override
    public boolean isCompanyExistsByID(int companyID) throws SQLException, InterruptedException {
        // Creates a map collection to replace "?" on the statement. Key--> number of "?", Value--> value (the replacement)
        Map<Integer, Object> params = new HashMap<>();
        // Adding all the replacement values and their order
        params.put(1, companyID);
        // Running the statement and getting a ResultSet
        ResultSet resultSet = DatabaseUtils.runQueryForResult(IS_COMPANY_EXISTS_BY_ID, params);
        // Moving for the first line of the ResultSet
        resultSet.next();
        // Returning whether it counts more than 0 matching values (=exist)
        return resultSet.getInt("total") > 0;
    }

    @Override
    public void addCompany(Company company) throws InterruptedException {
        // Creates a map collection to replace "?" on the statement. Key--> number of "?", Value--> value (the replacement)
        Map<Integer, Object> params = new HashMap<>();
        // Adding all the replacement values and their order
        params.put(1, company.getName());
        params.put(2, company.getEmail());
        params.put(3, company.getPassword());
        // Running the statement
        DatabaseUtils.runQuery(ADD_COMPANY, params);
    }

    @Override
    public void updateCompany(Company company) throws InterruptedException {
        // Creates a map collection to replace "?" on the statement. Key--> number of "?", Value--> value (the replacement)
        Map<Integer, Object> params = new HashMap<>();
        // Adding all the replacement values and their order
        params.put(1, company.getEmail());
        params.put(2, company.getPassword());
        params.put(3, company.getName());
        // Running the statement
        DatabaseUtils.runQuery(UPDATE_COMPANY, params);
    }

    @Override
    public void deleteCompany(int companyId) throws InterruptedException {
        // Creates a map collection to replace "?" on the statement. Key--> number of "?", Value--> value (the replacement)
        Map<Integer, Object> params = new HashMap<>();
        // Adding all the replacement values and their order
        params.put(1, companyId);
        // Running the statement
        DatabaseUtils.runQuery(DELETE_COMPANY, params);
    }


    @Override
    public ArrayList<Company> getAllCompany() throws SQLException, InterruptedException {
        // Running the statement and getting a ResultSet
        ResultSet resultSet = DatabaseUtils.runQueryForResult(GET_ALL_COMPANY);
        // Creating a blank list to fill
        ArrayList<Company> companyList = new ArrayList<>();
        // For every line on the ResultSet
        while (resultSet.next()) {
            // Creating a blank company to fill and add to the list
            Company company = new Company();
            // Changing its email
            company.setEmail(resultSet.getString("email"));
            // Changing its ID
            company.setId(resultSet.getInt("id"));
            // Changing its name
            company.setName(resultSet.getString("name"));
            // Changing its password
            company.setPassword(resultSet.getString("password"));
            // Changing its coupons list
            company.setCoupons(Factory.getCouponDAO("sql").getAllCompanyCoupons(resultSet.getInt("id")));
            // Adding the filled company to the list from above
            companyList.add(company);
        }
        // Return the wanted list
        return companyList;
    }


    @Override
    public Company getOneCompany(int companyId) throws SQLException, InterruptedException {
        // Creates a map collection to replace "?" on the statement. Key--> number of "?", Value--> value (the replacement)
        Map<Integer, Object> params = new HashMap<>();
        // Adding all the replacement values and their order
        params.put(1, companyId);
        // Running the statement and getting a ResultSet
        ResultSet resultSet = DatabaseUtils.runQueryForResult(GET_ONE_COMPANY, params);
        // Moving for the first line of the ResultSet
        resultSet.next();
        // Creating a blank company to fill
        Company company = new Company();
        // Changing its ID
        company.setId(resultSet.getInt("id"));
        // Changing its password
        company.setPassword(resultSet.getString("password"));
        // Changing its email
        company.setEmail(resultSet.getString("email"));
        // Changing its name
        company.setName(resultSet.getString("name"));
        // Changing its coupons list
        company.setCoupons(Factory.getCouponDAO("sql").getAllCompanyCoupons(resultSet.getInt("id")));
        // Return the wanted company
        return company;
    }


    @Override
    public int getCompanyIDbyEmail(String email) throws SQLException, InterruptedException {
        // Creates a map collection to replace "?" on the statement. Key--> number of "?", Value--> value (the replacement)
        Map<Integer, Object> params = new HashMap<>();
        // Adding all the replacement values and their order
        params.put(1, email);
        // Running the statement and getting a ResultSet
        ResultSet resultSet = DatabaseUtils.runQueryForResult(GET_COMPANY_ID_BY_EMAIL, params);
        // Moving for the first line of the ResultSet
        resultSet.next();
        // Return the wanted ID
        return resultSet.getInt("id");
    }

    @Override
    public boolean isCompanyExistsByName(String name) throws SQLException, InterruptedException {
        // Creates a map collection to replace "?" on the statement. Key--> number of "?", Value--> value (the replacement)
        Map<Integer, Object> params = new HashMap<>();
        // Adding all the replacement values and their order
        params.put(1, name);
        // Running the statement and getting a ResultSet
        ResultSet resultSet = DatabaseUtils.runQueryForResult(IS_COMPANY_EXISTS_BY_NAME, params);
        // Moving for the first line of the ResultSet
        resultSet.next();
        // Returning whether it counts more than 0 matching values (=exist)
        return resultSet.getInt("total")>0;
    }

    @Override
    public boolean isCompanyExistsByEmail(String email) throws SQLException, InterruptedException {
        // Creates a map collection to replace "?" on the statement. Key--> number of "?", Value--> value (the replacement)
        Map<Integer, Object> params = new HashMap<>();
        // Adding all the replacement values and their order
        params.put(1, email);
        // Running the statement and getting a ResultSet
        ResultSet resultSet = DatabaseUtils.runQueryForResult(IS_COMPANY_EXISTS_BY_EMAIL, params);
        // Moving for the first line of the ResultSet
        resultSet.next();
        // Returning whether it counts more than 0 matching values (=exist)
        return resultSet.getInt("total") > 0;
    }
}
