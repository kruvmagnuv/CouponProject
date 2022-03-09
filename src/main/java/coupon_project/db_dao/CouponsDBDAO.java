package coupon_project.db_dao;

import coupon_project.beans.Category;
import coupon_project.beans.Coupon;
import coupon_project.dao.CouponsDAO;
import coupon_project.db_util.DatabaseUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;

public class CouponsDBDAO implements CouponsDAO {

    private final String ADD_COUPON = "INSERT " +
            "INTO coupon_project.coupons " +
            "(`amount`,`category_id`,`company_id`, `description`, `start_date`, `end_date`, `title`, `image`, `price`,`id`) " +
            "VALUES (?,?,?,?,?,?,?,?,?,?)";

    private final String UPDATE_COUPON = "UPDATE " +
            "coupon_project.coupons " +
            "SET category_id=?, description=?, start_date=?, end_date=?, amount=?, price=?, image=? " +
            "WHERE title=?";

    private final String DELETE_COUPON = "DELETE " +
            "FROM coupon_project.coupons " +
            "WHERE id=?";

    private final String GET_ALL_COUPONS = "SELECT * " +
            "FROM coupon_project.coupons";

    private final String GET_ONE_COUPON = "SELECT * " +
            "FROM coupon_project.coupons " +
            "WHERE id=?";

    private final String IS_COUPON_LEFT = "SELECT amount " +
            "FROM coupon_project.coupons " +
            "WHERE id=?";

    private final String IS_COUPON_EXISTS = "SELECT COUNT(*) AS total " +
            "FROM coupon_project.coupons " +
            "WHERE id=?";

    private final String GET_ALL_COMPANY_COUPONS = "SELECT * " +
            "FROM coupon_project.coupons " +
            "WHERE company_id=?";

    private final String GET_COMPANY_COUPONS_BY_CATEGORY = "SELECT * " +
            "FROM coupon_project.coupons " +
            "WHERE company_id=? AND category_id=?";

    private final String GET_COMPANY_COUPONS_TILL_MAX_PRICE = "SELECT * " +
            "FROM coupon_project.coupons " +
            "WHERE company_id=? AND price<=? ";

    private final String DECREASE_COUPON_AMOUNT = "UPDATE " +
            "coupon_project.coupons " +
            "SET amount = amount-1 " +
            "WHERE id=?";

    private final String INCREASE_COUPON_AMOUNT = "UPDATE " +
            "coupon_project.coupons " +
            "SET amount = amount+1 " +
            "WHERE id=?";

    private final String DELETE_ALL_COMPANY_COUPONS = "DELETE " +
            "FROM coupon_project.coupons " +
            "WHERE company_id=?";

    private final String IS_COUPON_EXISTS_BY_NAME_FOR_COMPANY = "SELECT COUNT(*) as total " +
            "FROM coupon_project.coupons " +
            "WHERE company_id=? AND title=?";

    @Override
    public void addCoupon(Coupon coupon) throws InterruptedException {
        // Creates a map collection to replace "?" on the statement. Key--> number of "?", Value--> value (the replacement)
        Map<Integer, Object> params = new HashMap<>();
        // Adding all the replacement values and their order
        params.put(1, coupon.getAmount());
        params.put(2, coupon.getCategory().ordinal() + 1);
        params.put(3, coupon.getCompanyID());
        params.put(4, coupon.getDescription());
        params.put(5, coupon.getStartDate());
        params.put(6, coupon.getEndDate());
        params.put(7, coupon.getTitle());
        params.put(8, coupon.getImage());
        params.put(9, coupon.getPrice());
        params.put(10, coupon.getId());
        // Running the statement
        DatabaseUtils.runQuery(ADD_COUPON, params);
    }


    @Override
    public void updateCoupon(Coupon coupon) throws InterruptedException {
        // Creates a map collection to replace "?" on the statement. Key--> number of "?", Value--> value (the replacement)
        Map<Integer, Object> params = new HashMap<>();
        // Adding all the replacement values and their order
        params.put(1, coupon.getCategory().ordinal() + 1);
        params.put(2, coupon.getDescription());
        params.put(3, coupon.getStartDate());
        params.put(4, coupon.getEndDate());
        params.put(5, coupon.getAmount());
        params.put(6, coupon.getPrice());
        params.put(7, coupon.getImage());
        params.put(8, coupon.getTitle());
        // Running the statement
        DatabaseUtils.runQuery(UPDATE_COUPON, params);
    }


    @Override
    public void deleteCoupon(int couponID) throws InterruptedException {
        // Creates a map collection to replace "?" on the statement. Key--> number of "?", Value--> value (the replacement)
        Map<Integer, Object> params = new HashMap<>();
        // Adding all the replacement values and their order
        params.put(1, couponID);
        // Running the statement
        DatabaseUtils.runQuery(DELETE_COUPON, params);
    }


    @Override
    public ArrayList<Coupon> getAllCoupons() throws SQLException, InterruptedException {
        // Running the statement and getting a ResultSet
        ResultSet resultSet = DatabaseUtils.runQueryForResult(GET_ALL_COUPONS);
        // Creating a blank list to fill
        ArrayList<Coupon> couponsList = new ArrayList<>();
        // For every line on the ResultSet
        while (resultSet.next()) {
            // Creating a blank coupon to fill and add to the list above
            Coupon coupon = new Coupon();
            // Changing its amount
            coupon.setAmount(resultSet.getInt("amount"));
            // Changing its category
            coupon.setCategory(Category.values()[resultSet.getInt("category_id")]);
            // Changing its ID
            coupon.setId(resultSet.getInt("id"));
            // Changing its company ID
            coupon.setCompanyID(resultSet.getInt("company_id"));
            // Changing its description
            coupon.setDescription(resultSet.getString("description"));
            // Changing its end date
            coupon.setEndDate(resultSet.getDate("end_date"));
            // Changing its start date
            coupon.setStartDate(resultSet.getDate("start_date"));
            // Changing its image url
            coupon.setImage(resultSet.getString("image"));
            // Changing its price
            coupon.setPrice(resultSet.getDouble("price"));
            // Changing its title
            coupon.setTitle(resultSet.getString("title"));
            // Adding the filled coupon to the list from above
            couponsList.add(coupon);
        }
        // Return the wanted coupon list
        return couponsList;
    }


    @Override
    public Coupon getOneCoupon(int couponID) throws SQLException, InterruptedException {
        // Creates a map collection to replace "?" on the statement. Key--> number of "?", Value--> value (the replacement)
        Map<Integer, Object> params = new HashMap<>();
        // Adding all the replacement values and their order
        params.put(1, couponID);
        // Running the statement and getting a ResultSet
        ResultSet resultSet = DatabaseUtils.runQueryForResult(GET_ONE_COUPON, params);
        // Moving for the first line of the ResultSet
        resultSet.next();
        // Creating blank coupon to fill
        Coupon coupon = new Coupon();
        // Changing its amount
        coupon.setAmount(resultSet.getInt("amount"));
        // Changing its category
        coupon.setCategory((Category.values()[resultSet.getInt("category_id") - 1]));
        // Changing its ID
        coupon.setId(resultSet.getInt("id"));
        // Changing its company ID
        coupon.setCompanyID(resultSet.getInt("company_id"));
        // Changing its description
        coupon.setDescription(resultSet.getString("description"));
        // Changing its end date
        coupon.setEndDate(resultSet.getDate("end_date"));
        // Changing its start date
        coupon.setStartDate(resultSet.getDate("start_date"));
        // Changing its image url
        coupon.setImage(resultSet.getString("image"));
        // Changing its price
        coupon.setPrice(resultSet.getDouble("price"));
        // Changing its title
        coupon.setTitle(resultSet.getString("title"));
        // Return the wanted coupon
        return coupon;
    }


    @Override
    public boolean isCouponLeft(int couponID) throws SQLException, InterruptedException {
        // Creates a map collection to replace "?" on the statement. Key--> number of "?", Value--> value (the replacement)
        Map<Integer, Object> params = new HashMap<>();
        // Adding all the replacement values and their order
        params.put(1, couponID);
        // Running the statement and getting a ResultSet
        ResultSet resultSet = DatabaseUtils.runQueryForResult(IS_COUPON_LEFT, params);
        // Moving for the first line of the ResultSet
        resultSet.next();
        // Returning whether it counts more than 0 matching values (=exist)
        return resultSet.getInt("amount") > 0;
    }

    @Override
    public boolean isCouponExists(int couponID) throws SQLException, InterruptedException {
        // Creates a map collection to replace "?" on the statement. Key--> number of "?", Value--> value (the replacement)
        Map<Integer, Object> params = new HashMap<>();
        // Adding all the replacement values and their order
        params.put(1, couponID);
        // Running the statement and getting a ResultSet
        ResultSet resultSet = DatabaseUtils.runQueryForResult(IS_COUPON_EXISTS, params);
        // Moving for the first line of the ResultSet
        resultSet.next();
        // Returning whether it counts more than 0 matching values (=exist)
        return resultSet.getInt("total") > 0;
    }

    @Override
    public ArrayList<Coupon> getAllCompanyCoupons(int companyID) throws SQLException, InterruptedException {
        // Creates a map collection to replace "?" on the statement. Key--> number of "?", Value--> value (the replacement)
        Map<Integer, Object> params = new HashMap<>();
        // Adding all the replacement values and their order
        params.put(1, companyID);
        // Running the statement and getting a ResultSet
        ResultSet resultSet = DatabaseUtils.runQueryForResult(GET_ALL_COMPANY_COUPONS, params);
        // Creating a blank list to fill
        ArrayList<Coupon> couponsList = new ArrayList<>();
        // For every line on the ResultSet
        while (resultSet.next()) {
            // Creating a blank coupon to fill
            Coupon coupon = new Coupon();
            // Changing its amount
            coupon.setAmount(resultSet.getInt("amount"));
            // Changing its category
            coupon.setCategory(Category.values()[resultSet.getInt("category_id") - 1]);
            // Changing its ID
            coupon.setId(resultSet.getInt("id"));
            // Changing its company ID
            coupon.setCompanyID(resultSet.getInt("company_id"));
            // Changing its description
            coupon.setDescription(resultSet.getString("description"));
            // Changing its end date
            coupon.setEndDate(resultSet.getDate("end_date"));
            // Changing its start date
            coupon.setStartDate(resultSet.getDate("start_date"));
            // Changing its image url
            coupon.setImage(resultSet.getString("image"));
            // Changing its price
            coupon.setPrice(resultSet.getDouble("price"));
            // Changing its title
            coupon.setTitle(resultSet.getString("title"));
            // Adding the filled coupon to the list from above
            couponsList.add(coupon);
        }
        // Return the wanted list
        return couponsList;
    }


    @Override
    public ArrayList<Coupon> getCompanyCouponsByCategory(int companyID, Category category) throws SQLException, InterruptedException {
        // Creates a map collection to replace "?" on the statement. Key--> number of "?", Value--> value (the replacement)
        Map<Integer, Object> params = new HashMap<>();
        // Adding all the replacement values and their order
        params.put(1, companyID);
        params.put(2, category.ordinal() + 1);
        // Running the statement and getting a ResultSet
        ResultSet resultSet = DatabaseUtils.runQueryForResult(GET_COMPANY_COUPONS_BY_CATEGORY, params);
        // Creating a blank list to fill
        ArrayList<Coupon> couponsList = new ArrayList<>();
        // For every line on the ResultSet
        while (resultSet.next()) {
            // Creating a blank coupon to fill
            Coupon coupon = new Coupon();
            // Changing its amount
            coupon.setAmount(resultSet.getInt("amount"));
            // Changing its category
            coupon.setCategory(Category.values()[resultSet.getInt("category_id") - 1]);
            // Changing its ID
            coupon.setId(resultSet.getInt("id"));
            // Changing its company ID
            coupon.setCompanyID(resultSet.getInt("company_id"));
            // Changing its description
            coupon.setDescription(resultSet.getString("description"));
            // Changing its end date
            coupon.setEndDate(resultSet.getDate("end_date"));
            // Changing its start date
            coupon.setStartDate(resultSet.getDate("start_date"));
            // Changing its image url
            coupon.setImage(resultSet.getString("image"));
            // Changing its price
            coupon.setPrice(resultSet.getDouble("price"));
            // Changing its title
            coupon.setTitle(resultSet.getString("title"));
            // Adding the filled coupon to the list from above
            couponsList.add(coupon);
        }
        // Return the wanted list
        return couponsList;
    }


    @Override
    public ArrayList<Coupon> getCompanyCouponsTillMaxPrice(int companyID, double maxPrice) throws SQLException, InterruptedException {
        // Creates a map collection to replace "?" on the statement. Key--> number of "?", Value--> value (the replacement)
        Map<Integer, Object> params = new HashMap<>();
        // Adding all the replacement values and their order
        params.put(1, companyID);
        params.put(2, maxPrice);
        // Running the statement and getting a ResultSet
        ResultSet resultSet = DatabaseUtils.runQueryForResult(GET_COMPANY_COUPONS_TILL_MAX_PRICE, params);
        // Creating a blank list to fill
        ArrayList<Coupon> couponList = new ArrayList<>();
        // For every line on the ResultSet
        while (resultSet.next()) {
            // Creating a blank coupon to fill
            Coupon coupon = new Coupon();
            // Changing its amount
            coupon.setAmount(resultSet.getInt("amount"));
            // Changing its category
            coupon.setCategory(Category.values()[resultSet.getInt("category_id") - 1]);
            // Changing its ID
            coupon.setId(resultSet.getInt("id"));
            // Changing its company ID
            coupon.setCompanyID(resultSet.getInt("company_id"));
            // Changing its description
            coupon.setDescription(resultSet.getString("description"));
            // Changing its end date
            coupon.setEndDate(resultSet.getDate("end_date"));
            // Changing its start date
            coupon.setStartDate(resultSet.getDate("start_date"));
            // Changing its image url
            coupon.setImage(resultSet.getString("image"));
            // Changing its price
            coupon.setPrice(resultSet.getDouble("price"));
            // Changing its title
            coupon.setTitle(resultSet.getString("title"));
            // Adding the filled coupon to the list from above
            couponList.add(coupon);
        }
        // Return the wanted coupon list
        return couponList;
    }

    @Override
    public void decreaseCouponAmount(int couponID) throws InterruptedException {
        // Creates a map collection to replace "?" on the statement. Key--> number of "?", Value--> value (the replacement)
        Map<Integer, Object> params = new HashMap<>();
        // Adding all the replacement values and their order
        params.put(1, couponID);
        // Running the statement
        DatabaseUtils.runQuery(DECREASE_COUPON_AMOUNT, params);
    }

    @Override
    public void increaseCouponAmount(int couponID) throws InterruptedException {
        // Creates a map collection to replace "?" on the statement. Key--> number of "?", Value--> value (the replacement)
        Map<Integer, Object> params = new HashMap<>();
        // Adding all the replacement values and their order
        params.put(1, couponID);
        // Running the statement
        DatabaseUtils.runQuery(INCREASE_COUPON_AMOUNT, params);
    }

    @Override
    public boolean isCouponValid(int couponID) throws SQLException, InterruptedException {
        // Getting the wanted coupon
        Coupon coupon = getOneCoupon(couponID);
        // Returning whether the coupon end date already passed (true=haven't passed)
        return coupon.getEndDate().after(GregorianCalendar.getInstance().getTime());
    }

    @Override
    public void deleteAllCompanyCoupons(int companyID) throws InterruptedException {
        // Creates a map collection to replace "?" on the statement. Key--> number of "?", Value--> value (the replacement)
        Map<Integer, Object> params = new HashMap<>();
        // Adding all the replacement values and their order
        params.put(1, companyID);
        // Running the statement
        DatabaseUtils.runQuery(DELETE_ALL_COMPANY_COUPONS, params);
    }

    @Override
    public boolean isCouponExistsByNameForCompany(String name, int companyID) throws SQLException, InterruptedException {
        // Creates a map collection to replace "?" on the statement. Key--> number of "?", Value--> value (the replacement)
        Map<Integer, Object> params = new HashMap<>();
        // Adding all the replacement values and their order
        params.put(1, companyID);
        params.put(2, name);
        // Running the statement and getting a ResultSet
        ResultSet resultSet = DatabaseUtils.runQueryForResult(IS_COUPON_EXISTS_BY_NAME_FOR_COMPANY, params);
        // Moving for the first line of the ResultSet
        resultSet.next();
        // Returning whether it counts more than 0 matching values (=exist)
        return resultSet.getInt("total") > 0;
    }
}
