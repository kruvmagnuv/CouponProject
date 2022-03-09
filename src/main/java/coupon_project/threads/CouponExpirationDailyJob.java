package coupon_project.threads;

import coupon_project.beans.Coupon;
import coupon_project.dao.CouponsDAO;
import coupon_project.dao.CustomerVsCouponDAO;
import coupon_project.db_util.DatabaseUtils;
import coupon_project.db_util.Factory;

import java.sql.SQLException;

/**
 * Thread class that is used to erase every expired coupon from the database
 */
public class CouponExpirationDailyJob implements Runnable {
    // Boolean field that is used to define if the thread should keep running
    private boolean isContinue;
    // Coupon DAO instance used to access some database actions
    private final CouponsDAO couponActions;
    // CustomerVsCoupon DAO instance used to access some database actions
    private final CustomerVsCouponDAO purchaseActions;

    /**
     * Constructor to create an instance of CouponExpirationDailyJob in order to run it
     *
     * @param DB current used database (="sql")
     */
    public CouponExpirationDailyJob(String DB) {
        // Creates the CouponDBDAO instance
        this.couponActions = Factory.getCouponDAO(DB);
        // Creates the CustomerVsCouponDBDAO instance
        this.purchaseActions = Factory.getCustomerVsCouponDAO(DB);
        // Initializing isContinue field to true
        this.isContinue = true;
    }

    /**
     * Run function that implemented from the Runnable interface. The main function of the thread, begin running when
     * called "start" and stops running when catching InterruptedException and turning isContinue field false
     */
    @Override
    public void run() {
        // First, while isContinue field is true, the thread should continue running
        while (isContinue) {
            // Try and Catch to catch the InterruptedException or SQLException
            try {
                String sql = "DELETE FROM coupon_project.coupons WHERE end_date < CURDATE()";
                String sql2 = "DELETE FROM coupon_project.coupon_customers WHERE coupon_id IN ( " +
                        "SELECT id FROM coupon_project.coupons WHERE end_date < CURDATE()" +
                        ")";
                DatabaseUtils.runQuery(sql2);
                DatabaseUtils.runQuery(sql);
                // After checking all the coupons, the thread sleeps for 24 hours
                Thread.sleep(1000 * 60 * 60 * 24);
                //            milliseconds, seconds, minutes, hours
            } catch (InterruptedException | SQLException e) {
                // If the thread catches some problem, it stops itself using the stop function
                stop();
            }
        }
    }

    /**
     * changing the isContinue field to false, so the thread won't keep running
     */
    public void stop() {
        // Changes isContinue field to false
        this.isContinue = false;
    }
}
