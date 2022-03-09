package coupon_project.beans;

import java.util.Date;

public class Coupon {
    // Coupon ID
    private int id;
    // Company ID
    private int companyID;
    // Coupon category
    private Category category;
    // Coupon title (name)
    private String title;
    // Coupon description
    private String description;
    // Coupon start date
    private Date startDate;
    // Coupon end date
    private Date endDate;
    // Coupon amount left
    private int amount;
    // Coupon price
    private double price;
    // Coupon image URL
    private String image;

    /**
     * Constructor for coupon instance
     *
     * @param category    Coupon category
     * @param title       Coupon title (name)
     * @param description Coupon description
     * @param startDate   Coupon start date
     * @param endDate     Coupon end date
     * @param amount      Coupon amount left
     * @param price       Coupon price
     * @param image       Coupon image URL
     */
    public Coupon(Category category, String title, String description, Date startDate, Date endDate, int amount, double price, String image) {
        this.id = 0;
        this.companyID = 0;
        this.category = category;
        this.title = title;
        this.description = description;
        this.startDate = startDate;
        this.endDate = endDate;
        this.amount = amount;
        this.price = price;
        this.image = image;
    }

    /**
     * Blank constructor for using
     */
    public Coupon() {
    }

    /**
     * Get function for coupon ID
     *
     * @return Coupon ID
     */
    public int getId() {
        return id;
    }

    /**
     * Set function for coupon ID
     *
     * @param id Coupon ID
     */
    public void setId(int id) {
        this.id = id;
    }

    /**
     * Get function for company ID
     *
     * @return Company ID
     */
    public int getCompanyID() {
        return companyID;
    }

    /**
     * Set function for company ID
     *
     * @param companyID Company ID
     */
    public void setCompanyID(int companyID) {
        this.companyID = companyID;
    }

    /**
     * Get function for coupon category
     *
     * @return Coupon category
     */
    public Category getCategory() {
        return category;
    }

    /**
     * Set function for coupon category
     *
     * @param category Coupon category
     */
    public void setCategory(Category category) {
        this.category = category;
    }

    /**
     * Get function for coupon title (name)
     *
     * @return Coupon title (name)
     */
    public String getTitle() {
        return title;
    }

    /**
     * Set function for coupon title (name)
     *
     * @param title Coupon title (name)
     */
    public void setTitle(String title) {
        this.title = title;
    }

    /**
     * Get function for coupon description
     *
     * @return Coupon description
     */
    public String getDescription() {
        return description;
    }

    /** 
     * Set function for coupon description
     *
     * @param description Coupon description
     */
    public void setDescription(String description) {
        this.description = description;
    }

    /**
     * Get function for coupon start date
     *
     * @return Coupon start date
     */
    public Date getStartDate() {
        return startDate;
    }

    /**
     * Set function for coupon start date
     *
     * @param startDate Coupon start date
     */
    public void setStartDate(Date startDate) {
        this.startDate = startDate;
    }

    /**
     * Get function for coupon end date
     *
     * @return Coupon end date
     */
    public Date getEndDate() {
        return endDate;
    }

    /**
     * Set function for coupon end date
     *
     * @param endDate Coupon end date
     */
    public void setEndDate(Date endDate) {
        this.endDate = endDate;
    }

    /**
     * Get function for coupon amount left
     *
     * @return Coupon amount left
     */
    public int getAmount() {
        return amount;
    }

    /**
     * Set function for coupon amount left
     *
     * @param amount Coupon amount left
     */
    public void setAmount(int amount) {
        this.amount = amount;
    }

    /**
     * Get function for coupon price
     *
     * @return Coupon price
     */
    public double getPrice() {
        return price;
    }

    /**
     * Set function for coupon price
     *
     * @param price Coupon price
     */
    public void setPrice(double price) {
        this.price = price;
    }

    /**
     * Get function for coupon image URL
     *
     * @return Coupon image URL
     */
    public String getImage() {
        return image;
    }

    /**
     * Set function for coupon image URL
     *
     * @param image Coupon image URL
     */
    public void setImage(String image) {
        this.image = image;
    }

    @Override
    public String toString() {
        return id + ", " + companyID + ", " + category + ", \"" + title + '\"' + ", " + description +
                ", " + startDate + " - " + endDate + ", " + amount + "left, " + price + "$";
    }
}
