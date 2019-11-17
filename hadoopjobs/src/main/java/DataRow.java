public class DataRow {
    private Float tripDistance;
    private Integer pickupLocationId;
    private Integer dropoffLocationId;
    private Integer paymentType;
    private Float fareAmount;
    private Float surcharge;
    private Float tipAmount;
    private Float tollsAmount;
    private Float totalAmount;

    public DataRow(Float tripDistance, Integer pickupLocationId, Integer dropoffLocationId, Integer paymentType, Float fareAmount, Float surcharge, Float tipAmount, Float tollsAmount, Float totalAmount) {
        this.tripDistance = tripDistance;
        this.pickupLocationId = pickupLocationId;
        this.dropoffLocationId = dropoffLocationId;
        this.paymentType = paymentType;
        this.fareAmount = fareAmount;
        this.surcharge = surcharge;
        this.tipAmount = tipAmount;
        this.tollsAmount = tollsAmount;
        this.totalAmount = totalAmount;
    }

    public Float getTripDistance() {
        return tripDistance;
    }

    public Integer getPickupLocationId() {
        return pickupLocationId;
    }

    public Integer getDropoffLocationId() {
        return dropoffLocationId;
    }

    public Integer getPaymentType() {
        return paymentType;
    }

    public Float getFareAmount() {
        return fareAmount;
    }

    public Float getSurcharge() {
        return surcharge;
    }

    public Float getTipAmount() {
        return tipAmount;
    }

    public Float getTollsAmount() {
        return tollsAmount;
    }

    public Float getTotalAmount() {
        return totalAmount;
    }
}
