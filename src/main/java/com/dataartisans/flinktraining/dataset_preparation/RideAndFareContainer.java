package com.dataartisans.flinktraining.dataset_preparation;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;

public class RideAndFareContainer {
  TaxiRide taxiRide;
  TaxiFare taxiFare;

  public RideAndFareContainer(
      TaxiRide taxiRide,
      TaxiFare taxiFare) {
    this.taxiRide = taxiRide;
    this.taxiFare = taxiFare;
  }

  public TaxiFare getTaxiFare() {
    return taxiFare;
  }

  public void setTaxiFare(
      TaxiFare taxiFare) {
    this.taxiFare = taxiFare;
  }

  public TaxiRide getTaxiRide() {
    return taxiRide;
  }

  public void setTaxiRide(
      TaxiRide taxiRide) {
    this.taxiRide = taxiRide;
  }
}
