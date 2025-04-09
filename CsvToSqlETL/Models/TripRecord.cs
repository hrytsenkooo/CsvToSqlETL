﻿namespace CsvToSqlETL.Models
{
    public class TripRecord
    {
        public DateTime PickupDatetime { get; set; }
        public DateTime DropoffDatetime { get; set; }
        public int PassengerCount { get; set; }
        public double TripDistance { get; set; }
        public string StoreAndFwdFlag { get; set; } = "";
        public int PULocationID { get; set; }
        public int DOLocationID { get; set; }
        public decimal FareAmount { get; set; }
        public decimal TipAmount { get; set; }

        public override string ToString()
        {
            return $"{PickupDatetime} -> {DropoffDatetime}, Passengers: {PassengerCount}, Distance: {TripDistance} mi, Tip: {TipAmount}$";
        }
    }
}
