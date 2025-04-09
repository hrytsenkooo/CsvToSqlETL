using CsvHelper.Configuration.Attributes;

namespace CsvToSqlETL.Models
{
    public class TripRecord
    {
        [Name("tpep_pickup_datetime")]
        public DateTime PickupDatetime { get; set; }

        [Name("tpep_dropoff_datetime")]
        public DateTime DropoffDatetime { get; set; }

        [Name("passenger_count")]
        public int PassengerCount { get; set; }

        [Name("trip_distance")]
        public double TripDistance { get; set; }

        [Name("store_and_fwd_flag")]
        public string StoreAndFwdFlag { get; set; } = "";

        [Name("PULocationID")]
        public int PULocationID { get; set; }

        [Name("DOLocationID")]
        public int DOLocationID { get; set; }

        [Name("fare_amount")]
        public decimal FareAmount { get; set; }

        [Name("tip_amount")]
        public decimal TipAmount { get; set; }

        public override int GetHashCode()
        {
            return HashCode.Combine(PickupDatetime, DropoffDatetime, PassengerCount);
        }

        public override bool Equals(object? obj)
        {
            if (obj is not TripRecord other) return false;
            return PickupDatetime == other.PickupDatetime && DropoffDatetime == other.DropoffDatetime && PassengerCount == other.PassengerCount;
        }

        public override string ToString()
        {
            return $"{PickupDatetime} -> {DropoffDatetime}, Passengers: {PassengerCount}, Distance: {TripDistance} mi, Tip: {TipAmount}$";
        }
    }
}
