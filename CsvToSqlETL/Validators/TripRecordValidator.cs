using CsvToSqlETL.Models;

namespace CsvToSqlETL.Validators
{
    public static class TripRecordValidator
    {
        public static bool TryValidate(ref TripRecord record, out string error)
        {
            error = string.Empty;

            record.StoreAndFwdFlag = record.StoreAndFwdFlag?.Trim();

            if (record.PassengerCount == null || record.PassengerCount < 1 || record.PassengerCount > 8)
            {
                error = "PassengerCount must be between 1 and 8";
                return false;
            }

            if (record.PickupDatetime == default || record.DropoffDatetime == default)
            {
                error = "PickupDatetime or DropoffDatetime is missing or invalid";
                return false;
            }

            if (record.PickupDatetime > record.DropoffDatetime)
            {
                error = "PickupDatetime occurs after DropoffDatetime";
                return false;
            }

            if (record.TripDistance <= 0 || record.TripDistance > 1000)
            {
                error = "TripDistance must be greater than 0 and realistic (< 1000 miles)";
                return false;
            }

            if (record.FareAmount < 0 || record.TipAmount < 0)
            {
                error = "FareAmount or TipAmount cannot be negative";
                return false;
            }

            if (record.PULocationID == null || record.DOLocationID == null)
            {
                error = "PULocationID or DOLocationID is missing";
                return false;
            }

            if (record.TripDistance == 0 && (record.DropoffDatetime - record.PickupDatetime).TotalMinutes > 2)
            {
                error = "TripDistance is 0 but duration is too long — may be invalid";
                return false;
            }

            return true;
        }
    }
}
