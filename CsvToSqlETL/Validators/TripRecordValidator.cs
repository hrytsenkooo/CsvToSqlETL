using CsvToSqlETL.Models;

namespace CsvToSqlETL.Validators
{
    public static class TripRecordValidator
    {
        /// <summary>
        /// Validates a TripRecord object against a set of business rules and data quality checks.
        /// Trims certain fields, checks for required values, ensures numerical values are within expected ranges,
        /// and verifies logical consistency (e.g., pickup time before dropoff).
        /// </summary>
        /// <param name="record">The trip record to validate. May be modified during validation</param>
        /// <param name="error">If validation fails, contains the reason for the failure</param>
        /// <returns>True if the record is valid; otherwise, false with an error message</returns>
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
