import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

object TimeUtils {

  def epochMsToMarketDate(
      epochMs: Column,
      timezone: Column
  ): Column = {

    val tsUTC =
      to_timestamp((epochMs / 1000).cast("long"))

    when(timezone === "Asia/Kolkata",
      from_utc_timestamp(tsUTC, "Asia/Kolkata"))
      .otherwise(
        from_utc_timestamp(tsUTC, "US/Eastern"))
      .cast("date")
  }

  def epochMsToUTCDate(epochMs: Column): Column =
    to_timestamp((epochMs / 1000).cast("long"))
      .cast("date")
}