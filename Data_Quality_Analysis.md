# 1. Root Cause Analysis


# For the discrepancy in volume, I would first consider the calculation of volume in the query, which is calculated as (amount * exchange_rates.avg_usd_rate). I observed that the API calculates volume on the exact price at the moment of execution. The query's use of avg_usd_rate could incorrectly inflate or deflate the volume of trades that occurred when the price was far from the average price. This would happen if a massive event, such as a liquidation event, in which the method of calculation in the query could produce different values than simply getting the exact price at the moment of the trade.


# Another logical issue that could have driven this discrepancy is if there is a UTC vs. local time mismatch. The cast "transactions.timestamp::DATE" defaults to the database server's local timezone. If the database is set to UTC but the internal dashboard viewers operate in PST, then the window for calculating volume shifts by 8 hours. This is a misalignment that will likely cause inclusion of trading sessions past the cutoff and exclusion of some sessions within. This could lead to potential differences in trading volume calculations for the dashboards/query and the public API. The issue of double-counting sessions could also occur in this scenario if we forget to standardize some queries/dashboards for UTC while standardizing others.


# A final key logical issue that can occur is found in the time frame for which the trading volume is calculated, as SQL's "BETWEEN" keyword is inclusive and calculates 8 days in the query. This could be a potential logic error if this is not known to the pipeline creator, especially if we are only looking at 7 days of trading volumes, but I will assume this may not be an issue due to knowing that the volume discrepancy occurs even in "the same time period".


# For an example of a data source difference that could cause volume discrepancy, we can consider the internal "Transactions" database. If this database contains any transactions that are not trades, such as deposits or internal wallet transfers, volume could be inflated. We would then have more money moving than expected from just trades. We would need to check whether the "transaction_type" was a trade in the "Transactions" database. We would have to query on this (looking for words like "trades" within this column) to ensure we are excluding any other money moves that could inflate overall volume when looking for strictly trading volume.


# Another hypothetical issue is if the Transactions database includes any incomplete, cancelled, or failed trades. In this case, the query would be summing up money that never moved.


# A data quality issue I can see occurring is in the "Users" table. The query joins Users on user_id, but this can be an issue if "Users" produces duplicate user_ids but with different regions or created_at values. This could occur, for example, if a user updates their account settings and where they are located to not match their previous region, and perhaps a new account or entry in the "Users" table is created as a result of this. The join in the query could then multiply transaction rows and cause volume to be counted multiple times for each user.




# 2. Investigation Plan


# To efficiently identify the root of the $200M discrepancy, I will follow a top-down approach starting with high-level aggregate checks and drilling down to row-level logic. I would first query from the "Ground Truth" by running a raw query on the Public API. This will rule out simple parameter mismatches before debugging code, as discrepancies often stem from differing inputs rather than broken logic. This query would be for the exact same start and end timestamps used in the internal SQL query to see if the discrepancy persists when time zones are aligned. We likely suffer from timezone misalignment, as the public API is in UTC, but the internal SQL could be for a specific business zone. We will run both the public API and the SQL query on explicit UTC timestamps, and if the numbers converge, the issue is strictly timezone handling. To continue this stage of "verifying definitions," I will also check the potential "8-day" hypothesis, in which the current query may capture an extra day due to its inclusive time range. I will check the row count and data range boundaries to verify if the discrepancy equals roughly one average day of volume (then I will check the operators BETWEEN vs. >=/<).


# To drill down, I will then isolate where the extra volume is coming from via dimensional segmentation. This will help determine if the error is systemic or localized. I will segment volume calculation by day, hour, or even minute to locate temporal spikes or "fat-finger”/misinput events. I will then create aggregate calculations to see if they match across the API to the internal data, hopefully identifying calculation errors, or if the difference is spread out across days or consolidated to a single day/spike. I could then segment by asset and query by grouping with currency_pair. If an asset such as BTC-USD matches throughout but ETH-USD is off, then the issue could be with a bad avg_usd_rate (or even a bad transactions.amount through double counting). If we do observe that every currency_pair is inflated in volume, then this could point to potential errors in joining on repeated users, or in counting incomplete transactions.


# Lastly, I will validate row-level logic and integrity. If the goal is systemic (constant inflation), we must find the flaw in the SQL logic. I will run a cardinality check on the primary key in both the users and transactions tables to observe potential duplications in counting columns multiple times. After checking if there are any duplicate IDs and verifying if the JOIN logic is creating cartesian products, we can perform transaction filtering. Where the current query may be summing up all money, including non-volume types like deposits, we can break down volume by transaction_type and a new column called "status". This will verify where status is only like "completed" and not in the potential transaction_type like "deposit".




# 3. Prevention Strategy


# To prevent recurrence of volume discrepancies and ensure long-term data integrity, I propose a strategy that involves implementing automated quality checks at the stages of ingestion, transformation, and presentation.


# To stop bad data from entering the warehouse at ingestion, I would introduce validation through schema enforcement, duplicate detection, and reference integrity. I will run these checks each time the data is ingested, which runs in near real-time (streaming) or in per-batch execution. 


# I would introduce data quality tests, specifically at the "join" and for the "rate" potential issue. I will add a schema test to ensure that user_id in the users table is always unique, and if not, then the failed pipeline should send an alert before calculating the volume. Other records that do not match the expected schema, such as negative transaction amounts or timeframe inconsistencies, will also be rejected. For the reference integrity aspect of my checks, I will validate that every transaction's currency_pair exists in the exchange_rates reference table. All of these checks should contribute to cleaner data at the stage before the dashboard's SQL, in the data warehouse, to catch issues before they can occur. Issues that do occur will be sent with alerts to the data engineers. The implementation of these validations could be achieved using Apache Spark to validate micro-batches to capture issues quicker than on the entirety of the data. This allows for automation as well, and if the batch fails when we have a duplication rate > 0.5%, for example, the pipeline will automatically pause and keep the data we are using quarantined.


# For the transformation layer, I would introduce a logic check to ensure the business logic is applied correctly and consistently. I can further run uniqueness tests at this stage by implementing a strict primary key on the users table to prevent row multiplication (using a role like COUNT(user_id) = count(distinct user_id)). A check for the "Exchange_rates" table would be heuristic, and would involve triggering a warning if avg_usd_rate deviates more than about 10% from the previous day's close price. This would catch what we might call "fat finger" errors in this table. I would also improve logic standardization. I will move logic upstream, so we are not calculating "volume" in the dashboard SQL, but are instead pre-computed in a table such as "daily_exchange_volume" where rules are applied once and version-controlled. These rules can be asserted on every pipeline run and able to be accessed by members of the team wishing to work on the data warehouse through dbt (data build tool). These checks should run automatically immediately after the nightly transformation job (likely only run once a day due to potential limitations of heavy compute), and prior to dashboard refresh.


# In the final layer, which is the presentation and monitoring layer, my goal is to verify the output matches the source of truth (Public API) and then alert humans if not. Using Apache Airflow, I would run a metric reconciliation job to detect mismatches from the source of truth public API and the calculated internal volume. A set equation that I can use to deduce when calculated volume could become an issue is when (ABS(Internal_Vol - API_Vol) / API_Vol) > 1%. This run should be scheduled through something such as Airflow to run at about 2:00 UTC (after daily close) to detect if variance is higher than the team might want to allow. If the variance is higher than 1%, then an alert on a service such as Slack may need to be sent to the Data-Engineer on-call. Dashboard flags should appear in an indicator such as "Data Quality Status" to display in Tableau/Power BI, a red indicator when the data is currently under review. If applicable, metrics from upstream that are known to be in the calculation of the presented dashboard analytics should be red, while those perhaps unaffected can be orange to urge caution.
