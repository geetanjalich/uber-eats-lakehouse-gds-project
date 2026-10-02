# Genie Agent Instructions

## 1. Purpose

This guide provides the instruction text, trusted datasets, business vocabulary, and example questions for a Databricks Genie space built on the Uber Eats marketplace lakehouse.

The Genie experience should help users answer operational marketplace questions using curated gold views and data quality views, without exposing raw bronze tables or implementation details.

## 2. Recommended Tables And Views

Add these objects to the Genie space after the pipeline has published gold and DQ views.

For the `dev` target:

```text
ue_marketplace_lakehouse_dev.gold.vw_marketplace_executive_summary
ue_marketplace_lakehouse_dev.gold.vw_delivery_sla_analysis
ue_marketplace_lakehouse_dev.gold.vw_merchant_profitability
ue_marketplace_lakehouse_dev.dq.vw_failed_record_analysis
ue_marketplace_lakehouse_dev.dq.vw_dq_rule_trends
```

For the `prod` target:

```text
ue_marketplace_lakehouse_prod.gold.vw_marketplace_executive_summary
ue_marketplace_lakehouse_prod.gold.vw_delivery_sla_analysis
ue_marketplace_lakehouse_prod.gold.vw_merchant_profitability
ue_marketplace_lakehouse_prod.dq.vw_failed_record_analysis
ue_marketplace_lakehouse_prod.dq.vw_dq_rule_trends
```

## 3. Copy-Paste Genie Instructions

Paste the following text into the Genie space instruction area.

```text
You are an analytics assistant for an Uber Eats-style food delivery marketplace.

Use only the curated gold and DQ views added to this Genie space. Prefer aggregated views before detailed fact tables. Do not query bronze or raw landing data.

Use business-friendly language. Explain metrics in terms of orders, delivered orders, cancelled orders, gross booking amount, delivery SLA, average delivery time, platform commission, merchant performance, and data quality failures.

When a user asks for marketplace health, start from vw_marketplace_executive_summary.

When a user asks about merchant performance, cuisine trends, commission, or merchant-level revenue, start from vw_merchant_profitability.

When a user asks about delivery reliability, courier performance, pickup delays, prep delays, or SLA misses, start from vw_delivery_sla_analysis.

When a user asks about bad records, rejected records, quarantined records, failed data quality checks, or pipeline quality issues, start from vw_failed_record_analysis and vw_dq_rule_trends.

Treat sla_success_rate as a ratio between 0 and 1. When presenting it to users, show it as a percentage.

Treat gross_booking_amount and estimated_platform_commission as USD amounts.

For trend questions, group by order_date, delivery_date, checked_date, city_id, merchant_id, cuisine_type, or rule_id depending on the question.

If a user asks for root cause of a DQ issue, summarize the failing rule, source table, severity, failed record count, and representative source_record_json examples. Do not expose more record-level detail than needed.

If the question is ambiguous, ask a short follow-up question about date range, city, merchant, cuisine, or metric.

Order : A customer checkout request placed with a merchant

Delivered order : Order where order_status = delivered

Cancelled order : Order where order_status starts with cancelled

Gross booking amount : Customer-facing total payment amount

Platform commission : Estimated marketplace revenue from merchant commission

Delivery SLA : Whether actual delivery time is within estimated delivery time plus tolerance

SLA success rate : Share of deliveries completed within SLA

Prep minutes : Time from merchant acceptance to food prepared

Pickup wait minutes : Time from food prepared to courier pickup

Courier travel minutes : Time from pickup to delivery

Quarantined record : Record failing a native data quality rule

Critical DQ rule : Rule that blocks downstream trusted publication when failures exist
```

## 4. Business Terms

Use these meanings consistently.

| Term | Meaning |
|---|---|
| Order | A customer checkout request placed with a merchant |
| Delivered order | Order where `order_status = delivered` |
| Cancelled order | Order where `order_status` starts with `cancelled` |
| Gross booking amount | Customer-facing total payment amount |
| Platform commission | Estimated marketplace revenue from merchant commission |
| Delivery SLA | Whether actual delivery time is within estimated delivery time plus tolerance |
| SLA success rate | Share of deliveries completed within SLA |
| Prep minutes | Time from merchant acceptance to food prepared |
| Pickup wait minutes | Time from food prepared to courier pickup |
| Courier travel minutes | Time from pickup to delivery |
| Quarantined record | Record failing a native data quality rule |
| Critical DQ rule | Rule that blocks downstream trusted publication when failures exist |

## 5. Marketplace Health Questions

Ask questions like:

```text
Show daily orders, delivered orders, cancelled orders, and gross booking amount by city.
```

```text
Which city had the highest gross booking amount last week?
```

```text
Show the SLA success rate trend by city.
```

```text
Which dates had the highest cancellation rate?
```

```text
Compare average delivery minutes across cities.
```

## 6. Merchant Performance Questions

Ask questions like:

```text
Which merchants generated the highest gross booking amount?
```

```text
Show top merchants by estimated platform commission.
```

```text
Which cuisine types have the best SLA success rate?
```

```text
Which merchants have high order volume but low SLA success rate?
```

```text
Show merchant performance by city and cuisine type.
```

## 7. Delivery Reliability Questions

Ask questions like:

```text
Which couriers have the lowest SLA success rate?
```

```text
Show average prep minutes, pickup wait minutes, and courier travel minutes by city.
```

```text
Which city has the longest courier travel time?
```

```text
Compare delivery SLA by vehicle type.
```

```text
Find the biggest delivery bottleneck by city.
```

## 8. Data Quality Questions

Ask questions like:

```text
Which data quality rules failed in the latest run?
```

```text
Show failed records by rule ID and severity.
```

```text
Which source tables have the most quarantined records?
```

```text
Show recent DQ failure trends by checked date.
```

```text
Give sample failed records for the order required key rule.
```

```text
Explain why records were quarantined in the latest failed run.
```

## 9. Incident-Style Investigation Questions

Use these when a pipeline run fails because DQ blocked gold publication.

```text
Summarize the latest critical DQ failures.
```

```text
Which critical rule caused the pipeline to stop?
```

```text
Show the source table, failed record count, and sample payload for each critical rule.
```

```text
Are failures concentrated in one source table or spread across multiple feeds?
```

```text
What records should the source producer correct before re-execution?
```

## 10. Suggested Review Flow

After a successful pipeline run:

1. Ask for marketplace health by city and date.
2. Ask for top merchants by gross booking amount.
3. Ask for delivery SLA by city and vehicle type.
4. Ask for DQ rule trends.
5. Ask for any quarantined records in the latest run.

After a failed DQ run:

1. Ask which rules failed.
2. Ask which failures were critical.
3. Ask for sample quarantined payloads.
4. Identify the source table responsible for the failure.
5. Correct or remove the bad source files and re-execute from the appropriate checkpoint/table reset point.

