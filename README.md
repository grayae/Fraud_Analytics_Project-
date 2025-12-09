# Fraud_Analytics_Project

## Business Problem

The payments platform lacks a unified, behavior driven risk view across customers, devices and merchants, making it difficult to distinguish organic growth from coordinated fraud, device abuse and systemic transaction failure risk. The objective of this analysis is to identify hidden behavioral risk patterns, validate merchant risk classifications against real transaction outcomes and surface operational weak points that directly impact fraud losses, customer trust, and platform reliability.

## Tools

Power BI, SQL, DAX

## PowerBI Dashboards

### Executive Summary Dashboard
![Executive Summary](screenshots/executive_summary_screenshot.png)

### Customer & Device Behavior Dashboard
![Customer & Device Behavior](screenshots/customer_&_device_behaviour_screenshot.png)

### Merchant Risk & Transactions Dashboard
![Merchant Risk & Transactions](screenshots/merchant_risk_&_transactions_screenshot.png)

## Executive Summary Dashboard

This dashboard provides a high-level signal on whether platform activity is being driven by normal customer behavior or by elevated fraud and infrastructure stress.

### Key Findings

- Activity is highly concentrated, with a small set of customers and devices driving a disproportionate share of total transactions. This creates both revenue concentration risk and amplified fraud exposure if those entities are compromised.
- Device behavior is a major risk vector, with significant reuse and high device activity indicating possible automation, emulation or coordinated usage across multiple accounts rather than purely organic consumer behavior.
- Intraday transaction patterns show abnormal early morning spikes, a strong indicator of bot driven execution windows, bulk retries or scheduled attack behavior rather than natural human usage.
- Weekly transaction flow follows a business driven rhythm, confirming that while fraud pressure exists, the platform is still anchored in real commercial usage pattern rather than being dominated by synthetic traffic.

### Strategic Implications
- Fraud risk is structural, not episodic, requiring always-on monitoring rather than reactive intervention.
- Device level controls are as critical as customer level controls for reducing coordinated attacks.
- Time based risk scoring should be applied to early morning hours where automated activity concentrates.
Operational reliability improvements can reduce unnecessary friction caused by elevated transaction failures.

## Customer & Device Behavior Dashboard

This dashboard analyzes how customers interact with the platform across time and devices. The goal is to distinguish normal engagement from potentially risky behavior and to support both growth strategy and fraud monitoring.

### Key Findings

#### 1. Multi device usage & off hour activity as a coordinated fraud signal:
A significant portion of customers transact across multiple devices. While multi-device usage can reflect normal behavior (mobile + desktop), it is also a key fraud signal, especially when combined with high transaction frequency. Nighttime transactions and weekend transaction rates moderate activity outside standard business hours. 

#### 2. Customer activity segmentation as a measure of platform stability and revenue concentration:
The customer base is heavily concentrated in the medium activity segment, indicating strong recurring engagement across the platform. High activity power users represent a strategic revenue and risk segment, while low activity users make up a minor portion of the base. This distribution suggests that the platform’s usage is stable and not driven by one-time transactions.

#### 3. Customer activity segmentation as a measure of platform stability and revenue concentration:
Most devices fall into the medium activity range, while over one-third of devices are inactive possibly due to customer churn, one-time signups or abandoned installs. Very few devices generate extremely high transaction volumes, which reduces systemic device-level fraud risk. This suggests a broad device footprint with moderate usage intensity per device.

### Strategic Implications
- The high activity segment (222 users) should be targeted for loyalty rewards, premium services or early feature testing
- 18.62 percent night usage supports time-based fraud risk weighting or night-time transaction throttling
- Inactive customers are candidates for reactivation campaigns and onboarding optimization

## Merchant Risk & Transaction Exposure Dashboard

This dashboard evaluates merchant-level risk exposure by combining merchant risk classifications with transaction behavior, failure patterns and transaction value segmentation. The objective is to identify whether structural merchant risk aligns with real transaction outcomes and payment failure behavior.

### Key Findings

#### 1. Merchant risk distribution:
The merchant base is composed of 64.25% low-risk merchants and 35.75% high-risk merchants, indicating that a large portion of transaction flow passes through riskier onboarding controls, higher fraud susceptibility or potential compliance gaps. The absence of medium-risk merchants reflects a polarized risk classification framework, where merchants are either considered safe or highly risky. This structure amplifies the importance of continuous monitoring of the high risk segment.

#### 2. Transaction value environment:
Transaction volume is heavily skewed toward low amount payments, which dominate overall platform activity. Medium value transactions form a secondary tier, while high-value transactions represent only a very small fraction of total activity. This confirms that the platform operates as a high-volume, low-ticket payment environment, where fraud risk is more likely to emerge through repeated attempts rather than single large transactions.

#### 3. High value transaction exposure:
The average high-value transaction ratio is 0.66%, showing that large transactions are rare. This suggests possible card testing attacks, poor network reliability or fraud rules blocking many transactions. When segmented by merchant risk, both high and low risk merchants show roughly 1% high value exposure, indicating that high value fraud risk is distributed across all risk classes rather than isolated to high risk merchants. This implies that safeguards around large transactions must be implemented globally, not only on flagged merchants.

#### 4. Failed transaction risk:
The platform records an average failed transaction rate of 10.46%, which is elevated for a digital payments ecosystem. When segmented by merchant risk, high risk merchants record a slightly higher average failure rate (11%) compared to low risk merchants (10%). While this confirms directional alignment between risk classification and outcomes, the small gap also suggests that failure drivers are partly systemic (network issues, retries, card testing) and not limited to merchant-level risk alone.

### Strategic Implications
- The ecosystem exhibits a large high-risk merchant segment, making merchant-level risk controls a priority.
- Fraud exposure is driven more by transaction volume than transaction size, indicating elevated risk of automated testing, bot attacks or repeated low-value fraud attempts.
- High failure rates across both risk groups suggest combined effects of fraud prevention rules, network instability and attacker-driven transaction probing.
- High-value transaction controls must apply across all merchants, since exposure is not limited to the high-risk population.
- This dashboard provides a merchant-risk lens that supports risk-based merchant monitoring, transaction throttling strategies and differentiated fraud control enforcement across the payments platform.
