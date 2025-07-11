# Quantum Vulnerability Analysis: Anomaly Detection

This document provides a comprehensive explanation of each anomaly detection test performed by the quantum analysis system. These tests are designed to identify suspicious patterns in the behavior of quantum-vulnerable Bitcoin addresses (primarily P2PK) that could indicate the presence of a quantum computer attack or other high-risk events.

## Overview

Anomaly detection is a critical component of quantum risk monitoring. The following tests are implemented to detect:
- Sudden or unusual spending activity
- Large movements of vulnerable funds
- Unusual transaction fee patterns
- Suspicious timing of activity
- Coordinated movements across multiple addresses
- Whale (large holder) activity

Each test is described in detail below, including its rationale, detection logic, and what constitutes an anomaly.

---

## 1. Spending Spikes Detection
**Purpose:** Detects sudden increases in the number or value of transactions spending from vulnerable addresses.

**Rationale:**
A quantum attacker may attempt to rapidly drain vulnerable addresses once a quantum computer is available. This would manifest as a spike in spending activity compared to historical norms.

**Detection Logic:**
- Calculate the average hourly transaction count and total spent (in satoshis) over the past 7 days (baseline).
- Calculate the same metrics for the most recent 24-hour window.
- If the recent hourly rate exceeds the baseline by a configurable multiplier (default: 3x), flag as an anomaly.
- Both transaction count and total spent are checked independently.

**Anomaly Criteria:**
- Recent hourly transaction count > 3x baseline hourly transaction count
- Recent hourly total spent > 3x baseline hourly total spent

**Example:**
> If the baseline is 10 tx/hour and the last 24 hours saw 40 tx/hour, this is flagged as a spike.

---

## 2. Large Balance Movements
**Purpose:** Detects unusually large single transactions spending from vulnerable addresses.

**Rationale:**
A quantum attacker may attempt to move large amounts in a single transaction to minimize exposure or maximize gain.

**Detection Logic:**
- Scan all transactions in the last 24 hours where the input amount from a vulnerable address exceeds a threshold (default: 100 BTC).
- For each, calculate the ratio of the spent amount to the address's balance.
- Flag as an anomaly if the threshold is exceeded.

**Anomaly Criteria:**
- Any single transaction spending >= 100 BTC from a vulnerable address in the last 24 hours.
- Higher severity for amounts > 1000 BTC (whale movements).

**Example:**
> A transaction spending 500 BTC from a single P2PK address is flagged as a high-severity anomaly.

---

## 3. Fee Anomalies
**Purpose:** Detects transactions with unusually high fees.

**Rationale:**
A quantum attacker may be willing to pay very high fees to ensure their transaction is confirmed quickly, outpacing defenders.

**Detection Logic:**
- Calculate the average transaction fee for vulnerable address transactions over the last 7 days.
- Identify transactions in the last 24 hours with fees greater than a configurable multiplier (default: 5x) of the average.
- Flag these as anomalies.

**Anomaly Criteria:**
- Transaction fee > 5x average fee for vulnerable address transactions.

**Example:**
> If the average fee is 0.001 BTC and a transaction pays 0.01 BTC, it is flagged as a fee anomaly.

---

## 4. Time-Based Anomalies
**Purpose:** Detects unusual activity patterns based on the time of day.

**Rationale:**
Quantum attacks may be timed to coincide with periods of low network monitoring (e.g., early morning hours) or may show unusual temporal clustering.

**Detection Logic:**
- Analyze transaction activity by hour of day for the last week.
- Calculate the average number of transactions per hour.
- Flag hours (especially midnight to 5 AM) where activity is more than 2x the average.

**Anomaly Criteria:**
- Any hour in the midnight-5 AM window with > 2x average hourly activity.

**Example:**
> If 3 AM usually sees 2 tx/hour but suddenly sees 10 tx/hour, this is flagged as a time-based anomaly.

---

## 5. Address Clustering
**Purpose:** Detects coordinated activity across multiple vulnerable addresses within a short time window.

**Rationale:**
A quantum attacker may compromise and move funds from many addresses simultaneously, resulting in a burst of activity involving many unique addresses in a short period.

**Detection Logic:**
- For each minute in the last hour, count the number of unique vulnerable addresses spending funds.
- If the count exceeds a threshold (default: 5 addresses in one minute), flag as an anomaly.
- Higher severity for larger clusters.

**Anomaly Criteria:**
- >= 5 unique vulnerable addresses spending in the same minute.
- Higher severity for clusters > 10 addresses.

**Example:**
> If 12 addresses spend funds in the same minute, this is flagged as a high-severity cluster anomaly.

---

## 6. Whale Activity
**Purpose:** Detects any activity from "whale" addresses (those holding > 1000 BTC).

**Rationale:**
Whale addresses are high-value targets. Any movement from these addresses is significant and may indicate a quantum attack or a defensive move by the owner.

**Detection Logic:**
- Identify all transactions in the last 24 hours where the sending address has a balance >= 1000 BTC.
- Flag all such transactions as critical anomalies.

**Anomaly Criteria:**
- Any transaction from a vulnerable address with >= 1000 BTC balance.

**Example:**
> A whale address with 5000 BTC sends any amount—this is always flagged as a critical anomaly.

---

## Severity and Confidence
Each anomaly is assigned a severity (LOW, MEDIUM, HIGH, CRITICAL) and a confidence score (0.0 to 1.0) based on how extreme the detected pattern is relative to normal activity.

- **Severity** is based on the magnitude and potential impact of the anomaly.
- **Confidence** is based on how far the observed value deviates from the baseline or threshold.

---

## Customization
Thresholds and detection windows can be configured in the analysis scripts or via environment variables. This allows tuning for sensitivity and false positive control.

---

## Future Enhancements
- Machine learning-based anomaly detection
- Graph/network analysis of address relationships
- Real-time streaming detection
- Integration with alerting and response systems

---

**For more details, see the implementation in `detect_anomalies.py`.** 