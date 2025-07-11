# Quantum Vulnerability Analysis

A comprehensive analysis system for detecting quantum computer attacks and assessing Bitcoin's quantum vulnerability risk exposure.

## Overview

This subproject analyzes the data collected by the P2PK scanner to provide critical insights into Bitcoin's quantum vulnerability landscape. It focuses on:

- **Risk Assessment**: Quantifying the total value at risk from quantum attacks
- **Anomaly Detection**: Identifying patterns that could indicate active quantum computer attacks
- **Trend Analysis**: Monitoring changes in vulnerable address behavior over time
- **Attack Simulation**: Modeling potential quantum attack scenarios

## Key Statistics & Metrics

### 1. Value at Risk (VaR) Analysis
- **Total BTC at Risk**: Sum of all vulnerable address balances
- **USD Value at Risk**: Current market value of vulnerable funds
- **Risk Distribution**: Breakdown by balance ranges (whales vs small holders)
- **Historical VaR**: How risk exposure has changed over time

### 2. Address Vulnerability Metrics
- **Active vs Dormant**: Ratio of recently active vs long-dormant addresses
- **Age Distribution**: How old the vulnerable addresses are
- **Balance Concentration**: Gini coefficient and concentration metrics
- **Geographic Risk**: Time-based patterns suggesting geographic clustering

### 3. Transaction Pattern Analysis
- **Spending Patterns**: How quickly vulnerable addresses are being spent
- **Input/Output Ratios**: Unusual transaction patterns
- **Fee Analysis**: Anomalous fee structures that might indicate urgency
- **Multi-input Transactions**: Complex transactions involving multiple vulnerable addresses

### 4. Anomaly Detection Indicators
- **Sudden Spending Spikes**: Unusual increases in vulnerable address spending
- **Large Balance Movements**: Significant transfers from vulnerable addresses
- **Time-based Anomalies**: Unusual activity patterns (weekends, holidays, etc.)
- **Fee Anomalies**: Unusually high fees suggesting urgency
- **Address Clustering**: Multiple vulnerable addresses moving funds simultaneously

### 5. Quantum Attack Simulation
- **Attack Scenarios**: Modeling different quantum attack strategies
- **Recovery Time Estimates**: How long it would take to compromise addresses
- **Economic Impact**: Potential market effects of large-scale attacks
- **Defense Effectiveness**: Assessing current mitigation strategies

## Database Schema

### quantum_analysis_results
Stores computed statistics and analysis results.

```sql
CREATE TABLE quantum_analysis_results (
    id SERIAL PRIMARY KEY,
    analysis_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    metric_name VARCHAR(100) NOT NULL,
    metric_value DECIMAL(20,8),
    metric_json JSONB,
    description TEXT,
    risk_level VARCHAR(20), -- LOW, MEDIUM, HIGH, CRITICAL
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### anomaly_events
Tracks detected anomalies and suspicious patterns.

```sql
CREATE TABLE anomaly_events (
    id SERIAL PRIMARY KEY,
    event_date TIMESTAMP NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    severity VARCHAR(20) NOT NULL, -- LOW, MEDIUM, HIGH, CRITICAL
    description TEXT NOT NULL,
    affected_addresses INTEGER,
    affected_balance_satoshi BIGINT,
    confidence_score DECIMAL(3,2), -- 0.00 to 1.00
    details_json JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### risk_assessments
Historical risk assessments for trend analysis.

```sql
CREATE TABLE risk_assessments (
    id SERIAL PRIMARY KEY,
    assessment_date TIMESTAMP NOT NULL,
    total_at_risk_satoshi BIGINT NOT NULL,
    total_at_risk_usd DECIMAL(20,2),
    active_addresses INTEGER,
    dormant_addresses INTEGER,
    whale_addresses INTEGER, -- > 1000 BTC
    medium_addresses INTEGER, -- 100-1000 BTC
    small_addresses INTEGER, -- < 100 BTC
    risk_score DECIMAL(3,2), -- 0.00 to 1.00
    btc_price_usd DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## Usage

### Setup

1. **Install dependencies**:
   ```bash
   cd quantum_analysis
   pip install -r requirements.txt
   ```

2. **Initialize database**:
   ```bash
   python setup_database.py
   ```

3. **Run analysis**:
   ```bash
   python run_analysis.py
   ```

### Analysis Scripts

#### Basic Statistics
```bash
python basic_stats.py
```
- Total value at risk
- Address distribution
- Balance concentration metrics

#### Anomaly Detection
```bash
python detect_anomalies.py
```
- Spending pattern analysis
- Fee anomaly detection
- Time-based pattern analysis

#### Risk Assessment
```bash
python risk_assessment.py
```
- Comprehensive risk scoring
- Historical trend analysis
- Attack scenario modeling

#### Real-time Monitoring
```bash
python monitor.py
```
- Continuous anomaly monitoring
- Alert generation
- Dashboard updates

## Configuration

Key configuration options in `.env`:

```env
# Analysis Configuration
ANALYSIS_BATCH_SIZE=1000
ANOMALY_THRESHOLD=0.8
RISK_UPDATE_INTERVAL=3600  # 1 hour
BTC_PRICE_API_URL=https://api.coingecko.com/api/v3/simple/price

# Alert Configuration
ENABLE_ALERTS=true
ALERT_EMAIL=admin@example.com
ALERT_WEBHOOK=https://hooks.slack.com/services/...

# Risk Thresholds
WHALE_THRESHOLD=100000000000  # 1000 BTC in satoshis
MEDIUM_THRESHOLD=10000000000  # 100 BTC in satoshis
DORMANT_THRESHOLD_DAYS=365    # 1 year
```

## Output Formats

### Console Output
```
Quantum Vulnerability Analysis Report
====================================
Date: 2025-01-15 14:30:00

VALUE AT RISK:
- Total BTC at Risk: 1,234.56789012 BTC
- USD Value at Risk: $54,321,098.76
- Active Addresses: 1,234
- Dormant Addresses: 156,832

RISK DISTRIBUTION:
- Whale Addresses (>1000 BTC): 5 ($12,345,678.90)
- Medium Addresses (100-1000 BTC): 45 ($8,765,432.10)
- Small Addresses (<100 BTC): 156,947 ($33,209,987.76)

ANOMALY DETECTION:
- Recent Anomalies: 3 (HIGH severity)
- Suspicious Patterns: 12 (MEDIUM severity)
- Risk Score: 0.73 (HIGH)

RECOMMENDATIONS:
- Monitor whale address 1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa
- Investigate spending spike on 2025-01-14
- Consider emergency response protocols
```

### JSON API
```json
{
  "analysis_date": "2025-01-15T14:30:00Z",
  "value_at_risk": {
    "total_btc": 1234.56789012,
    "total_usd": 54321098.76,
    "active_addresses": 1234,
    "dormant_addresses": 156832
  },
  "risk_distribution": {
    "whale_addresses": 5,
    "medium_addresses": 45,
    "small_addresses": 156947
  },
  "anomalies": [
    {
      "date": "2025-01-14T10:15:00Z",
      "type": "spending_spike",
      "severity": "HIGH",
      "description": "Unusual spending activity detected"
    }
  ],
  "risk_score": 0.73
}
```

## Alert System

The analysis system can generate alerts for:

- **Critical Anomalies**: High-confidence suspicious activity
- **Risk Thresholds**: When risk exposure exceeds configured limits
- **Trend Changes**: Significant changes in vulnerability patterns
- **Attack Indicators**: Multiple indicators suggesting active attacks

### Alert Channels
- Email notifications
- Slack/Teams webhooks
- SMS alerts (via Twilio)
- Custom webhook endpoints

## Integration

### With BTCNow Service
- Real-time Bitcoin price integration
- Market impact analysis
- Price correlation with risk metrics

### With P2PK Scanner
- Continuous data updates
- Real-time anomaly detection
- Progressive risk assessment

### External APIs
- CoinGecko for price data
- Blockchain.info for additional metrics
- Custom data sources

## Security Considerations

- **Data Privacy**: All analysis data is stored locally
- **Access Control**: Database access is restricted
- **Audit Logging**: All analysis runs are logged
- **Encryption**: Sensitive data is encrypted at rest

## Performance

- **Analysis Speed**: ~1000 addresses/second
- **Memory Usage**: < 1GB for full analysis
- **Storage**: ~100MB per analysis run
- **Update Frequency**: Configurable (default: hourly)

## Troubleshooting

### Common Issues

**Analysis fails to start:**
- Check database connection
- Verify P2PK scanner data exists
- Check Python dependencies

**Anomaly detection too sensitive:**
- Adjust `ANOMALY_THRESHOLD` in configuration
- Review false positive patterns
- Fine-tune detection algorithms

**Performance issues:**
- Reduce `ANALYSIS_BATCH_SIZE`
- Increase database connection pool
- Optimize query performance

## Future Enhancements

- **Machine Learning**: Advanced anomaly detection using ML models
- **Network Analysis**: Graph-based analysis of address relationships
- **Predictive Modeling**: Forecasting attack probability
- **Real-time Streaming**: Live blockchain monitoring
- **Multi-chain Support**: Extend to other vulnerable cryptocurrencies

## License

Part of the Bitcoin Quantum Vulnerability Scanner project. 