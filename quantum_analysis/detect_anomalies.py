#!/usr/bin/env python3
"""
Anomaly Detection for Quantum Vulnerability Analysis.
Identifies suspicious patterns that could indicate quantum computer attacks.
"""

import sys
import os
import logging
import json
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import List, Dict, Any, Optional

# Add parent directory to path to import utils
sys.path.append(str(Path(__file__).parent.parent))

from utils.config import config
from utils.database import db_manager

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class AnomalyDetector:
    """Detects anomalies in P2PK address behavior that could indicate quantum attacks."""
    
    def __init__(self):
        self.analysis_date = datetime.now()
        self.anomalies_found = []
        
    def detect_spending_spikes(self, hours_window=24, threshold_multiplier=3.0):
        """Detect unusual spikes in spending activity."""
        try:
            # Get baseline spending rate
            baseline_query = """
            SELECT 
                COUNT(*) as tx_count,
                SUM(amount_satoshi) as total_spent
            FROM p2pk_transactions 
            WHERE is_input = true 
            AND block_time >= %s
            """
            
            # Calculate baseline from last 7 days
            baseline_start = datetime.now() - timedelta(days=7)
            baseline_result = db_manager.execute_query(baseline_query, (baseline_start,))
            
            if not baseline_result:
                return []
            
            baseline = baseline_result[0]
            baseline_tx_count = baseline['tx_count'] if baseline['tx_count'] is not None else 0
            baseline_total_spent = baseline['total_spent'] if baseline['total_spent'] is not None else 0
            avg_daily_tx = baseline_tx_count / 7 if baseline_tx_count else 0
            avg_daily_spent = baseline_total_spent / 7 if baseline_total_spent else 0
            
            # Check recent activity
            recent_start = datetime.now() - timedelta(hours=hours_window)
            recent_query = """
            SELECT 
                COUNT(*) as tx_count,
                SUM(amount_satoshi) as total_spent,
                COUNT(DISTINCT address_id) as unique_addresses
            FROM p2pk_transactions 
            WHERE is_input = true 
            AND block_time >= %s
            """
            
            recent_result = db_manager.execute_query(recent_query, (recent_start,))
            
            if not recent_result:
                return []
            
            recent = recent_result[0]
            recent_tx_count = recent['tx_count'] if recent['tx_count'] is not None else 0
            recent_total_spent = recent['total_spent'] if recent['total_spent'] is not None else 0
            
            # Calculate hourly rates
            hours_in_window = hours_window
            recent_hourly_tx = recent_tx_count / hours_in_window if hours_in_window else 0
            recent_hourly_spent = recent_total_spent / hours_in_window if hours_in_window else 0
            
            baseline_hourly_tx = avg_daily_tx / 24 if avg_daily_tx else 0
            baseline_hourly_spent = avg_daily_spent / 24 if avg_daily_spent else 0
            
            anomalies = []
            
            # Check transaction count spike
            if baseline_hourly_tx > 0 and recent_hourly_tx > baseline_hourly_tx * threshold_multiplier:
                confidence = min(1.0, recent_hourly_tx / (baseline_hourly_tx * threshold_multiplier))
                severity = 'HIGH' if confidence > 0.8 else 'MEDIUM'
                
                anomalies.append({
                    'type': 'spending_spike',
                    'severity': severity,
                    'confidence': confidence,
                    'description': f'Unusual spending spike detected: {recent_hourly_tx:.1f} tx/hour vs baseline {baseline_hourly_tx:.1f} tx/hour',
                    'affected_addresses': recent['unique_addresses'],
                    'affected_balance_satoshi': recent_total_spent,
                    'details': {
                        'recent_hourly_tx': recent_hourly_tx,
                        'baseline_hourly_tx': baseline_hourly_tx,
                        'multiplier': recent_hourly_tx / baseline_hourly_tx if baseline_hourly_tx else 0,
                        'time_window_hours': hours_window
                    }
                })
            
            # Check spending amount spike
            if baseline_hourly_spent > 0 and recent_hourly_spent > baseline_hourly_spent * threshold_multiplier:
                confidence = min(1.0, recent_hourly_spent / (baseline_hourly_spent * threshold_multiplier))
                severity = 'HIGH' if confidence > 0.8 else 'MEDIUM'
                
                anomalies.append({
                    'type': 'amount_spike',
                    'severity': severity,
                    'confidence': confidence,
                    'description': f'Unusual spending amount spike: {recent_hourly_spent/100000000:.2f} BTC/hour vs baseline {baseline_hourly_spent/100000000:.2f} BTC/hour',
                    'affected_addresses': recent['unique_addresses'],
                    'affected_balance_satoshi': recent_total_spent,
                    'details': {
                        'recent_hourly_spent': recent_hourly_spent,
                        'baseline_hourly_spent': baseline_hourly_spent,
                        'multiplier': recent_hourly_spent / baseline_hourly_spent if baseline_hourly_spent else 0,
                        'time_window_hours': hours_window
                    }
                })
            
            return anomalies
            
        except Exception as e:
            logger.error(f"Failed to detect spending spikes: {e}")
            return []
    
    def detect_large_balance_movements(self, threshold_btc=100):
        """Detect large balance movements from vulnerable addresses."""
        try:
            threshold_satoshi = int(threshold_btc * 100000000)
            
            query = """
            SELECT 
                t.txid,
                t.block_time,
                t.amount_satoshi,
                a.address,
                a.current_balance_satoshi as address_balance
            FROM p2pk_transactions t
            JOIN p2pk_addresses a ON t.address_id = a.id
            WHERE t.is_input = true 
            AND t.amount_satoshi >= %s
            AND t.block_time >= %s
            ORDER BY t.block_time DESC
            """
            
            # Check last 24 hours
            recent_start = datetime.now() - timedelta(hours=24)
            results = db_manager.execute_query(query, (threshold_satoshi, recent_start))
            
            anomalies = []
            for row in results:
                amount_btc = float(row['amount_satoshi']) / 100000000
                address_balance_btc = float(row['address_balance']) / 100000000
                
                # Calculate confidence based on size relative to address balance
                if address_balance_btc > 0:
                    ratio = amount_btc / address_balance_btc
                    confidence = min(1.0, ratio)
                else:
                    confidence = 1.0
                
                severity = 'CRITICAL' if amount_btc > 1000 else 'HIGH' if amount_btc > 100 else 'MEDIUM'
                
                anomalies.append({
                    'type': 'large_movement',
                    'severity': severity,
                    'confidence': confidence,
                    'description': f'Large balance movement: {amount_btc:.2f} BTC from {row["address"]}',
                    'affected_addresses': 1,
                    'affected_balance_satoshi': row['amount_satoshi'],
                    'details': {
                        'txid': row['txid'],
                        'amount_btc': amount_btc,
                        'address': row['address'],
                        'address_balance_btc': address_balance_btc,
                        'movement_ratio': ratio if address_balance_btc > 0 else 1.0
                    }
                })
            
            return anomalies
            
        except Exception as e:
            logger.error(f"Failed to detect large balance movements: {e}")
            return []
    
    def detect_fee_anomalies(self, fee_threshold_multiplier=5.0):
        """Detect unusually high fees that might indicate urgency."""
        try:
            # Get average fee from recent transactions
            avg_fee_query = """
            SELECT AVG(fee_satoshi) as avg_fee
            FROM (
                SELECT 
                    t.txid,
                    SUM(CASE WHEN t.is_input THEN t.amount_satoshi ELSE 0 END) - 
                    SUM(CASE WHEN NOT t.is_input THEN t.amount_satoshi ELSE 0 END) as fee_satoshi
                FROM p2pk_transactions t
                WHERE t.block_time >= %s
                GROUP BY t.txid
                HAVING SUM(CASE WHEN t.is_input THEN t.amount_satoshi ELSE 0 END) > 
                       SUM(CASE WHEN NOT t.is_input THEN t.amount_satoshi ELSE 0 END)
                AND SUM(CASE WHEN t.is_input THEN t.amount_satoshi ELSE 0 END) - 
                    SUM(CASE WHEN NOT t.is_input THEN t.amount_satoshi ELSE 0 END) > 0
            ) fee_calc
            """
            
            recent_start = datetime.now() - timedelta(days=7)
            avg_fee_result = db_manager.execute_query(avg_fee_query, (recent_start,))
            
            if not avg_fee_result or not avg_fee_result[0]['avg_fee']:
                return []
            
            avg_fee = float(avg_fee_result[0]['avg_fee'])
            threshold_fee = avg_fee * fee_threshold_multiplier
            
            # Find transactions with unusually high fees
            high_fee_query = """
            SELECT 
                t.txid,
                t.block_time,
                SUM(CASE WHEN t.is_input THEN t.amount_satoshi ELSE 0 END) - 
                SUM(CASE WHEN NOT t.is_input THEN t.amount_satoshi ELSE 0 END) as fee_satoshi,
                COUNT(DISTINCT t.address_id) as address_count
            FROM p2pk_transactions t
            WHERE t.block_time >= %s
            GROUP BY t.txid
            HAVING SUM(CASE WHEN t.is_input THEN t.amount_satoshi ELSE 0 END) > 
                   SUM(CASE WHEN NOT t.is_input THEN t.amount_satoshi ELSE 0 END)
            AND SUM(CASE WHEN t.is_input THEN t.amount_satoshi ELSE 0 END) - 
                SUM(CASE WHEN NOT t.is_input THEN t.amount_satoshi ELSE 0 END) > %s
            ORDER BY fee_satoshi DESC
            """
            
            recent_24h = datetime.now() - timedelta(hours=24)
            results = db_manager.execute_query(high_fee_query, (recent_24h, threshold_fee))
            
            anomalies = []
            for row in results:
                fee_btc = float(row['fee_satoshi']) / 100000000
                fee_multiplier = row['fee_satoshi'] / avg_fee
                
                confidence = min(1.0, fee_multiplier / fee_threshold_multiplier)
                severity = 'HIGH' if fee_multiplier > 10 else 'MEDIUM'
                
                anomalies.append({
                    'type': 'fee_anomaly',
                    'severity': severity,
                    'confidence': confidence,
                    'description': f'Unusually high fee: {fee_btc:.8f} BTC ({fee_multiplier:.1f}x average)',
                    'affected_addresses': row['address_count'],
                    'affected_balance_satoshi': row['fee_satoshi'],
                    'details': {
                        'txid': row['txid'],
                        'fee_btc': fee_btc,
                        'fee_multiplier': fee_multiplier,
                        'avg_fee_btc': avg_fee / 100000000
                    }
                })
            
            return anomalies
            
        except Exception as e:
            logger.error(f"Failed to detect fee anomalies: {e}")
            return []
    
    def detect_time_based_anomalies(self):
        """Detect unusual activity patterns based on time."""
        try:
            # Get activity by hour of day for the last week
            hourly_query = """
            SELECT 
                EXTRACT(HOUR FROM block_time) as hour_of_day,
                COUNT(*) as tx_count,
                COUNT(DISTINCT address_id) as unique_addresses
            FROM p2pk_transactions 
            WHERE is_input = true 
            AND block_time >= %s
            GROUP BY EXTRACT(HOUR FROM block_time)
            ORDER BY hour_of_day
            """
            
            week_ago = datetime.now() - timedelta(days=7)
            hourly_results = db_manager.execute_query(hourly_query, (week_ago,))
            
            if not hourly_results:
                return []
            
            # Calculate average activity per hour
            total_tx = sum(row['tx_count'] for row in hourly_results)
            total_hours = len(hourly_results)
            avg_tx_per_hour = total_tx / total_hours if total_hours > 0 else 0
            
            anomalies = []
            
            # Check for unusual hours (e.g., very early morning activity)
            unusual_hours = [0, 1, 2, 3, 4, 5]  # Midnight to 5 AM
            for row in hourly_results:
                hour = int(row['hour_of_day'])
                if hour in unusual_hours and row['tx_count'] > avg_tx_per_hour * 2:
                    confidence = min(1.0, row['tx_count'] / (avg_tx_per_hour * 2))
                    severity = 'MEDIUM'
                    
                    anomalies.append({
                        'type': 'time_anomaly',
                        'severity': severity,
                        'confidence': confidence,
                        'description': f'Unusual activity at {hour:02d}:00: {row["tx_count"]} transactions',
                        'affected_addresses': row['unique_addresses'],
                        'affected_balance_satoshi': 0,  # We don't have amount info here
                        'details': {
                            'hour_of_day': hour,
                            'tx_count': row['tx_count'],
                            'avg_tx_per_hour': avg_tx_per_hour,
                            'multiplier': row['tx_count'] / avg_tx_per_hour
                        }
                    })
            
            return anomalies
            
        except Exception as e:
            logger.error(f"Failed to detect time-based anomalies: {e}")
            return []
    
    def detect_address_clustering(self, cluster_threshold=5, time_window_hours=1):
        """Detect multiple vulnerable addresses moving funds simultaneously."""
        try:
            # Find transactions within a short time window
            time_window = datetime.now() - timedelta(hours=time_window_hours)
            
            query = """
            SELECT 
                DATE_TRUNC('minute', t.block_time) as minute_time,
                COUNT(DISTINCT t.address_id) as address_count,
                COUNT(*) as tx_count,
                SUM(t.amount_satoshi) as total_amount
            FROM p2pk_transactions t
            WHERE t.is_input = true 
            AND t.block_time >= %s
            GROUP BY DATE_TRUNC('minute', t.block_time)
            HAVING COUNT(DISTINCT t.address_id) >= %s
            ORDER BY address_count DESC
            """
            
            results = db_manager.execute_query(query, (time_window, cluster_threshold))
            
            anomalies = []
            for row in results:
                confidence = min(1.0, row['address_count'] / cluster_threshold)
                severity = 'HIGH' if row['address_count'] > 10 else 'MEDIUM'
                
                anomalies.append({
                    'type': 'address_clustering',
                    'severity': severity,
                    'confidence': confidence,
                    'description': f'Multiple vulnerable addresses active simultaneously: {row["address_count"]} addresses in 1 minute',
                    'affected_addresses': row['address_count'],
                    'affected_balance_satoshi': row['total_amount'],
                    'details': {
                        'minute_time': row['minute_time'].isoformat(),
                        'address_count': row['address_count'],
                        'tx_count': row['tx_count'],
                        'total_amount_btc': float(row['total_amount']) / 100000000,
                        'time_window_hours': time_window_hours
                    }
                })
            
            return anomalies
            
        except Exception as e:
            logger.error(f"Failed to detect address clustering: {e}")
            return []
    
    def detect_whale_activity(self):
        """Detect activity from whale addresses (>1000 BTC)."""
        try:
            whale_threshold = 100000000000  # 1000 BTC in satoshis
            
            query = """
            SELECT 
                t.txid,
                t.block_time,
                t.amount_satoshi,
                a.address,
                a.current_balance_satoshi
            FROM p2pk_transactions t
            JOIN p2pk_addresses a ON t.address_id = a.id
            WHERE t.is_input = true 
            AND a.current_balance_satoshi >= %s
            AND t.block_time >= %s
            ORDER BY t.block_time DESC
            """
            
            recent_24h = datetime.now() - timedelta(hours=24)
            results = db_manager.execute_query(query, (whale_threshold, recent_24h))
            
            anomalies = []
            for row in results:
                amount_btc = float(row['amount_satoshi']) / 100000000
                address_balance_btc = float(row['current_balance_satoshi']) / 100000000
                
                confidence = 1.0  # High confidence for whale activity
                severity = 'CRITICAL'  # Whale activity is always critical
                
                anomalies.append({
                    'type': 'whale_activity',
                    'severity': severity,
                    'confidence': confidence,
                    'description': f'Whale address activity: {amount_btc:.2f} BTC from address with {address_balance_btc:.2f} BTC balance',
                    'affected_addresses': 1,
                    'affected_balance_satoshi': row['amount_satoshi'],
                    'details': {
                        'txid': row['txid'],
                        'amount_btc': amount_btc,
                        'address': row['address'],
                        'address_balance_btc': address_balance_btc
                    }
                })
            
            return anomalies
            
        except Exception as e:
            logger.error(f"Failed to detect whale activity: {e}")
            return []
    
    def save_anomalies(self, anomalies: List[Dict[str, Any]]):
        """Save detected anomalies to database."""
        try:
            for anomaly in anomalies:
                query = """
                INSERT INTO anomaly_events 
                (event_date, event_type, severity, description, affected_addresses, 
                 affected_balance_satoshi, confidence_score, details_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                db_manager.execute_command(query, (
                    self.analysis_date,
                    anomaly['type'],
                    anomaly['severity'],
                    anomaly['description'],
                    anomaly['affected_addresses'],
                    anomaly['affected_balance_satoshi'],
                    anomaly['confidence'],
                    json.dumps(anomaly['details'])
                ))
            
            logger.info(f"Saved {len(anomalies)} anomalies to database")
            
        except Exception as e:
            logger.error(f"Failed to save anomalies: {e}")
    
    def print_anomaly_report(self, anomalies: List[Dict[str, Any]]):
        """Print anomaly detection report."""
        print("\n" + "="*80)
        print("ANOMALY DETECTION REPORT")
        print("="*80)
        print(f"Analysis Date: {self.analysis_date.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Total Anomalies Detected: {len(anomalies)}")
        print()
        
        if not anomalies:
            print("✓ No anomalies detected in the analysis period.")
            print("This could indicate normal activity or no recent suspicious patterns.")
            return
        
        # Group by severity
        critical = [a for a in anomalies if a['severity'] == 'CRITICAL']
        high = [a for a in anomalies if a['severity'] == 'HIGH']
        medium = [a for a in anomalies if a['severity'] == 'MEDIUM']
        low = [a for a in anomalies if a['severity'] == 'LOW']
        
        print("ANOMALY SUMMARY BY SEVERITY:")
        print("-" * 40)
        print(f"CRITICAL: {len(critical)}")
        print(f"HIGH: {len(high)}")
        print(f"MEDIUM: {len(medium)}")
        print(f"LOW: {len(low)}")
        print()
        
        # Show critical and high severity anomalies
        high_priority = critical + high
        if high_priority:
            print("HIGH PRIORITY ANOMALIES:")
            print("-" * 80)
            print(f"{'Type':<20} {'Severity':<10} {'Confidence':<10} {'Description':<40}")
            print("-" * 80)
            
            for anomaly in high_priority:
                desc = anomaly['description'][:38] + '..' if len(anomaly['description']) > 40 else anomaly['description']
                print(f"{anomaly['type']:<20} {anomaly['severity']:<10} {anomaly['confidence']:<10.2f} {desc:<40}")
        
        print("\n" + "="*80)
    
    def run_detection(self):
        """Run complete anomaly detection analysis."""
        logger.info("Starting anomaly detection analysis...")
        
        all_anomalies = []
        
        # Run all detection methods
        detection_methods = [
            ('Spending Spikes', self.detect_spending_spikes),
            ('Large Balance Movements', self.detect_large_balance_movements),
            ('Fee Anomalies', self.detect_fee_anomalies),
            ('Time-based Anomalies', self.detect_time_based_anomalies),
            ('Address Clustering', self.detect_address_clustering),
            ('Whale Activity', self.detect_whale_activity)
        ]
        
        for method_name, method_func in detection_methods:
            logger.info(f"Running {method_name} detection...")
            anomalies = method_func()
            all_anomalies.extend(anomalies)
            logger.info(f"Found {len(anomalies)} {method_name.lower()} anomalies")
        
        # Save anomalies to database
        self.save_anomalies(all_anomalies)
        
        # Print report
        self.print_anomaly_report(all_anomalies)
        
        logger.info(f"Anomaly detection completed. Found {len(all_anomalies)} total anomalies.")
        
        return all_anomalies


def main():
    """Main function."""
    try:
        detector = AnomalyDetector()
        anomalies = detector.run_detection()
        
        # Return results for potential API use
        return anomalies
        
    except Exception as e:
        logger.error(f"Anomaly detection failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 