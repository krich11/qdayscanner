#!/usr/bin/env python3
"""
Basic Statistics Analysis for Quantum Vulnerability Assessment.
Analyzes P2PK data to provide key metrics and risk indicators.
"""

import sys
import os
import logging
import json
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

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


class QuantumBasicStats:
    """Analyzes basic statistics for quantum vulnerability assessment."""
    
    def __init__(self):
        self.btc_price_usd = None
        self.analysis_date = datetime.now()
        
    def get_bitcoin_price(self):
        """Fetch current Bitcoin price from CoinGecko API."""
        try:
            import requests
            response = requests.get(
                'https://api.coingecko.com/api/v3/simple/price',
                params={'ids': 'bitcoin', 'vs_currencies': 'usd'},
                timeout=10
            )
            if response.status_code == 200:
                data = response.json()
                self.btc_price_usd = data['bitcoin']['usd']
                logger.info(f"Current Bitcoin price: ${self.btc_price_usd:,.2f}")
                return self.btc_price_usd
        except Exception as e:
            logger.warning(f"Failed to fetch Bitcoin price: {e}")
            # Use a default price for analysis
            self.btc_price_usd = 45000.0
        return self.btc_price_usd
    
    def calculate_total_value_at_risk(self):
        """Calculate total value at risk from vulnerable addresses."""
        try:
            query = """
            SELECT 
                COUNT(*) as total_addresses,
                SUM(current_balance_satoshi) as total_balance_satoshi,
                COUNT(CASE WHEN current_balance_satoshi > 0 THEN 1 END) as active_addresses,
                COUNT(CASE WHEN current_balance_satoshi = 0 THEN 1 END) as empty_addresses
            FROM p2pk_addresses
            """
            
            result = db_manager.execute_query(query)
            if not result:
                logger.error("No data found in p2pk_addresses table")
                return None
                
            stats = result[0]
            total_btc = float(stats['total_balance_satoshi']) / 100000000  # Convert satoshis to BTC
            
            return {
                'total_addresses': stats['total_addresses'],
                'total_balance_satoshi': stats['total_balance_satoshi'],
                'total_balance_btc': total_btc,
                'total_balance_usd': total_btc * self.btc_price_usd if self.btc_price_usd else None,
                'active_addresses': stats['active_addresses'],
                'empty_addresses': stats['empty_addresses']
            }
            
        except Exception as e:
            logger.error(f"Failed to calculate total value at risk: {e}")
            return None
    
    def analyze_balance_distribution(self):
        """Analyze balance distribution across different categories."""
        try:
            # Define balance thresholds (in satoshis)
            whale_threshold = 100000000000  # 1000 BTC
            medium_threshold = 10000000000   # 100 BTC
            
            query = """
            SELECT 
                COUNT(CASE WHEN current_balance_satoshi >= %s THEN 1 END) as whale_addresses,
                COUNT(CASE WHEN current_balance_satoshi >= %s AND current_balance_satoshi < %s THEN 1 END) as medium_addresses,
                COUNT(CASE WHEN current_balance_satoshi > 0 AND current_balance_satoshi < %s THEN 1 END) as small_addresses,
                SUM(CASE WHEN current_balance_satoshi >= %s THEN current_balance_satoshi ELSE 0 END) as whale_balance_satoshi,
                SUM(CASE WHEN current_balance_satoshi >= %s AND current_balance_satoshi < %s THEN current_balance_satoshi ELSE 0 END) as medium_balance_satoshi,
                SUM(CASE WHEN current_balance_satoshi > 0 AND current_balance_satoshi < %s THEN current_balance_satoshi ELSE 0 END) as small_balance_satoshi
            FROM p2pk_addresses
            """
            
            result = db_manager.execute_query(query, (
                whale_threshold, medium_threshold, whale_threshold, medium_threshold,
                whale_threshold, medium_threshold, whale_threshold, medium_threshold
            ))
            
            if not result:
                return None
                
            stats = result[0]
            
            return {
                'whale_addresses': {
                    'count': stats['whale_addresses'],
                    'balance_satoshi': stats['whale_balance_satoshi'],
                    'balance_btc': float(stats['whale_balance_satoshi']) / 100000000,
                    'balance_usd': float(stats['whale_balance_satoshi']) / 100000000 * self.btc_price_usd if self.btc_price_usd else None
                },
                'medium_addresses': {
                    'count': stats['medium_addresses'],
                    'balance_satoshi': stats['medium_balance_satoshi'],
                    'balance_btc': float(stats['medium_balance_satoshi']) / 100000000,
                    'balance_usd': float(stats['medium_balance_satoshi']) / 100000000 * self.btc_price_usd if self.btc_price_usd else None
                },
                'small_addresses': {
                    'count': stats['small_addresses'],
                    'balance_satoshi': stats['small_balance_satoshi'],
                    'balance_btc': float(stats['small_balance_satoshi']) / 100000000,
                    'balance_usd': float(stats['small_balance_satoshi']) / 100000000 * self.btc_price_usd if self.btc_price_usd else None
                }
            }
            
        except Exception as e:
            logger.error(f"Failed to analyze balance distribution: {e}")
            return None
    
    def analyze_dormant_addresses(self, days_threshold=365):
        """Analyze dormant addresses (no recent activity)."""
        try:
            cutoff_date = datetime.now() - timedelta(days=days_threshold)
            
            query = """
            SELECT 
                COUNT(*) as dormant_addresses,
                SUM(current_balance_satoshi) as dormant_balance_satoshi,
                COUNT(CASE WHEN current_balance_satoshi >= 100000000000 THEN 1 END) as dormant_whales,
                COUNT(CASE WHEN current_balance_satoshi >= 10000000000 AND current_balance_satoshi < 100000000000 THEN 1 END) as dormant_medium
            FROM p2pk_addresses a
            WHERE a.last_seen_block < (
                SELECT MAX(block_height) FROM p2pk_transactions 
                WHERE block_time < %s
            )
            AND a.current_balance_satoshi > 0
            """
            
            result = db_manager.execute_query(query, (cutoff_date,))
            
            if not result:
                return None
                
            stats = result[0]
            
            return {
                'dormant_days_threshold': days_threshold,
                'dormant_addresses': stats['dormant_addresses'],
                'dormant_balance_satoshi': stats['dormant_balance_satoshi'],
                'dormant_balance_btc': float(stats['dormant_balance_satoshi']) / 100000000,
                'dormant_balance_usd': float(stats['dormant_balance_satoshi']) / 100000000 * self.btc_price_usd if self.btc_price_usd else None,
                'dormant_whales': stats['dormant_whales'],
                'dormant_medium': stats['dormant_medium']
            }
            
        except Exception as e:
            logger.error(f"Failed to analyze dormant addresses: {e}")
            return None
    
    def calculate_gini_coefficient(self):
        """Calculate Gini coefficient for balance concentration."""
        try:
            query = """
            SELECT current_balance_satoshi
            FROM p2pk_addresses
            WHERE current_balance_satoshi > 0
            ORDER BY current_balance_satoshi
            """
            
            results = db_manager.execute_query(query)
            
            if not results or len(results) < 2:
                return 0.0
            
            balances = [float(row['current_balance_satoshi']) for row in results]
            n = len(balances)
            
            # Calculate Gini coefficient
            sorted_balances = sorted(balances)
            cumsum = 0
            for i, balance in enumerate(sorted_balances):
                cumsum += (i + 1) * balance
            
            gini = (2 * cumsum) / (n * sum(sorted_balances)) - (n + 1) / n
            return max(0, gini)  # Ensure non-negative
            
        except Exception as e:
            logger.error(f"Failed to calculate Gini coefficient: {e}")
            return 0.0
    
    def get_top_vulnerable_addresses(self, limit=10):
        """Get top vulnerable addresses by balance."""
        try:
            query = """
            SELECT 
                address,
                public_key_hex,
                current_balance_satoshi,
                first_seen_block,
                last_seen_block,
                (current_balance_satoshi / 100000000.0) as balance_btc
            FROM p2pk_addresses
            WHERE current_balance_satoshi > 0
            ORDER BY current_balance_satoshi DESC
            LIMIT %s
            """
            
            results = db_manager.execute_query(query, (limit,))
            
            addresses = []
            for row in results:
                addresses.append({
                    'address': row['address'],
                    'public_key_hex': row['public_key_hex'][:20] + '...',  # Truncate for display
                    'balance_satoshi': row['current_balance_satoshi'],
                    'balance_btc': float(row['balance_btc']),
                    'balance_usd': float(row['balance_btc']) * self.btc_price_usd if self.btc_price_usd else None,
                    'first_seen_block': row['first_seen_block'],
                    'last_seen_block': row['last_seen_block']
                })
            
            return addresses
            
        except Exception as e:
            logger.error(f"Failed to get top vulnerable addresses: {e}")
            return []
    
    def calculate_risk_score(self, var_stats, balance_dist, dormant_stats):
        """Calculate overall risk score (0.0 to 1.0)."""
        try:
            risk_factors = []
            
            # Factor 1: Total value at risk (higher = more risk)
            if var_stats and var_stats['total_balance_btc']:
                total_btc = var_stats['total_balance_btc']
                # Normalize: 1000 BTC = 0.5 risk, 10000 BTC = 1.0 risk
                var_risk = min(1.0, total_btc / 10000.0)
                risk_factors.append(var_risk * 0.3)  # 30% weight
            
            # Factor 2: Whale concentration (higher = more risk)
            if balance_dist and balance_dist['whale_addresses']['count'] > 0:
                whale_ratio = balance_dist['whale_addresses']['count'] / var_stats['total_addresses']
                whale_risk = min(1.0, whale_ratio * 100)  # Normalize
                risk_factors.append(whale_risk * 0.25)  # 25% weight
            
            # Factor 3: Dormant addresses (higher = more risk)
            if dormant_stats and var_stats:
                dormant_ratio = dormant_stats['dormant_addresses'] / var_stats['active_addresses']
                dormant_risk = min(1.0, dormant_ratio)
                risk_factors.append(dormant_risk * 0.25)  # 25% weight
            
            # Factor 4: Gini coefficient (higher = more risk)
            gini = self.calculate_gini_coefficient()
            risk_factors.append(gini * 0.2)  # 20% weight
            
            # Calculate weighted average
            if risk_factors:
                risk_score = sum(risk_factors)
                return min(1.0, risk_score)
            
            return 0.5  # Default moderate risk
            
        except Exception as e:
            logger.error(f"Failed to calculate risk score: {e}")
            return 0.5
    
    def save_analysis_results(self, var_stats, balance_dist, dormant_stats, risk_score):
        """Save analysis results to database."""
        try:
            # Save risk assessment
            assessment_query = """
            INSERT INTO risk_assessments 
            (assessment_date, total_at_risk_satoshi, total_at_risk_usd, 
             active_addresses, dormant_addresses, whale_addresses, medium_addresses, small_addresses,
             risk_score, btc_price_usd)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            db_manager.execute_command(assessment_query, (
                self.analysis_date,
                var_stats['total_balance_satoshi'] if var_stats else 0,
                var_stats['total_balance_usd'] if var_stats else None,
                var_stats['active_addresses'] if var_stats else 0,
                dormant_stats['dormant_addresses'] if dormant_stats else 0,
                balance_dist['whale_addresses']['count'] if balance_dist else 0,
                balance_dist['medium_addresses']['count'] if balance_dist else 0,
                balance_dist['small_addresses']['count'] if balance_dist else 0,
                risk_score,
                self.btc_price_usd
            ))
            
            # Save individual metrics
            metrics = [
                ('total_addresses', var_stats['total_addresses'] if var_stats else 0, 'Total P2PK addresses'),
                ('total_balance_btc', var_stats['total_balance_btc'] if var_stats else 0, 'Total BTC at risk'),
                ('gini_coefficient', self.calculate_gini_coefficient(), 'Balance concentration (Gini)'),
                ('whale_count', balance_dist['whale_addresses']['count'] if balance_dist else 0, 'Whale addresses (>1000 BTC)'),
                ('dormant_ratio', dormant_stats['dormant_addresses'] / var_stats['active_addresses'] if dormant_stats and var_stats else 0, 'Dormant address ratio')
            ]
            
            for metric_name, metric_value, description in metrics:
                metric_query = """
                INSERT INTO quantum_analysis_results 
                (analysis_date, metric_name, metric_value, description, risk_level)
                VALUES (%s, %s, %s, %s, %s)
                """
                
                # Determine risk level based on metric
                if metric_name == 'total_balance_btc':
                    risk_level = 'CRITICAL' if metric_value > 1000 else 'HIGH' if metric_value > 100 else 'MEDIUM'
                elif metric_name == 'whale_count':
                    risk_level = 'CRITICAL' if metric_value > 10 else 'HIGH' if metric_value > 5 else 'MEDIUM'
                else:
                    risk_level = 'MEDIUM'
                
                db_manager.execute_command(metric_query, (
                    self.analysis_date, metric_name, metric_value, description, risk_level
                ))
            
            logger.info("Analysis results saved to database")
            
        except Exception as e:
            logger.error(f"Failed to save analysis results: {e}")
    
    def print_report(self, var_stats, balance_dist, dormant_stats, risk_score, top_addresses):
        """Print comprehensive analysis report."""
        print("\n" + "="*80)
        print("QUANTUM VULNERABILITY ANALYSIS REPORT")
        print("="*80)
        print(f"Analysis Date: {self.analysis_date.strftime('%Y-%m-%d %H:%M:%S')}")
        if self.btc_price_usd:
            print(f"Bitcoin Price: ${self.btc_price_usd:,.2f}")
        print()
        
        # Value at Risk
        print("VALUE AT RISK:")
        print("-" * 40)
        if var_stats:
            print(f"Total BTC at Risk: {var_stats['total_balance_btc']:,.8f} BTC")
            if var_stats['total_balance_usd']:
                print(f"USD Value at Risk: ${var_stats['total_balance_usd']:,.2f}")
            print(f"Total Addresses: {var_stats['total_addresses']:,}")
            print(f"Active Addresses: {var_stats['active_addresses']:,}")
            print(f"Empty Addresses: {var_stats['empty_addresses']:,}")
        print()
        
        # Balance Distribution
        print("BALANCE DISTRIBUTION:")
        print("-" * 40)
        if balance_dist:
            whale = balance_dist['whale_addresses']
            medium = balance_dist['medium_addresses']
            small = balance_dist['small_addresses']
            
            print(f"Whale Addresses (>1000 BTC): {whale['count']:,} (${whale['balance_usd']:,.2f})")
            print(f"Medium Addresses (100-1000 BTC): {medium['count']:,} (${medium['balance_usd']:,.2f})")
            print(f"Small Addresses (<100 BTC): {small['count']:,} (${small['balance_usd']:,.2f})")
        print()
        
        # Dormant Analysis
        print("DORMANT ADDRESS ANALYSIS:")
        print("-" * 40)
        if dormant_stats:
            print(f"Dormant Addresses (>1 year): {dormant_stats['dormant_addresses']:,}")
            print(f"Dormant Balance: {dormant_stats['dormant_balance_btc']:,.8f} BTC")
            if dormant_stats['dormant_balance_usd']:
                print(f"Dormant Value: ${dormant_stats['dormant_balance_usd']:,.2f}")
            print(f"Dormant Whales: {dormant_stats['dormant_whales']:,}")
            print(f"Dormant Medium: {dormant_stats['dormant_medium']:,}")
        print()
        
        # Risk Assessment
        print("RISK ASSESSMENT:")
        print("-" * 40)
        gini = self.calculate_gini_coefficient()
        print(f"Overall Risk Score: {risk_score:.3f} ({self._get_risk_level(risk_score)})")
        print(f"Balance Concentration (Gini): {gini:.3f}")
        print()
        
        # Top Vulnerable Addresses
        print("TOP VULNERABLE ADDRESSES:")
        print("-" * 80)
        print(f"{'Rank':<4} {'Address':<35} {'Balance (BTC)':<15} {'Balance (USD)':<15} {'First Block':<12}")
        print("-" * 80)
        
        for i, addr in enumerate(top_addresses[:10], 1):
            balance_usd = f"${addr['balance_usd']:,.2f}" if addr['balance_usd'] else "N/A"
            print(f"{i:<4} {addr['address']:<35} {addr['balance_btc']:<15.8f} {balance_usd:<15} {addr['first_seen_block']:<12}")
        
        print("\n" + "="*80)
    
    def _get_risk_level(self, risk_score):
        """Convert risk score to risk level."""
        if risk_score >= 0.8:
            return "CRITICAL"
        elif risk_score >= 0.6:
            return "HIGH"
        elif risk_score >= 0.4:
            return "MEDIUM"
        else:
            return "LOW"
    
    def run_analysis(self):
        """Run complete analysis and generate report."""
        logger.info("Starting quantum vulnerability analysis...")
        
        # Get Bitcoin price
        self.get_bitcoin_price()
        
        # Calculate statistics
        var_stats = self.calculate_total_value_at_risk()
        balance_dist = self.analyze_balance_distribution()
        dormant_stats = self.analyze_dormant_addresses()
        risk_score = self.calculate_risk_score(var_stats, balance_dist, dormant_stats)
        top_addresses = self.get_top_vulnerable_addresses()
        
        # Save results
        self.save_analysis_results(var_stats, balance_dist, dormant_stats, risk_score)
        
        # Print report
        self.print_report(var_stats, balance_dist, dormant_stats, risk_score, top_addresses)
        
        logger.info("Analysis completed successfully")
        
        return {
            'var_stats': var_stats,
            'balance_dist': balance_dist,
            'dormant_stats': dormant_stats,
            'risk_score': risk_score,
            'top_addresses': top_addresses
        }


def main():
    """Main function."""
    try:
        analyzer = QuantumBasicStats()
        results = analyzer.run_analysis()
        
        # Return results for potential API use
        return results
        
    except Exception as e:
        logger.error(f"Analysis failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 