#!/usr/bin/env python3
"""
Main Quantum Vulnerability Analysis Runner.
Orchestrates all analysis components and provides comprehensive reporting.
"""

import sys
import os
import logging
import json
from datetime import datetime
from pathlib import Path

# Add parent directory to path to import utils
sys.path.append(str(Path(__file__).parent.parent))

from utils.config import config
from utils.database import db_manager
from basic_stats import QuantumBasicStats
from detect_anomalies import AnomalyDetector

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class QuantumAnalysisRunner:
    """Main runner for quantum vulnerability analysis."""
    
    def __init__(self):
        self.analysis_date = datetime.now()
        self.results = {}
        
    def verify_prerequisites(self):
        """Verify that all prerequisites are met for analysis."""
        try:
            logger.info("Verifying analysis prerequisites...")
            
            # Check if quantum analysis tables exist
            required_tables = [
                'quantum_analysis_results',
                'anomaly_events', 
                'risk_assessments',
                'spending_patterns',
                'address_clusters'
            ]
            
            for table in required_tables:
                if not db_manager.table_exists(table):
                    logger.error(f"Required table '{table}' not found!")
                    logger.error("Please run setup_database.py first.")
                    return False
            
            # Check if P2PK data exists
            if not db_manager.table_exists('p2pk_addresses'):
                logger.error("P2PK addresses table not found!")
                logger.error("Please run the P2PK scanner first to collect data.")
                return False
            
            address_count = db_manager.get_table_count('p2pk_addresses')
            if address_count == 0:
                logger.error("No P2PK addresses found in database!")
                logger.error("Please run the P2PK scanner first to collect data.")
                return False
            
            logger.info(f"✓ Prerequisites verified: {address_count} P2PK addresses available")
            return True
            
        except Exception as e:
            logger.error(f"Failed to verify prerequisites: {e}")
            return False
    
    def run_basic_statistics(self):
        """Run basic statistics analysis."""
        try:
            logger.info("Running basic statistics analysis...")
            
            stats_analyzer = QuantumBasicStats()
            results = stats_analyzer.run_analysis()
            
            self.results['basic_stats'] = results
            logger.info("✓ Basic statistics analysis completed")
            
            return results
            
        except Exception as e:
            logger.error(f"Basic statistics analysis failed: {e}")
            return None
    
    def run_anomaly_detection(self):
        """Run anomaly detection analysis."""
        try:
            logger.info("Running anomaly detection analysis...")
            
            anomaly_detector = AnomalyDetector()
            anomalies = anomaly_detector.run_detection()
            
            self.results['anomalies'] = anomalies
            logger.info(f"✓ Anomaly detection completed: {len(anomalies)} anomalies found")
            
            return anomalies
            
        except Exception as e:
            logger.error(f"Anomaly detection failed: {e}")
            return None
    
    def calculate_overall_risk_score(self):
        """Calculate overall risk score based on all analysis results."""
        try:
            risk_factors = []
            weights = []
            
            # Factor 1: Total value at risk (30% weight)
            if 'basic_stats' in self.results and self.results['basic_stats']:
                var_stats = self.results['basic_stats']['var_stats']
                if var_stats and var_stats['total_balance_btc']:
                    total_btc = var_stats['total_balance_btc']
                    var_risk = min(1.0, total_btc / 10000.0)  # Normalize to 10k BTC
                    risk_factors.append(var_risk)
                    weights.append(0.3)
            
            # Factor 2: Anomaly severity (25% weight)
            if 'anomalies' in self.results and self.results['anomalies']:
                anomaly_scores = []
                for anomaly in self.results['anomalies']:
                    if anomaly['severity'] == 'CRITICAL':
                        anomaly_scores.append(1.0)
                    elif anomaly['severity'] == 'HIGH':
                        anomaly_scores.append(0.8)
                    elif anomaly['severity'] == 'MEDIUM':
                        anomaly_scores.append(0.5)
                    else:
                        anomaly_scores.append(0.2)
                
                if anomaly_scores:
                    avg_anomaly_score = sum(anomaly_scores) / len(anomaly_scores)
                    risk_factors.append(avg_anomaly_score)
                    weights.append(0.25)
            
            # Factor 3: Whale concentration (20% weight)
            if 'basic_stats' in self.results and self.results['basic_stats']:
                balance_dist = self.results['basic_stats']['balance_dist']
                if balance_dist and balance_dist['whale_addresses']['count'] > 0:
                    whale_ratio = balance_dist['whale_addresses']['count'] / self.results['basic_stats']['var_stats']['total_addresses']
                    whale_risk = min(1.0, whale_ratio * 100)
                    risk_factors.append(whale_risk)
                    weights.append(0.2)
            
            # Factor 4: Recent activity (15% weight)
            if 'anomalies' in self.results and self.results['anomalies']:
                recent_anomalies = [a for a in self.results['anomalies'] if a['type'] in ['spending_spike', 'large_movement']]
                if recent_anomalies:
                    activity_risk = min(1.0, len(recent_anomalies) / 10.0)  # Normalize to 10 anomalies
                    risk_factors.append(activity_risk)
                    weights.append(0.15)
            
            # Factor 5: Dormant address ratio (10% weight)
            if 'basic_stats' in self.results and self.results['basic_stats']:
                dormant_stats = self.results['basic_stats']['dormant_stats']
                var_stats = self.results['basic_stats']['var_stats']
                if dormant_stats and var_stats:
                    dormant_ratio = dormant_stats['dormant_addresses'] / var_stats['active_addresses']
                    dormant_risk = min(1.0, dormant_ratio)
                    risk_factors.append(dormant_risk)
                    weights.append(0.1)
            
            # Calculate weighted average
            if risk_factors and weights:
                total_weight = sum(weights)
                weighted_sum = sum(factor * weight for factor, weight in zip(risk_factors, weights))
                overall_risk = weighted_sum / total_weight
                return min(1.0, overall_risk)
            
            return 0.5  # Default moderate risk
            
        except Exception as e:
            logger.error(f"Failed to calculate overall risk score: {e}")
            return 0.5
    
    def generate_recommendations(self):
        """Generate actionable recommendations based on analysis results."""
        try:
            recommendations = []
            
            # Check for critical anomalies
            if 'anomalies' in self.results:
                critical_anomalies = [a for a in self.results['anomalies'] if a['severity'] == 'CRITICAL']
                if critical_anomalies:
                    recommendations.append({
                        'priority': 'IMMEDIATE',
                        'action': 'Investigate critical anomalies immediately',
                        'description': f'Found {len(critical_anomalies)} critical anomalies that require immediate attention',
                        'anomalies': [a['description'] for a in critical_anomalies[:3]]  # Top 3
                    })
            
            # Check for whale activity
            if 'anomalies' in self.results:
                whale_anomalies = [a for a in self.results['anomalies'] if a['type'] == 'whale_activity']
                if whale_anomalies:
                    recommendations.append({
                        'priority': 'HIGH',
                        'action': 'Monitor whale address activity closely',
                        'description': f'Detected {len(whale_anomalies)} whale address movements',
                        'anomalies': [a['description'] for a in whale_anomalies[:3]]
                    })
            
            # Check for high value at risk
            if 'basic_stats' in self.results and self.results['basic_stats']:
                var_stats = self.results['basic_stats']['var_stats']
                if var_stats and var_stats['total_balance_btc'] > 1000:
                    recommendations.append({
                        'priority': 'HIGH',
                        'action': 'Consider emergency response protocols',
                        'description': f'Total value at risk is {var_stats["total_balance_btc"]:.2f} BTC (${var_stats["total_balance_usd"]:,.2f})',
                        'anomalies': []
                    })
            
            # Check for spending spikes
            if 'anomalies' in self.results:
                spending_spikes = [a for a in self.results['anomalies'] if a['type'] == 'spending_spike']
                if spending_spikes:
                    recommendations.append({
                        'priority': 'MEDIUM',
                        'action': 'Monitor spending patterns for escalation',
                        'description': f'Detected {len(spending_spikes)} spending spikes that may indicate urgency',
                        'anomalies': [a['description'] for a in spending_spikes[:2]]
                    })
            
            # General recommendations
            recommendations.append({
                'priority': 'MEDIUM',
                'action': 'Continue regular monitoring',
                'description': 'Maintain continuous monitoring of vulnerable addresses',
                'anomalies': []
            })
            
            return recommendations
            
        except Exception as e:
            logger.error(f"Failed to generate recommendations: {e}")
            return []
    
    def print_comprehensive_report(self):
        """Print comprehensive analysis report."""
        print("\n" + "="*100)
        print("QUANTUM VULNERABILITY ANALYSIS - COMPREHENSIVE REPORT")
        print("="*100)
        print(f"Analysis Date: {self.analysis_date.strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
        # Overall Risk Assessment
        overall_risk = self.calculate_overall_risk_score()
        risk_level = self._get_risk_level(overall_risk)
        
        print("OVERALL RISK ASSESSMENT:")
        print("-" * 50)
        print(f"Risk Score: {overall_risk:.3f} ({risk_level})")
        print()
        
        # Basic Statistics Summary
        if 'basic_stats' in self.results and self.results['basic_stats']:
            var_stats = self.results['basic_stats']['var_stats']
            if var_stats:
                print("VALUE AT RISK SUMMARY:")
                print("-" * 50)
                print(f"Total BTC at Risk: {var_stats['total_balance_btc']:,.8f} BTC")
                if var_stats['total_balance_usd']:
                    print(f"USD Value at Risk: ${var_stats['total_balance_usd']:,.2f}")
                print(f"Total Addresses: {var_stats['total_addresses']:,}")
                print(f"Active Addresses: {var_stats['active_addresses']:,}")
                print()
        
        # Anomaly Summary
        if 'anomalies' in self.results and self.results['anomalies']:
            anomalies = self.results['anomalies']
            critical = len([a for a in anomalies if a['severity'] == 'CRITICAL'])
            high = len([a for a in anomalies if a['severity'] == 'HIGH'])
            medium = len([a for a in anomalies if a['severity'] == 'MEDIUM'])
            low = len([a for a in anomalies if a['severity'] == 'LOW'])
            
            print("ANOMALY DETECTION SUMMARY:")
            print("-" * 50)
            print(f"Total Anomalies: {len(anomalies)}")
            print(f"Critical: {critical}")
            print(f"High: {high}")
            print(f"Medium: {medium}")
            print(f"Low: {low}")
            print()
        
        # Recommendations
        recommendations = self.generate_recommendations()
        if recommendations:
            print("RECOMMENDATIONS:")
            print("-" * 50)
            for i, rec in enumerate(recommendations, 1):
                print(f"{i}. [{rec['priority']}] {rec['action']}")
                print(f"   {rec['description']}")
                if rec['anomalies']:
                    for anomaly in rec['anomalies']:
                        print(f"   - {anomaly}")
                print()
        
        print("="*100)
        print("Analysis completed successfully.")
        print("For detailed results, check the database tables:")
        print("- quantum_analysis_results")
        print("- anomaly_events") 
        print("- risk_assessments")
        print("="*100)
    
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
    
    def run_complete_analysis(self):
        """Run complete quantum vulnerability analysis."""
        try:
            logger.info("Starting complete quantum vulnerability analysis...")
            
            # Verify prerequisites
            if not self.verify_prerequisites():
                logger.error("Prerequisites not met. Analysis cannot proceed.")
                return False
            
            # Run basic statistics
            basic_stats = self.run_basic_statistics()
            if not basic_stats:
                logger.error("Basic statistics analysis failed.")
                return False
            
            # Run anomaly detection
            anomalies = self.run_anomaly_detection()
            if anomalies is None:
                logger.error("Anomaly detection failed.")
                return False
            
            # Calculate overall risk
            overall_risk = self.calculate_overall_risk_score()
            self.results['overall_risk'] = overall_risk
            
            # Generate recommendations
            recommendations = self.generate_recommendations()
            self.results['recommendations'] = recommendations
            
            # Print comprehensive report
            self.print_comprehensive_report()
            
            logger.info("Complete quantum vulnerability analysis finished successfully.")
            return True
            
        except Exception as e:
            logger.error(f"Complete analysis failed: {e}")
            return False


def main():
    """Main function."""
    try:
        runner = QuantumAnalysisRunner()
        success = runner.run_complete_analysis()
        
        if success:
            logger.info("Quantum vulnerability analysis completed successfully.")
            return 0
        else:
            logger.error("Quantum vulnerability analysis failed.")
            return 1
            
    except Exception as e:
        logger.error(f"Analysis runner failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main()) 