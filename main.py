import pandas as pd
import pyodbc
import json
import logging
from contextlib import contextmanager

# Simple logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


class SanTrackETL:
    def __init__(self):
        self.risk_weights = {
            'handwashing': -5, 'pests': 10,
            'waste_disposal': 8, 'stagnant_water': 12
        }

    @staticmethod
    @contextmanager
    def get_connection():
        """Your WORKING connection string"""
        CONNECTION_STR = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=localhost\\SQLEXPRESS;"
            "DATABASE=SanTrack;"
            "Trusted_Connection=yes;"
        )
        conn = None
        try:
            conn = pyodbc.connect(CONNECTION_STR)
            yield conn
        except Exception as e:
            print(f"❌ DB Error: {e}")
            raise
        finally:
            if conn:
                conn.close()

    def calculate_risk_score(self, checklist_str):
        """Calculate risk score (0-10, lower = better)"""
        try:
            checklist = json.loads(checklist_str)
            score = 0
            for factor, weight in self.risk_weights.items():
                present = checklist.get(factor, False)
                score += weight * (0 if present else 1)
            return round(min(max(score / 20, 0), 10), 2)
        except:
            return 10.0  # Max risk for bad data

    def run_etl(self):
        """🚀 MAIN ETL PROCESS"""
        print("🚀 Starting SanTrack ETL...")

        processed = 0
        with self.get_connection() as conn:
            # Get pending inspections
            df = pd.read_sql(
                "SELECT id, checklist FROM inspections WHERE risk_score IS NULL",
                conn
            )
            print(f"📊 Found {len(df)} pending inspections")

            if len(df) == 0:
                print("✅ No pending data - ETL up to date!")
                return

            # Process each one
            for _, row in df.iterrows():
                risk_score = self.calculate_risk_score(row['checklist'])

                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE inspections SET risk_score = ?, status = 'scored' WHERE id = ?",
                    (risk_score, row['id'])
                )
                print(f"✅ ID {row['id']}: Risk Score = {risk_score}")
                processed += 1

            conn.commit()
            print(f"\n🎉 ETL COMPLETE! Processed {processed} inspections")

        print("💾 Check your SanTrack database - scores are updated!")


# RUN IT!
if __name__ == "__main__":
    etl = SanTrackETL()
    etl.run_etl()
