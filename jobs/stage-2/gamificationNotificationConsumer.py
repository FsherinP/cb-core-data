from pathlib import Path
import psycopg2
import requests
import os
import sys
sys.path.append(str(Path(__file__).resolve().parents[2]))

from jobs.default_config import create_config
from jobs.config import get_environment_config
class GamificationNotificationConsumer:
    def __init__(self, config):
        self.config = config
        self.class_name = "org.ekstep.analytics.dashboard.report.GamificationNotificationConsumer"

    def get_db_connection(self):
        host, port = self.config.dwPostgresHost.split(":")
        return psycopg2.connect(
        host=host,
        port=int(port),
        database=self.config.dwPostgresSchema,
        user=self.config.dwPostgresUsername,
        password=self.config.dwPostgresCredential
    )

    def process_batch(self, conn):
        
        cursor = conn.cursor()
        query = f"""
            UPDATE {self.config.dwnotificationQueue} 
            SET status = 'PROCESSING', updated_at = NOW()
            WHERE notification_id IN (
                SELECT notification_id FROM {self.config.dwnotificationQueue}
                WHERE event_type = 'gamification' 
                AND status = 'PENDING'
                ORDER BY created_at ASC 
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            )
            RETURNING notification_id, user_id, payload;
        """
        cursor.execute(query, (self.config.gamificationNotificationBatchSize,))
        rows = cursor.fetchall()

        if not rows:
            return 0
        
        success_count = 0
        for row in rows:
            (notification_id, user_id, payload) = row
            try:
                response = requests.post(self.config.gamificationNotificationEndpoint, json=payload, timeout=300)
                if response.status_code == 200:
                    cursor.execute(f"""UPDATE {self.config.dwnotificationQueue} SET status = 'SENT', error_message = %s, updated_at = NOW() WHERE notification_id = %s""", (response.text, notification_id))
                    success_count += 1
                else:
                    cursor.execute(
                        f"UPDATE {self.config.dwnotificationQueue} SET status = 'FAILED', error_message = %s, updated_at = NOW() WHERE notification_id = %s",
                        (response.text, notification_id)
                    )

            except Exception as e:
                cursor.execute(f"""UPDATE {self.config.dwnotificationQueue} SET status = 'FAILED', error_message = %s, updated_at = NOW() WHERE notification_id = %s""", (str(e), notification_id))
        conn.commit()
        return success_count
def main():
    config_dict = get_environment_config()
    config = create_config(config_dict)
    model = GamificationNotificationConsumer(config)
    conn = None
    try: 
        conn = model.get_db_connection()
        print("[INFO] Starting Gamification Notification Consumer...")

        count = model.process_batch(conn)
        if count:
            print(f"[INFO] Processed {count} Gamification notifications.")
        else:
            print("[INFO] No pending notifications found in this batch.")
    except Exception as e:
        print(f"[ERROR] An error occurred: {str(e)}")
    finally:
        print("Closing database connection")
        if conn:
            conn.close()
if __name__ == "__main__":
    main()