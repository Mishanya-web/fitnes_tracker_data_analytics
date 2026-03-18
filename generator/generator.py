import os
import time
import random
import logging
import psycopg2

from contextlib import contextmanager
from datetime import datetime, timedelta
from psycopg2.extras import RealDictCursor
from faker import Faker

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FitnessDataGenerator:
    def __init__(self):
        self.faker = Faker('ru_RU')
        self.conn = None
        self.cur = None

        self.db_config = {
            'dbname': os.getenv('DB_NAME', 'fitness_db'),
            'user': os.getenv('DB_USER', 'user'),
            'password': os.getenv('DB_PASSWORD', 'password'),
            'host': os.getenv('DB_HOST', 'db'),
            'port': '5432'
        }

        self.interval = float(os.getenv('GENERATOR_INTERVAL', '2.0'))
        self.batch_size = int(os.getenv('GENERATOR_BATCH_SIZE', '3'))
        self.history_days = int(os.getenv('GENERATOR_HISTORY_DAYS', '7'))

        self.users = []
        self.current_time = datetime.now()

        self.stats = {
            'records_created': 0,
            'batches_created': 0
        }

        self.activities = {
            'Rest': {
                'step_range': (0, 20),
                'hr_range': (60, 75),
                'calories_per_min': (1.0, 1.8),
                'intensity': 'low',
                'emoji': '😴',
                'probability_weights': {
                    'night': 0.8,
                    'morning': 0.2,
                    'day': 0.3,
                    'evening': 0.25,
                    'weekend_morning': 0.3,
                    'weekend_day': 0.4
                }
            },
            'Walking': {
                'step_range': (70, 130),
                'hr_range': (85, 110),
                'calories_per_min': (3.5, 5.5),
                'intensity': 'moderate',
                'emoji': '🚶',
                'probability_weights': {
                    'night': 0.05,
                    'morning': 0.4,
                    'day': 0.35,
                    'evening': 0.4,
                    'weekend_morning': 0.3,
                    'weekend_day': 0.25
                }
            },
            'Running': {
                'step_range': (150, 200),
                'hr_range': (130, 170),
                'calories_per_min': (8.0, 12.0),
                'intensity': 'high',
                'emoji': '🏃',
                'probability_weights': {
                    'night': 0.01,
                    'morning': 0.25,
                    'day': 0.15,
                    'evening': 0.2,
                    'weekend_morning': 0.2,
                    'weekend_day': 0.15
                }
            },
            'Cycling': {
                'step_range': (0, 0),
                'hr_range': (120, 160),
                'calories_per_min': (7.0, 11.0),
                'intensity': 'high',
                'emoji': '🚴',
                'probability_weights': {
                    'night': 0.02,
                    'morning': 0.1,
                    'day': 0.1,
                    'evening': 0.1,
                    'weekend_morning': 0.1,
                    'weekend_day': 0.1
                }
            },
            'Swimming': {
                'step_range': (0, 0),
                'hr_range': (115, 150),
                'calories_per_min': (6.0, 10.0),
                'intensity': 'moderate',
                'emoji': '🏊',
                'probability_weights': {
                    'night': 0.01,
                    'morning': 0.05,
                    'day': 0.05,
                    'evening': 0.03,
                    'weekend_morning': 0.05,
                    'weekend_day': 0.05
                }
            },
            'Yoga': {
                'step_range': (0, 30),
                'hr_range': (70, 95),
                'calories_per_min': (2.0, 4.0),
                'intensity': 'low',
                'emoji': '🧘',
                'probability_weights': {
                    'night': 0.02,
                    'morning': 0.1,
                    'day': 0.03,
                    'evening': 0.1,
                    'weekend_morning': 0.1,
                    'weekend_day': 0.05
                }
            },
            'Strength Training': {
                'step_range': (10, 50),
                'hr_range': (100, 140),
                'calories_per_min': (4.0, 7.0),
                'intensity': 'moderate',
                'emoji': '🏋️',
                'probability_weights': {
                    'night': 0.01,
                    'morning': 0.1,
                    'day': 0.02,
                    'evening': 0.1,
                    'weekend_morning': 0.1,
                    'weekend_day': 0.05
                }
            }
        }

    @contextmanager
    def get_cursor(self):
        try:
            if not self.conn or self.conn.closed:
                self.conn = psycopg2.connect(**self.db_config)
                self.cur = self.conn.cursor(cursor_factory=RealDictCursor)

            yield self.cur
            self.conn.commit()

        except Exception as e:
            if self.conn:
                self.conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            if self.cur:
                self.cur.close()
            if self.conn:
                self.conn.close()

    def initialize_users(self):
        try:
            with self.get_cursor() as cur:
                cur.execute("SELECT COUNT(*) as count FROM users")
                count = cur.fetchone()['count']

                if count == 0:
                    logger.info("Creating initial users...")
                    user_profiles = [
                        {'age': 25, 'weight': 70, 'height': 175, 'fitness_level': 'advanced'},
                        {'age': 34, 'weight': 82, 'height': 182, 'fitness_level': 'intermediate'},
                        {'age': 28, 'weight': 63, 'height': 168, 'fitness_level': 'intermediate'},
                        {'age': 45, 'weight': 78, 'height': 176, 'fitness_level': 'beginner'},
                        {'age': 31, 'weight': 58, 'height': 165, 'fitness_level': 'beginner'}
                    ]

                    for profile in user_profiles:
                        first_name = self.faker.first_name()
                        last_name = self.faker.last_name()
                        email = self.faker.unique.email()
                        phone = self.faker.unique.phone_number()[:20]

                        cur.execute("""
                            INSERT INTO users 
                            (first_name, last_name, email, phone, age, weight, height, fitness_level)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            RETURNING user_id, first_name, last_name, age, weight, fitness_level
                        """, (first_name, last_name, email, phone,
                              profile['age'], profile['weight'], profile['height'],
                              profile['fitness_level']))

                        user = cur.fetchone()
                        self.users.append(user)
                        logger.info(f"Created user: {first_name} {last_name} (ID: {user['user_id']})")
                else:
                    cur.execute("""
                        SELECT user_id, first_name, last_name, age, weight, fitness_level
                        FROM users
                    """)
                    self.users = cur.fetchall()
                    logger.info(f"Loaded {len(self.users)} existing users")

        except Exception as e:
            logger.error(f"Failed to initialize users: {e}")

    def get_time_period(self, dt):
        hour = dt.hour
        is_weekend = dt.weekday() >= 5

        if is_weekend:
            if 5 <= hour <= 10:
                return 'weekend_morning'
            elif 11 <= hour <= 16:
                return 'weekend_day'
            else:
                return 'evening'
        else:
            if 0 <= hour <= 5:
                return 'night'
            elif 6 <= hour <= 10:
                return 'morning'
            elif 11 <= hour <= 16:
                return 'day'
            else:
                return 'evening'

    def generate_timestamp(self, days_ago=0):
        if days_ago > 0:

            date = datetime.now() - timedelta(days=days_ago)

            date = date.replace(
                hour=random.randint(0, 23),
                minute=random.randint(0, 59),
                second=random.randint(0, 59),
                microsecond=0
            )
            return date
        else:
            return datetime.now() - timedelta(seconds=random.randint(0, 5))

    def generate_fitness_record(self, user, timestamp):
        user_id = user['user_id']

        time_period = self.get_time_period(timestamp)

        activities = []
        weights = []
        for act_name, act_config in self.activities.items():
            activities.append(act_name)
            weights.append(act_config['probability_weights'].get(time_period, 0.1))

        activity_type = random.choices(activities, weights=weights)[0]
        activity_meta = self.activities[activity_type]

        hr_adjustment = {
            'beginner': 1.1,
            'intermediate': 1.0,
            'advanced': 0.9
        }.get(user['fitness_level'], 1.0)

        if activity_meta['step_range'][0] == 0 and activity_meta['step_range'][1] == 0:
            steps = 0
        else:
            steps = random.randint(activity_meta['step_range'][0], activity_meta['step_range'][1])
            steps = int(steps * random.uniform(0.7, 1.3))

        base_hr = random.randint(activity_meta['hr_range'][0], activity_meta['hr_range'][1])
        heart_rate = int(base_hr * hr_adjustment)

        age_factor = 1.0 - (user['age'] - 20) * 0.005
        heart_rate = int(heart_rate * age_factor)

        base_calories = round(random.uniform(activity_meta['calories_per_min'][0],
                                             activity_meta['calories_per_min'][1]), 2)
        weight_factor = user['weight'] / 70
        calories = round(base_calories * weight_factor * random.uniform(0.9, 1.1), 2)

        additional_metrics = {}
        if random.random() < 0.15:
            additional_metrics = {
                'sleep_hours': round(random.uniform(5, 9), 1),
                'sleep_quality': random.randint(1, 5),
                'mood': random.randint(1, 5),
                'stress_level': random.randint(1, 5)
            }

        return {
            'user_id': user_id,
            'steps': steps,
            'heart_rate': heart_rate,
            'calories_burned': calories,
            'activity_type': activity_type,
            'intensity': activity_meta['intensity'],
            'additional_metrics': additional_metrics,
            'timestamp': timestamp
        }

    def insert_record(self, record, user):
        try:
            with self.get_cursor() as cur:
                insert_query = """
                    INSERT INTO fitness_data (
                        timestamp, user_id, user_name, user_age, user_weight,
                        steps, heart_rate, calories_burned, 
                        activity_type, activity_intensity,
                        sleep_hours, sleep_quality, mood, stress_level
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """

                values = (
                    record['timestamp'],
                    record['user_id'],
                    f"{user['first_name']} {user['last_name']}",
                    user['age'],
                    user['weight'],
                    record['steps'],
                    record['heart_rate'],
                    record['calories_burned'],
                    record['activity_type'],
                    record['intensity'],
                    record['additional_metrics'].get('sleep_hours'),
                    record['additional_metrics'].get('sleep_quality'),
                    record['additional_metrics'].get('mood'),
                    record['additional_metrics'].get('stress_level')
                )

                cur.execute(insert_query, values)

                self.stats['records_created'] += 1

                activity_emoji = self.activities[record['activity_type']]['emoji']
                time_str = record['timestamp'].strftime('%Y-%m-%d %H:%M:%S')

                additional_info = ""
                if record['additional_metrics']:
                    additional_info = f" [+{', '.join(record['additional_metrics'].keys())}]"

                logger.info(
                    f"[{time_str}] {activity_emoji} {user['first_name']}: "
                    f"{record['activity_type']:15} | "
                    f"Шаги: {record['steps']:3d} | "
                    f"HR: {record['heart_rate']:3d} | "
                    f"Ккал: {record['calories_burned']:5.2f}{additional_info}"
                )

        except Exception as e:
            logger.error(f"Failed to insert record: {e}")

    def generate_historical_data(self):
        logger.info(f"Generating historical data for the last {self.history_days} days...")

        for day in range(self.history_days, 0, -1):
            records_per_day = random.randint(20, 40)
            logger.info(f"Day {day}: generating {records_per_day} records")

            for _ in range(records_per_day):
                user = random.choice(self.users)
                timestamp = self.generate_timestamp(days_ago=day)
                record = self.generate_fitness_record(user, timestamp)
                self.insert_record(record, user)

            time.sleep(0.5)

        logger.info("Historical data generation completed")

    def generate_realtime_data(self):
        for _ in range(self.batch_size):
            user = random.choice(self.users)
            timestamp = self.generate_timestamp()
            record = self.generate_fitness_record(user, timestamp)
            self.insert_record(record, user)

        self.stats['batches_created'] += 1

    def print_stats(self):
        logger.info("=" * 60)
        logger.info("FITNESS DATA GENERATOR STATISTICS")
        logger.info("=" * 60)

        for key, value in self.stats.items():
            logger.info(f"{key.replace('_', ' ').title()}: {value}")

        logger.info(f"Active users: {len(self.users)}")
        logger.info("=" * 60)

    def run(self):
        logger.info("🚀 Starting Fitness Data Generator...")
        logger.info(f"Configuration: interval={self.interval}s, batch_size={self.batch_size}")

        self.initialize_users()

        if self.stats['records_created'] == 0:
            self.generate_historical_data()

        counter = 0
        try:
            while True:
                self.generate_realtime_data()

                if counter % 50 == 0:
                    self.print_stats()

                time.sleep(self.interval)
                counter += 1

        except KeyboardInterrupt:
            logger.info("\n👋 Generator stopped by user")
            self.print_stats()
        except Exception as e:
            logger.error(f"Generator error: {e}")
            self.print_stats()


if __name__ == "__main__":
    generator = FitnessDataGenerator()
    generator.run()