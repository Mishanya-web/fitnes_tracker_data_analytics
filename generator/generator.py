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
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'password'),
            'host': os.getenv('DB_HOST', 'db'),
            'port': '5432'
        }

        self.interval = float(os.getenv('GENERATOR_INTERVAL', '2.0'))
        self.history_days = 0

        self.users = []
        self.user_state = {}

        self.stats = {
            'records_created': 0
        }

        self.activities_config = {
            'Rest': {
                'step_rate_per_sec': (0, 0),
                'hr_range': (60, 80),
                'calories_per_sec': (0.015, 0.025),
                'duration_min': (5, 120),
                'probability_weights': {
                    'night': 0.95, 'morning': 0.70, 'day': 0.75, 'evening': 0.80,
                    'weekend_morning': 0.65, 'weekend_day': 0.70, 'weekend_evening': 0.75
                }
            },
            'Walking': {
                'step_rate_per_sec': (1.5, 2.0),
                'hr_range': (95, 120),
                'calories_per_sec': (0.05, 0.08),
                'duration_min': (10, 60),
                'probability_weights': {
                    'night': 0.01, 'morning': 0.60, 'day': 0.55, 'evening': 0.50,
                    'weekend_morning': 0.70, 'weekend_day': 0.60, 'weekend_evening': 0.45
                }
            },
            'Fast Walking': {
                'step_rate_per_sec': (2.0, 2.5),
                'hr_range': (110, 135),
                'calories_per_sec': (0.07, 0.11),
                'duration_min': (15, 45),
                'probability_weights': {
                    'night': 0.005, 'morning': 0.40, 'day': 0.35, 'evening': 0.30,
                    'weekend_morning': 0.50, 'weekend_day': 0.40, 'weekend_evening': 0.25
                }
            },
            'Running': {
                'step_rate_per_sec': (2.5, 3.0),
                'hr_range': (140, 175),
                'calories_per_sec': (0.12, 0.18),
                'duration_min': (5, 30),
                'probability_weights': {
                    'night': 0.002, 'morning': 0.35, 'day': 0.20, 'evening': 0.25,
                    'weekend_morning': 0.45, 'weekend_day': 0.30, 'weekend_evening': 0.20
                }
            },
            'Cycling': {
                'step_rate_per_sec': (0, 0),
                'hr_range': (120, 155),
                'calories_per_sec': (0.10, 0.15),
                'duration_min': (20, 90),
                'probability_weights': {
                    'night': 0.005, 'morning': 0.20, 'day': 0.35, 'evening': 0.30,
                    'weekend_morning': 0.35, 'weekend_day': 0.45, 'weekend_evening': 0.35
                }
            },
            'Yoga': {
                'step_rate_per_sec': (0, 0),
                'hr_range': (70, 100),
                'calories_per_sec': (0.03, 0.06),
                'duration_min': (15, 75),
                'probability_weights': {
                    'night': 0.01, 'morning': 0.35, 'day': 0.25, 'evening': 0.40,
                    'weekend_morning': 0.40, 'weekend_day': 0.35, 'weekend_evening': 0.45
                }
            },
            'Strength Training': {
                'step_rate_per_sec': (0.08, 0.25),
                'hr_range': (100, 140),
                'calories_per_sec': (0.07, 0.12),
                'duration_min': (20, 60),
                'probability_weights': {
                    'night': 0.003, 'morning': 0.30, 'day': 0.20, 'evening': 0.35,
                    'weekend_morning': 0.35, 'weekend_day': 0.30, 'weekend_evening': 0.30
                }
            },
            'Swimming': {
                'step_rate_per_sec': (0, 0),
                'hr_range': (115, 150),
                'calories_per_sec': (0.11, 0.16),
                'duration_min': (20, 60),
                'probability_weights': {
                    'night': 0.001, 'morning': 0.25, 'day': 0.30, 'evening': 0.20,
                    'weekend_morning': 0.30, 'weekend_day': 0.40, 'weekend_evening': 0.25
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
                        {'first_name': 'Алексей', 'last_name': 'Смирнов', 'age': 28, 'weight': 75,
                         'fitness_level': 'advanced'},
                        {'first_name': 'Мария', 'last_name': 'Иванова', 'age': 25, 'weight': 62,
                         'fitness_level': 'intermediate'},
                        {'first_name': 'Дмитрий', 'last_name': 'Кузнецов', 'age': 34, 'weight': 82,
                         'fitness_level': 'intermediate'},
                        {'first_name': 'Елена', 'last_name': 'Петрова', 'age': 45, 'weight': 70,
                         'fitness_level': 'beginner'},
                        {'first_name': 'Сергей', 'last_name': 'Волков', 'age': 31, 'weight': 78,
                         'fitness_level': 'beginner'},
                        {'first_name': 'Анна', 'last_name': 'Соколова', 'age': 22, 'weight': 58,
                         'fitness_level': 'advanced'},
                        {'first_name': 'Игорь', 'last_name': 'Морозов', 'age': 38, 'weight': 85,
                         'fitness_level': 'intermediate'},
                        {'first_name': 'Ольга', 'last_name': 'Новикова', 'age': 29, 'weight': 64,
                         'fitness_level': 'intermediate'},
                        {'first_name': 'Павел', 'last_name': 'Федоров', 'age': 42, 'weight': 88,
                         'fitness_level': 'beginner'},
                        {'first_name': 'Татьяна', 'last_name': 'Морозова', 'age': 33, 'weight': 68,
                         'fitness_level': 'advanced'}
                    ]

                    for profile in user_profiles:
                        email = self.faker.unique.email()
                        phone = self.faker.unique.phone_number()[:20]

                        cur.execute("""
                            INSERT INTO users 
                            (first_name, last_name, email, phone, age, weight, height, fitness_level)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            RETURNING user_id, first_name, last_name, age, weight, fitness_level
                        """, (profile['first_name'], profile['last_name'], email, phone,
                              profile['age'], profile['weight'], 170, profile['fitness_level']))

                        user = cur.fetchone()
                        self.users.append(user)
                        self.initialize_user_state(user)
                        logger.info(f"Created user: {profile['first_name']} {profile['last_name']}")
                else:
                    cur.execute("SELECT user_id, first_name, last_name, age, weight, fitness_level FROM users")
                    self.users = cur.fetchall()
                    for user in self.users:
                        self.initialize_user_state(user)
                    logger.info(f"Loaded {len(self.users)} existing users")

        except Exception as e:
            logger.error(f"Failed to initialize users: {e}")

    def initialize_user_state(self, user):
        user_id = user['user_id']
        self.user_state[user_id] = {
            'total_steps': 0,
            'total_calories': 0,
            'current_activity': 'Rest',
            'activity_start_time': datetime.now(),
            'last_heart_rate': random.randint(65, 75),
            'last_date': datetime.now().date()
        }

    def check_daily_reset(self, user_id, current_time):
        state = self.user_state[user_id]
        current_date = current_time.date()
        if current_date > state['last_date']:
            logger.debug(
                f"User {user_id}: Day reset - Steps: {state['total_steps']}, Calories: {state['total_calories']:.0f}")
            state['total_steps'] = 0
            state['total_calories'] = 0
            state['last_date'] = current_date
            state['current_activity'] = 'Rest'

    def get_time_period(self, dt):
        hour = dt.hour
        is_weekend = dt.weekday() >= 5
        if is_weekend:
            if 5 <= hour <= 10:
                return 'weekend_morning'
            elif 11 <= hour <= 16:
                return 'weekend_day'
            else:
                return 'weekend_evening'
        else:
            if 0 <= hour <= 5:
                return 'night'
            elif 6 <= hour <= 10:
                return 'morning'
            elif 11 <= hour <= 16:
                return 'day'
            else:
                return 'evening'

    def update_activity(self, user, current_time):
        user_id = user['user_id']
        state = self.user_state[user_id]
        time_period = self.get_time_period(current_time)

        if state['current_activity'] != 'Rest':
            duration = (current_time - state['activity_start_time']).total_seconds() / 60
            max_duration = self.activities_config[state['current_activity']]['duration_min'][1]
            if duration > max_duration:
                state['current_activity'] = 'Rest'
                return

        if state['current_activity'] == 'Rest':
            base_prob = self.activities_config['Rest']['probability_weights'].get(time_period, 0.5)
            fitness_factor = {'beginner': 0.7, 'intermediate': 1.0, 'advanced': 1.3}.get(user['fitness_level'], 1.0)

            if random.random() < base_prob * fitness_factor:
                activities = [a for a in self.activities_config.keys() if a != 'Rest']
                weights = []
                for activity in activities:
                    weight = self.activities_config[activity]['probability_weights'].get(time_period, 0.1)
                    if user['fitness_level'] == 'beginner' and activity in ['Running', 'Cycling', 'Swimming']:
                        weight *= 0.5
                    elif user['fitness_level'] == 'advanced' and activity in ['Running', 'Cycling', 'Swimming']:
                        weight *= 1.4
                    weights.append(weight)

                total = sum(weights)
                if total > 0:
                    weights = [w / total for w in weights]
                    new_activity = random.choices(activities, weights=weights)[0]
                    state['current_activity'] = new_activity
                    state['activity_start_time'] = current_time
        else:
            duration = (current_time - state['activity_start_time']).total_seconds() / 60
            if random.random() < min(0.2, duration / 90):
                state['current_activity'] = 'Rest'

    def update_metrics(self, user, seconds_passed):
        user_id = user['user_id']
        state = self.user_state[user_id]
        activity = state['current_activity']
        config = self.activities_config[activity]

        step_rate_min, step_rate_max = config['step_rate_per_sec']
        if step_rate_max > 0:
            step_rate = random.uniform(step_rate_min, step_rate_max)
            steps_added = int(step_rate * seconds_passed)
            state['total_steps'] += steps_added

        cal_rate_min, cal_rate_max = config['calories_per_sec']
        cal_rate = random.uniform(cal_rate_min, cal_rate_max)
        cal_rate *= (user['weight'] / 70)
        calories_added = cal_rate * seconds_passed
        state['total_calories'] += calories_added

        return calories_added

    def calculate_heart_rate(self, user, current_time):
        user_id = user['user_id']
        state = self.user_state[user_id]
        activity = state['current_activity']
        config = self.activities_config[activity]

        resting_hr = {'beginner': 70, 'intermediate': 65, 'advanced': 60}.get(user['fitness_level'], 65)
        resting_hr = int(resting_hr * (1.0 + (user['age'] - 30) * 0.003))

        if activity == 'Rest':
            target_hr = resting_hr + random.randint(-3, 3)
        else:
            duration = (current_time - state['activity_start_time']).total_seconds() / 60
            duration_factor = min(1.0, duration / 15)
            hr_min, hr_max = config['hr_range']
            fitness_factor = {'beginner': 1.08, 'intermediate': 1.0, 'advanced': 0.92}.get(user['fitness_level'], 1.0)
            target_hr = hr_min + (hr_max - hr_min) * duration_factor
            target_hr *= fitness_factor
            target_hr = int(target_hr) + random.randint(-3, 3)

        max_change = 4
        if state['last_heart_rate']:
            diff = target_hr - state['last_heart_rate']
            if abs(diff) > max_change:
                target_hr = state['last_heart_rate'] + (max_change if diff > 0 else -max_change)

        target_hr = max(45, min(200, target_hr))
        state['last_heart_rate'] = target_hr
        return target_hr

    def generate_fitness_record(self, user, current_time, seconds_passed):
        calories_added = self.update_metrics(user, seconds_passed)
        heart_rate = self.calculate_heart_rate(user, current_time)
        state = self.user_state[user['user_id']]

        return {
            'user_id': user['user_id'],
            'steps': state['total_steps'],
            'heart_rate': heart_rate,
            'calories_burned': state['total_calories'],
            'calories_added': calories_added,
            'activity_type': state['current_activity'],
            'timestamp': current_time
        }

    def insert_record(self, record, user):
        try:
            with self.get_cursor() as cur:
                cur.execute("""
                    INSERT INTO fitness_data 
                    (timestamp, user_id, user_name, user_age, user_weight, steps, heart_rate, calories_burned, activity_type)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    record['timestamp'], record['user_id'],
                    f"{user['first_name']} {user['last_name']}",
                    user['age'], user['weight'],
                    record['steps'], record['heart_rate'],
                    record['calories_burned'], record['activity_type']
                ))
                self.stats['records_created'] += 1

                time_str = record['timestamp'].strftime('%H:%M:%S')
                logger.info(
                    f"[{time_str}] {user['first_name']:10} ({user['fitness_level']:12}): "
                    f"{record['activity_type']:16} | "
                    f"Шаги: {record['steps']:5d} | "
                    f"HR: {record['heart_rate']:3d} | "
                    f"Ккал: {record['calories_burned']:6.0f} (+{record['calories_added']:.1f})"
                )
        except Exception as e:
            logger.error(f"Failed to insert record: {e}")

    def run(self):
        logger.info(f"🚀 Starting Fitness Data Generator (NO HISTORICAL DATA)")
        logger.info(f"Configuration: interval={self.interval}s")

        self.initialize_users()

        self.last_generation_time = None

        try:
            while True:
                start = time.time()
                current_time = datetime.now()

                if not self.last_generation_time:
                    seconds_passed = self.interval
                else:
                    seconds_passed = min((current_time - self.last_generation_time).total_seconds(), 10.0)

                user = random.choice(self.users)
                self.check_daily_reset(user['user_id'], current_time)
                self.update_activity(user, current_time)
                record = self.generate_fitness_record(user, current_time, seconds_passed)
                self.insert_record(record, user)

                self.last_generation_time = current_time

                elapsed = time.time() - start
                time.sleep(max(0, self.interval - elapsed))

        except KeyboardInterrupt:
            logger.info("\n👋 Generator stopped")
            self.print_stats()

    def print_stats(self):
        logger.info(f"Total records created: {self.stats['records_created']}")


if __name__ == "__main__":
    generator = FitnessDataGenerator()
    generator.run()