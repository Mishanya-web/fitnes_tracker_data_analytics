CREATE TABLE IF NOT EXISTS users (
    user_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    phone VARCHAR(20) UNIQUE NOT NULL,
    age INT CHECK (age BETWEEN 15 AND 100),
    weight INT CHECK (weight BETWEEN 30 AND 200),
    height INT CHECK (height BETWEEN 100 AND 250),
    fitness_level VARCHAR(20) CHECK (fitness_level IN ('beginner', 'intermediate', 'advanced')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_users_updated_at
    BEFORE UPDATE ON users
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TABLE IF NOT EXISTS fitness_data (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    user_id INT REFERENCES users(user_id) ON DELETE CASCADE,
    user_name VARCHAR(100),
    user_age INT,
    user_weight INT,

    steps INT NOT NULL CHECK (steps >= 0),
    heart_rate INT NOT NULL CHECK (heart_rate BETWEEN 30 AND 220),
    calories_burned DECIMAL(6, 2) NOT NULL CHECK (calories_burned >= 0),
    activity_type VARCHAR(50) NOT NULL,
    activity_intensity VARCHAR(20),

    sleep_hours DECIMAL(3, 1) CHECK (sleep_hours BETWEEN 0 AND 24),
    sleep_quality INT CHECK (sleep_quality BETWEEN 1 AND 5),
    mood INT CHECK (mood BETWEEN 1 AND 5),
    stress_level INT CHECK (stress_level BETWEEN 1 AND 5),

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE fitness_data IS 'Данные от фитнес-трекеров';
COMMENT ON COLUMN fitness_data.timestamp IS 'Время генерации данных';

CREATE INDEX IF NOT EXISTS idx_fitness_data_timestamp ON fitness_data (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_fitness_data_user_id ON fitness_data (user_id);
CREATE INDEX IF NOT EXISTS idx_fitness_data_activity ON fitness_data (activity_type);

CREATE OR REPLACE VIEW hourly_stats AS
SELECT
    date_trunc('hour', timestamp) as hour,
    user_id,
    user_name,
    AVG(heart_rate) as avg_heart_rate,
    SUM(steps) as total_steps,
    SUM(calories_burned) as total_calories,
    COUNT(*) as records_count
FROM fitness_data
GROUP BY date_trunc('hour', timestamp), user_id, user_name;

CREATE OR REPLACE VIEW daily_stats AS
SELECT
    date(timestamp) as date,
    user_id,
    user_name,
    SUM(steps) as total_steps,
    AVG(heart_rate) as avg_heart_rate,
    SUM(calories_burned) as total_calories,
    COUNT(DISTINCT activity_type) as unique_activities,
    AVG(sleep_hours) as avg_sleep_hours,
    AVG(mood) as avg_mood
FROM fitness_data
GROUP BY date(timestamp), user_id, user_name;

CREATE OR REPLACE FUNCTION prevent_fitness_data_deletion()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'Удаление данных фитнес-трекера запрещено!';
    RETURN NULL;
END;
$$ language 'plpgsql';

CREATE TRIGGER prevent_fitness_data_delete
    BEFORE DELETE ON fitness_data
    FOR EACH ROW
    EXECUTE FUNCTION prevent_fitness_data_deletion();