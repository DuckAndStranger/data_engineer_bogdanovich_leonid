CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS topics (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    user_id INTEGER NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS activity_types (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS comments (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    topic_id INTEGER NOT NULL,
    parent_id INTEGER,
    text VARCHAR(1000) NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (topic_id) REFERENCES topics(id),
    FOREIGN KEY (parent_id) REFERENCES comments(id)
);

CREATE TABLE IF NOT EXISTS logs (
    id SERIAL PRIMARY KEY,
    time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    user_id INTEGER,
    activity_type INTEGER NOT NULL,
    activity_id INTEGER,
    server_response INTEGER NOT NULL,
    cookie VARCHAR(32),
    extra VARCHAR(255),
    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (activity_type) REFERENCES activity_types(id)
);

INSERT INTO activity_types (id, name) VALUES 
    (1, 'first_visit'),
    (2, 'registration'),
    (3, 'login'),
    (4, 'logout'),
    (5, 'create_topic'),
    (6, 'view_topic'),
    (7, 'delete_topic'),
    (8, 'create_comment');