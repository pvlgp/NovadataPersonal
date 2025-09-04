from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta
import re

# Default arguments from JSON parameters
default_args = {
    'owner': 'sql_team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

# Function to parse SQL script into individual commands
def parse_sql_commands(sql_content):
    """
    Parse SQL content into individual DDL/DML commands separated by semicolons
    """
    commands = []
    current_command = []
    in_string = False
    string_char = None
    in_comment = False
    comment_type = None

    for char in sql_content:
        # Handle comments
        if not in_string and not in_comment and char == '-' and len(current_command) > 0 and current_command[-1] == '-':
            # Start of single-line comment
            in_comment = True
            comment_type = 'single'
            current_command.pop()  # Remove the previous dash
            current_command = []   # Start fresh for comment
        elif not in_string and not in_comment and char == '*' and len(current_command) > 0 and current_command[-1] == '/':
            # Start of multi-line comment
            in_comment = True
            comment_type = 'multi'
            current_command = []   # Start fresh for comment
        elif in_comment and comment_type == 'single' and char == '\n':
            # End of single-line comment
            in_comment = False
            comment_type = None
            current_command = []
        elif in_comment and comment_type == 'multi' and char == '/' and len(current_command) > 0 and current_command[-1] == '*':
            # End of multi-line comment
            in_comment = False
            comment_type = None
            current_command = []
        elif in_comment:
            # Inside comment, skip processing
            current_command.append(char)
            continue

        # Handle strings
        elif not in_comment and char in ['"', "'"] and not in_string:
            in_string = True
            string_char = char
            current_command.append(char)
        elif not in_comment and in_string and char == string_char:
            in_string = False
            string_char = None
            current_command.append(char)
        elif not in_comment and char == ';' and not in_string:
            # End of command
            command_text = ''.join(current_command).strip()
            if command_text and not command_text.isspace():
                commands.append(command_text)
            current_command = []
        else:
            current_command.append(char)

    # Add the last command if exists
    if current_command:
        command_text = ''.join(current_command).strip()
        if command_text and not command_text.isspace():
            commands.append(command_text)

    return commands

# Function to generate meaningful task names
def generate_task_name(sql_command, index):
    """
    Generate meaningful task name based on SQL command content
    """
    sql_command_upper = sql_command.upper()

    if re.search(r'^\s*CREATE\s+TABLE', sql_command_upper):
        table_match = re.search(r'CREATE\s+TABLE\s+([^\s\(]+)', sql_command_upper, re.IGNORECASE)
        if table_match:
            return f"create_table_{table_match.group(1).replace('.', '_')}"

    elif re.search(r'^\s*INSERT\s+INTO', sql_command_upper):
        table_match = re.search(r'INSERT\s+INTO\s+([^\s\(]+)', sql_command_upper, re.IGNORECASE)
        if table_match:
            return f"insert_into_{table_match.group(1).replace('.', '_')}"

    elif re.search(r'^\s*UPDATE\s+', sql_command_upper):
        table_match = re.search(r'UPDATE\s+([^\s]+)', sql_command_upper, re.IGNORECASE)
        if table_match:
            return f"update_{table_match.group(1).replace('.', '_')}"

    elif re.search(r'^\s*DELETE\s+FROM', sql_command_upper):
        table_match = re.search(r'DELETE\s+FROM\s+([^\s]+)', sql_command_upper, re.IGNORECASE)
        if table_match:
            return f"delete_from_{table_match.group(1).replace('.', '_')}"

    elif re.search(r'^\s*SELECT\s+', sql_command_upper):
        return f"select_query_{index}"

    elif re.search(r'^\s*ALTER\s+TABLE', sql_command_upper):
        table_match = re.search(r'ALTER\s+TABLE\s+([^\s]+)', sql_command_upper, re.IGNORECASE)
        if table_match:
            return f"alter_table_{table_match.group(1).replace('.', '_')}"

    elif re.search(r'^\s*DROP\s+TABLE', sql_command_upper):
        table_match = re.search(r'DROP\s+TABLE\s+([^\s]+)', sql_command_upper, re.IGNORECASE)
        if table_match:
            return f"drop_table_{table_match.group(1).replace('.', '_')}"

    # Default fallback
    command_type = re.search(r'^\s*(\w+)', sql_command_upper)
    if command_type:
        return f"{command_type.group(1).lower()}_command_{index}"
    else:
        return f"sql_command_{index}"

with DAG(
    dag_id='generated_sql_select_data',
    default_args=default_args,
    description='Запрос данных',
    schedule_interval='@daily',
    start_date=datetime(2025, 9, 4),
    tags=['data-processing', 'etl'],
    catchup=False,
    max_active_runs=1
) as dag:

    # Parse SQL script into individual commands
    sql_commands = parse_sql_commands("""-- Create table
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100) UNIQUE
);

-- Insert sample data
INSERT INTO users (name, email) VALUES ('John Doe', 'john@example.com');

-- Update data
UPDATE users SET name = 'John Updated' WHERE email = 'john@example.com';

-- Select data (this won't create a task as it's just a query)
SELECT * FROM users WHERE name LIKE '%John%';

-- Create another table
CREATE TABLE user_logs (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    action VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW()
);""")

    # Create tasks for each SQL command
    previous_task = None

    for i, sql_command in enumerate(sql_commands, 1):
        task_id = generate_task_name(sql_command, i)

        # Clean up task ID to be Airflow compatible
        task_id = re.sub(r'[^a-zA-Z0-9_]', '_', task_id)
        task_id = re.sub(r'_+', '_', task_id).strip('_')

        # Ensure task ID is not too long
        if len(task_id) > 50:
            task_id = task_id[:50]

        task = SQLExecuteQueryOperator(
            task_id=task_id,
            conn_id='postgres_default',
            sql=sql_command,
            dag=dag,
            autocommit=True,
            database='None'
        )

        # Set dependencies - sequential execution
        if previous_task:
            previous_task >> task
        previous_task = task

    # If no commands were found (empty script or no semicolons), create a single task
    if not sql_commands:
        task = SQLExecuteQueryOperator(
            task_id="execute_full_sql_script",
            conn_id='postgres_default',
            sql="""-- Create table
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100) UNIQUE
);

-- Insert sample data
INSERT INTO users (name, email) VALUES ('John Doe', 'john@example.com');

-- Update data
UPDATE users SET name = 'John Updated' WHERE email = 'john@example.com';

-- Select data (this won't create a task as it's just a query)
SELECT * FROM users WHERE name LIKE '%John%';

-- Create another table
CREATE TABLE user_logs (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    action VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW()
);""",
            dag=dag,
            autocommit=True,
            database='None'
        )