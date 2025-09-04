"""
DAG Scanner - Automated DAG Generation System for Dockerized Airflow

This DAG scans for scripts in the mounted volumes, generates DAGs using
Airflow Connections for database access, and cleans up processed files.
Optimized for Docker Compose deployment with PostgreSQL connections.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import os
import logging
import glob
import json
from typing import List, Optional, Dict, Any

# Configure logging to use Airflow's log directory
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("/opt/airflow/logs/dag_scanner.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("DAG_Scanner")

class Config:
    """Docker-optimized configuration using Airflow volumes"""
    # Используем стандартные пути Docker Volume из docker-compose
    SCRIPTS_DIR = os.environ.get('SCRIPTS_DIR', '/opt/airflow/scripts')
    JSON_PARAMS_DIR = os.environ.get('JSON_PARAMS_DIR', '/opt/airflow/json_params')
    OUTPUT_DAGS_DIR = os.environ.get('OUTPUT_DAGS_DIR', '/opt/airflow/dags')
    TEMPLATES_DIR = os.environ.get('TEMPLATES_DIR', '/opt/airflow/templates')

    # Email configuration
    ADMIN_EMAIL = os.environ.get('ADMIN_EMAIL', 'admin@example.com')
    NOTIFY_ON_ERRORS = os.environ.get('NOTIFY_ON_ERRORS', 'true').lower() == 'true'

    # Airflow Connection IDs
    POSTGRES_CONN_ID = os.environ.get('POSTGRES_CONN_ID', 'postgres_default')
    EXTRA_DB_CONN_ID = os.environ.get('EXTRA_DB_CONN_ID', 'postgres_extra')

    @classmethod
    def validate_directories(cls):
        """Validate and create required directories with proper permissions"""
        directories = [
            cls.SCRIPTS_DIR, cls.JSON_PARAMS_DIR,
            cls.TEMPLATES_DIR  # OUTPUT_DAGS_DIR создается автоматически Airflow
        ]

        for directory in directories:
            try:
                os.makedirs(directory, exist_ok=True)
                # Устанавливаем правильные permissions для Docker
                os.chmod(directory, 0o775)
                logger.info(f"Directory validated: {directory}")
            except Exception as e:
                logger.error(f"Failed to create directory {directory}: {e}")
                raise

# Initialize configuration
Config.validate_directories()

class DAGGenerator:
    """Docker-optimized DAG generator with Airflow Connections support"""

    def __init__(self, scripts_dir: str, json_params_dir: str,
                 output_dir: str, template_dir: str = "templates"):
        self.scripts_dir = scripts_dir
        self.json_params_dir = json_params_dir
        self.output_dir = output_dir
        self.template_dir = template_dir
        self.logger = logging.getLogger("DAGGenerator")

        try:
            from jinja2 import Environment, FileSystemLoader
            self.env = Environment(
                loader=FileSystemLoader(template_dir),
                trim_blocks=True,
                lstrip_blocks=True
            )
            self.logger.info("Jinja2 environment initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize Jinja2 environment: {e}")
            raise

    def get_database_connection(self, conn_id: str) -> Optional[Dict[str, Any]]:
        """
        Get database connection details from Airflow Connections

        Args:
            conn_id: Airflow Connection ID

        Returns:
            Dictionary with connection details or None if error
        """
        try:
            conn = BaseHook.get_connection(conn_id)
            return {
                'host': conn.host,
                'port': conn.port,
                'database': conn.schema,
                'username': conn.login,
                'password': conn.password,
                'conn_string': f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
            }
        except Exception as e:
            self.logger.error(f"Failed to get connection {conn_id}: {e}")
            return None

    def find_script_files(self) -> List[str]:
        """Find scripts in Docker volume"""
        try:
            self.logger.info(f"Scanning for scripts in: {self.scripts_dir}")
            python_files = glob.glob(os.path.join(self.scripts_dir, "*.py"))
            sql_files = glob.glob(os.path.join(self.scripts_dir, "*.sql"))
            all_files = python_files + sql_files

            self.logger.info(f"Found {len(all_files)} script files")
            return all_files

        except Exception as e:
            self.logger.error(f"Error scanning for script files: {e}")
            return []

    def find_json_params_for_script(self, script_path: str) -> Optional[str]:
        """Find JSON parameters in Docker volume"""
        script_name = os.path.basename(script_path)
        base_name = os.path.splitext(script_name)[0]

        try:
            json_patterns = [
                os.path.join(self.json_params_dir, f"{base_name}.json"),
                os.path.join(self.json_params_dir, f"{base_name}_params.json"),
                os.path.join(self.json_params_dir, f"params_{base_name}.json"),
                os.path.join(self.json_params_dir, f"{base_name}.config.json"),
            ]

            for pattern in json_patterns:
                if os.path.exists(pattern):
                    self.logger.info(f"Found JSON parameters: {os.path.basename(pattern)}")
                    return pattern

            self.logger.warning(f"No JSON parameters found for: {script_name}")
            return None

        except Exception as e:
            self.logger.error(f"Error finding JSON parameters for {script_path}: {e}")
            return None

    def load_template(self, script_type: str):
        """Load template from Docker volume"""
        template_name = f"{script_type}_template.j2"

        try:
            self.logger.info(f"Loading template: {template_name}")
            template = self.env.get_template(template_name)
            self.logger.info("Template loaded successfully")
            return template
        except Exception as e:
            self.logger.error(f"Error loading template {template_name}: {e}")
            return None

    def load_script_content(self, script_path: str) -> Optional[str]:
        """Read script from Docker volume"""
        script_name = os.path.basename(script_path)

        try:
            self.logger.info(f"Reading script: {script_name}")
            with open(script_path, "r", encoding="UTF-8") as f:
                content = f.read()
            self.logger.debug(f"Script content length: {len(content)} characters")
            return content
        except Exception as e:
            self.logger.error(f"Error reading script {script_path}: {e}")
            return None

    def load_json_params(self, json_path: str) -> Optional[Dict[str, Any]]:
        """Load and parse JSON parameters file"""
        json_name = os.path.basename(json_path)

        try:
            self.logger.info(f"Loading JSON parameters: {json_name}")
            with open(json_path, "r", encoding="UTF-8") as f:
                params = json.load(f)

            # Convert string booleans to Python booleans
            for key, value in params.items():
                if isinstance(value, str) and value.lower() in ['true', 'false']:
                    params[key] = value.lower() == 'true'

            self.logger.info(f"JSON parameters loaded successfully")
            return params

        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON format in {json_name}: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Error reading JSON parameters {json_name}: {e}")
            return None

    def generate_dag_id(self, script_name: str) -> str:
        """Generate unique DAG ID based on script name"""
        file_base = os.path.splitext(script_name)[0]
        ext = os.path.splitext(script_name)[1]

        try:
            dag_id = f"generated_{ext[1:] if ext else 'script'}_{file_base}"
            self.logger.info(f"Generated DAG ID: {dag_id}")
            return dag_id
        except Exception as e:
            from datetime import datetime
            fallback_id = f"generated_dag_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            self.logger.warning(f"Error generating DAG ID, using fallback: {fallback_id}")
            return fallback_id

    def get_output_path(self, script_name: str) -> str:
        """Generate output path for generated DAG file"""
        base_name = os.path.splitext(script_name)[0]
        output_path = os.path.join(self.output_dir, f"dag_{base_name}.py")
        self.logger.debug(f"Output path generated: {output_path}")
        return output_path

    def upload_dag(self, content: str, output_path: str) -> bool:
        """Write generated DAG content to file"""
        output_filename = os.path.basename(output_path)

        try:
            output_dir = os.path.dirname(output_path)
            os.makedirs(output_dir, exist_ok=True)

            with open(output_path, "w", encoding="UTF-8") as f:
                f.write(content)

            self.logger.info(f"DAG successfully written to: {output_filename}")
            return True

        except Exception as e:
            self.logger.error(f"Error writing DAG {output_filename}: {e}")
            return False

    def create_dag_for_script(self, script_path: str) -> bool:
        """Create DAG for a single script file"""
        script_name = os.path.basename(script_path)
        script_ext = os.path.splitext(script_name)[1].lower()

        try:
            self.logger.info(f"Processing script: {script_name}")

            # Determine script type
            if script_ext == '.py':
                script_type = 'python'
            elif script_ext == '.sql':
                script_type = 'sql'
            else:
                self.logger.warning(f"Unsupported file type: {script_name}")
                return False

            # Find JSON parameters
            json_path = self.find_json_params_for_script(script_path)
            if not json_path:
                self.logger.warning(f"Skipping {script_name} - no JSON parameters found")
                return False

            # Load all required data
            script_content = self.load_script_content(script_path)
            dag_params = self.load_json_params(json_path)
            template = self.load_template(script_type)

            if not all([script_content, dag_params, template]):
                self.logger.error(f"Failed to load required data for: {script_name}")
                return False

            # Get database connections for SQL templates
            db_connections = {}
            if script_type == 'sql':
                db_connections['postgres_conn'] = self.get_database_connection(Config.POSTGRES_CONN_ID)
                db_connections['extra_db_conn'] = self.get_database_connection(Config.EXTRA_DB_CONN_ID)

            # Generate DAG
            from datetime import datetime
            current_date = datetime.now()

            render_dag = template.render(
                script_content=script_content,
                script_path=script_path,
                script_name=script_name,
                dag_id=self.generate_dag_id(script_name),
                year=current_date.year,
                month=current_date.month,
                day=current_date.day,
                **dag_params,
                **db_connections
            )

            # Save generated DAG
            output_path = self.get_output_path(script_name)
            success = self.upload_dag(render_dag, output_path)

            if success:
                self.logger.info(f"Successfully created DAG for: {script_name}")
            else:
                self.logger.error(f"Failed to create DAG for: {script_name}")

            return success

        except Exception as e:
            self.logger.error(f"Unexpected error creating DAG for {script_name}: {e}")
            return False

def scan_and_generate_dags():
    """Main function to scan directory and generate DAGs"""
    logger.info("Starting DAG scanner execution")

    try:
        generator = DAGGenerator(
            Config.SCRIPTS_DIR,
            Config.JSON_PARAMS_DIR,
            Config.OUTPUT_DAGS_DIR,
            Config.TEMPLATES_DIR
        )

        script_files = generator.find_script_files()

        if not script_files:
            logger.info("No script files found for processing")
            return

        logger.info(f"Found {len(script_files)} script(s) to process")

        processed_scripts = []

        for script_path in script_files:
            script_name = os.path.basename(script_path)

            try:
                json_path = generator.find_json_params_for_script(script_path)

                if json_path:
                    success = generator.create_dag_for_script(script_path)
                    if success:
                        processed_scripts.append((script_path, json_path))
                    else:
                        logger.error(f"Failed to process: {script_name}")
                else:
                    logger.warning(f"Skipping {script_name} - no JSON parameters")

            except Exception as e:
                logger.error(f"Error processing {script_name}: {e}")

        # Cleanup processed files
        cleanup_processed_files(processed_scripts)

        logger.info(f"Processing completed. Success: {len(processed_scripts)}/{len(script_files)}")

    except Exception as e:
        logger.critical(f"Critical error in DAG scanner: {e}")
        raise

def cleanup_processed_files(processed_scripts: List[tuple]):
    """Clean up processed script and JSON files"""
    for script_path, json_path in processed_scripts:
        script_name = os.path.basename(script_path)

        try:
            if os.path.exists(script_path):
                os.remove(script_path)
                logger.info(f"Removed processed script: {script_name}")
        except Exception as e:
            logger.error(f"Failed to remove script {script_name}: {e}")

        try:
            if os.path.exists(json_path):
                os.remove(json_path)
                logger.info(f"Removed JSON parameters: {os.path.basename(json_path)}")
        except Exception as e:
            logger.error(f"Failed to remove JSON {os.path.basename(json_path)}: {e}")

# DAG Definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': Config.NOTIFY_ON_ERRORS,
    'email': [Config.ADMIN_EMAIL],
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG(
    'dag_scanner',
    default_args=default_args,
    description='Automated DAG generation system that scans for scripts and creates DAGs',
    schedule_interval=timedelta(minutes=15),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['automation', 'scanner', 'dag-generation'],
)

scan_task = PythonOperator(
    task_id='scan_and_generate_dags',
    python_callable=scan_and_generate_dags,
    dag=dag,
)

email_task = EmailOperator(
    task_id='send_summary_email',
    to=Config.ADMIN_EMAIL,
    subject='DAG Scanner Execution Summary - {{ ds }}',
    html_content="""<h3>DAG Scanner Execution Completed</h3>
    <p>Execution date: {{ ds }}</p>
    <p>Check logs for detailed results.</p>""",
    dag=dag,
    trigger_rule='one_success'
)

scan_task >> email_task