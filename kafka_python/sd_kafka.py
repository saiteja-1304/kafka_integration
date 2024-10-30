import json
import re
import logging
from logging.handlers import RotatingFileHandler
import uuid
from flask import Flask, request, jsonify, g, send_file, send_from_directory
from werkzeug.utils import secure_filename
from langchain.prompts import PromptTemplate
from langchain_openai import AzureOpenAI
from tenacity import retry, stop_after_attempt, wait_fixed
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask_cors import CORS
import jwt
from datetime import datetime, timedelta
import secrets
from functools import wraps
import pandas as pd
import os
from io import BytesIO
import zipfile
import io
import shutil
import dspy

from sd_kafka1 import initialize_kafka_integration

# Logging setup
log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

log_file = 'app1.log'
handler = RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=10)
handler.setFormatter(log_formatter)

console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)

logging.basicConfig(level=logging.INFO, handlers=[handler, console_handler])

logger = logging.getLogger(__name__)

app = Flask(__name__)
kafka_handler = initialize_kafka_integration(app)

@app.route('/trigger', methods=['POST'])
def call_kafka():
    
    kafka_handler.produce_generation_requests()

CORS(app)

SECRET_KEY = secrets.token_hex(16)

UPLOAD_FOLDER = 'uploads'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
ALLOWED_EXTENSIONS = {'xlsx', 'json'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


PROMPT_TEMPLATE = """
You are tasked with generating synthetic data for a loan application system.
The data should be realistic but must not match any real person's information.
The structure may involve one or more templates. Strictly adhere to the given template for data generation.
Here's an example of the JSON structure:

{example}

{s_data_prompt}


1. Generate data specifically for the policy rule: {policy_rule}.
2. Carefully analyze the rules file provided in the **{extra}** section and ensure values in the data align with those rules.
3. Apply both explicit rules (clearly stated conditions) and implicit rules (relationships inferred from the data).
4. Ensure that all generated field values are consistent with the rules and relationships specified in the rules file.
5. Pay attention to any hierarchical or nested rule structures, applying rules in the correct order or priority.
6. If there are conflicting rules, prioritize more specific rules over general ones.
7. For fields not explicitly covered by the rules, generate realistic values that maintain overall consistency with the other fields.
8. Do not use personal information from seed data; instead, use new names and surnames that haven't been used recently. All other policies and rules must remain consistent.
9. Generate an evenly distributed set of results based on the mentioned policies.
10. Strictly adhere to the rules mentioned for the specific policy rule.
11. Use the following field mappings when generating data:
    - For `riskModel.modelIndicator`, use "v4" when the value is "VantageScoreV4."
    - For `premierAttributes.attributeSet.id`, use only the following mappings (and not for other IDs or values):
      IQT9420, ILN0300, ALL2388, REV7110, ALL8220, ALL2800, ALL2488.
12.Do not use firstnames and lastnames that are commonly used such as John Doe.

** As you can see from the structure, the JSON is of the form: {{"request":{{}},"response":{{}}}}.

** When generating data, there should be three responses, i.e., {{"request":{{}},"responses":[{{"response1":{{}},"response2":{{}},"response3":{{}}}}]}}.

** These three responses should be consistent with each other. Common fields such as names, IDs, etc., should remain consistent across the responses.

** Responses should not be identical; keys will change based on the logic (**only some key not whole).

** The logic for generating responses is as follows:

{logic}

** Important logic will override everything in priority , logic must be applied when generating output
The output should be in JSON format and include realistic data for both the `request` and `responses` sections.

** Important Must follow -> the data shouldnt be generic and identical , it should reflect real world data . DO NOT USE GENERIC VALUES FOR FIELDS . THINK RANDOMLY
"""

prompt_template = PromptTemplate(
    input_variables=["example", "s_data_prompt", "extra", "policy_rule", "logic"],
    template=PROMPT_TEMPLATE
)

api_key = "3c5061a5634440d383ec259c088c1450"
azure_endpoint = "https://beta-gpt4o-001.openai.azure.com"
deployment_name = "gpt-4o-001"

llm = dspy.AzureOpenAI(
    deployment_id=deployment_name,
    api_version="2024-02-15-preview",
    api_base=azure_endpoint,
    api_key=api_key,
    temperature=0.9,
    max_tokens=4000
)

dspy.settings.configure(lm=llm)

# Define the DSPy predictor
predict = dspy.Predict("question -> answer_as_json", temperature=0.7, n=10)

def generate_with_dspy(prompt_value):
    prediction = predict(question=prompt_value)
    formatted_results = []
    
    for completion in prediction.completions.answer_as_json:
        if isinstance(completion, str):
            cleaned_json = re.sub(r'^```json\n|\n```$|^```\n|\n```$', '', completion.strip())
            try:
                parsed_json = json.loads(cleaned_json)
                formatted_json = json.dumps(parsed_json, indent=2)
                formatted_results.append(formatted_json)
            except json.JSONDecodeError:
                logger.error(f"Error parsing JSON: {completion}")
                continue
        else:
            try:
                formatted_json = json.dumps(completion, indent=2)
                formatted_results.append(formatted_json)
            except Exception as e:
                logger.error(f"Error formatting JSON: {e}")
                continue
    
    return formatted_results

def create_openai_data_generator(prompt):
    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    def generate_with_retry(example, s_data, extra, policy_rule, n=3):
        logic = "Instead of the key additionalAttributes , in the responses there shouldnt be a key and value additionalAttributes instead make meaningful keys , you can put a key that suits the whole context , you can also remove unneccessary key"
        s_data_prompt = f"The following fields and values must be used consistently across all generated records:\n{s_data}" if s_data else ""
        prompt_value = prompt.format(
            example=json.dumps(example, indent=2),
            s_data_prompt=s_data_prompt,
            logic=logic,
            extra=extra,
            policy_rule=policy_rule
        )

        json_outputs = generate_with_dspy(prompt_value)
        
        validated_outputs = []
        for json_output in json_outputs:
            try:
                parsed_output = json.loads(json_output) if isinstance(json_output, str) else json_output
                validated_outputs.append(parsed_output)
            except ValueError as e:
                logging.error(f"Invalid output structure: {e}")
                continue
        
        if not validated_outputs:
            raise ValueError("All generated outputs were invalid.")
        
        return validated_outputs

    def generate(example, s_data, extra, runs, policy_rule):
        print("TESTING")
        results = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [
                executor.submit(generate_with_retry, example, s_data, extra, policy_rule, n=3)
                for _ in range(runs)
            ]
            
            for future in as_completed(futures):
                try:
                    results.extend(future.result())
                except Exception as e:
                    logger.error(f"Error in generate: {e}")
                    continue
                    
        return results

    return generate

synthetic_data_generator = create_openai_data_generator(prompt_template)

def merge_templates(templates):
    merged = {}
    for template_name, template_content in templates.items():
        merged[template_name] = template_content
    return merged

def extract_values_from_excel(excel_file):
    df = pd.read_excel(excel_file)
    fixed_data = {}

    for _, row in df.iterrows():
        if not pd.isna(row['Field']) and not pd.isna(row['Value']):
            keys = row['Field'].split('.')
            d = fixed_data
            for key in keys[:-1]:
                if key not in d:
                    d[key] = {}
                d = d[key]
            d[keys[-1]] = row['Value']

    logger.info(f"Extracted Excel data: {json.dumps(fixed_data, default=str)}")
    return fixed_data

def extract_rules_values_from_excel(excel_file):
    df = pd.read_excel(excel_file)
    
    data = {
        'flat_data': {},
        'hierarchical_data': []
    }

    current_group = None
    for _, row in df.iterrows():
        row_data = row.dropna().to_dict()
        if row_data:
            if len(row_data) == 1:
                if current_group:
                    data['hierarchical_data'].append(current_group)
                current_group = {
                    'group_name': list(row_data.values())[0],
                    'items': []
                }
            elif current_group:
                current_group['items'].append(row_data)
            else:
                data['hierarchical_data'].append(row_data)
    
    logger.info(f"Extracted Excel data: {json.dumps(data, default=str)}")
    return data

@app.route('/generate', methods=['POST'])
def generate_data():
    try:
        extra_file = os.path.join(app.config['UPLOAD_FOLDER'], 'extra_file.xlsx')
        template_files = [f for f in os.listdir(app.config['UPLOAD_FOLDER']) if f.endswith('_template.json')]
        AddtnlInfo = os.path.join(app.config['UPLOAD_FOLDER'], 'AddtnlInfo.xlsx')

        if not os.path.exists(extra_file) or len(template_files) != 2:
            return jsonify({"error": "Required files not found"}), 400

        templates = {}
        for template_file in template_files:
            with open(os.path.join(app.config['UPLOAD_FOLDER'], template_file), 'r') as f:
                template_name = template_file.replace('_template.json', '')
                templates[template_name] = json.load(f)

        excel_data = extract_values_from_excel(AddtnlInfo) if os.path.exists(AddtnlInfo) else None
        merged_template = merge_templates(templates)
        extra = extract_rules_values_from_excel(extra_file)

        total_runs = 10
        runs_per_policy = total_runs 

        policy_rules = ["CC Policy Rule 12", "CC Policy Rule 13", "CC Policy Rule 14"]
        print("HERE")
        with ThreadPoolExecutor(max_workers=len(policy_rules)) as outer_executor:
            futures = [
                outer_executor.submit(
                    synthetic_data_generator,
                    merged_template,
                    excel_data,
                    extra,
                    runs_per_policy,
                    policy_rule
                )
                for policy_rule in policy_rules
            ]

            synthetic_results = []
            for future in as_completed(futures):
                synthetic_results.extend(future.result())

        output_folder = 'outputs'
        os.makedirs(output_folder, exist_ok=True)

        # Save each request and response separately
        for i, result in enumerate(synthetic_results, 1):
            unique_id = str(uuid.uuid4())

# # Define the output folder with the UUID
            data_folder = os.path.join(output_folder, f'{unique_id}data{i}')
            # data_folder = os.path.join(output_folder, f'data{i}')
            os.makedirs(data_folder, exist_ok=True)

            # Debug: Log the structure of each result to ensure it has the correct format
            logger.info(f"Processing data set {i}, Structure: {json.dumps(result, indent=2)}")

            # Check if result has "request" and "responses"
            if "request" not in result or "responses" not in result:
                logger.error(f"Missing request or responses in data set {i}")
                continue

            # Save the request part as request.json
            request_file = os.path.join(data_folder, 'request.json')
            with open(request_file, 'w') as f:
                json.dump(result["request"], f, indent=2)

            # Save each response separately (checking for each response key)
            responses = result.get("responses", [])
            for j in range(1, 4):
                response_key = f'response{j}'
                response_data = next((r.get(response_key) for r in responses if response_key in r), None)
                if response_data is None:
                    logger.error(f"{response_key} not found in data set {i}")
                    continue
                response_file = os.path.join(data_folder, f'{response_key}.json')
                with open(response_file, 'w') as f:
                    json.dump(response_data, f, indent=2)

        return jsonify(synthetic_results)
    except Exception as e:
        logger.error(f"Error generating data: {e}")
        return jsonify({"error": str(e)}), 500



def generate_jwt_token(user_data):
    payload = {
        'user_data': user_data,
        'exp': datetime.utcnow() + timedelta(minutes=40)
    }
    token = jwt.encode(payload, SECRET_KEY, algorithm='HS256')
    return token

def verify_jwt_token(token):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        logger.info(f"Decoded payload: {payload}")
        return payload['user_data']
    except jwt.ExpiredSignatureError:
        return None
    except jwt.InvalidTokenError:
        return None

def auth_required(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        auth_header = request.headers.get('Authorization')
        
        if auth_header:
            token = auth_header.split(" ")[1]  # Bearer <token>
            user_data = verify_jwt_token(token)
            
            if user_data:
                g.user_data = user_data
                return func(*args, **kwargs)
            else:
                return jsonify({'error': 'Invalid or expired token'}), 401
        else:
            return jsonify({'error': 'Token missing'}), 401
    
    return wrapper

@app.route('/login', methods=['POST'])
def login():
    data = request.json
    username = data.get('username')
    password = data.get('password')
    
    # Hardcoded credentials
    valid_username = 'admin'
    valid_password = 'password123'
    
    if username == valid_username and password == valid_password:
        user_data = {'username': username}
        token = generate_jwt_token(user_data)
        return jsonify({'token': token}), 200
    else:
        return jsonify({'error': 'Invalid credentials'}), 401

@app.route('/upload', methods=['POST'])
@auth_required
def upload_files():
    if 'extra_file' not in request.files:
        return jsonify({"error": "Missing required extra file"}), 400

    extra_file = request.files['extra_file']
    template_files = [file for file in request.files.values() if file.filename.endswith('_template.json')]

    if len(template_files) != 2:
        return jsonify({"error": "Exactly two template files are required"}), 400

    files_to_check = [extra_file] + template_files

    if 'seed_file' in request.files:
        seed_file = request.files['seed_file']
        files_to_check.append(seed_file)

    if not all(allowed_file(f.filename) for f in files_to_check):
        return jsonify({"error": "Invalid file type"}), 400

    try:
        output_folder = 'outputs'
        if os.path.exists(output_folder):
            shutil.rmtree(output_folder)
            logger.info("Removed existing outputs folder")

        # Clear the upload folder
        upload_folder = app.config['UPLOAD_FOLDER']
        if os.path.exists(upload_folder):
            shutil.rmtree(upload_folder)
        os.makedirs(upload_folder, exist_ok=True)

        upload_folder = app.config['UPLOAD_FOLDER']
        os.makedirs(upload_folder, exist_ok=True)
        
        for file in files_to_check:
            filename = secure_filename(file.filename)
            file.save(os.path.join(upload_folder, filename))

        return jsonify({"message": "Files uploaded successfully"}), 200
    except Exception as e:
        logger.error(f"Error uploading files: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/download/<path:foldername>', methods=['GET'])
@auth_required
def download_folder(foldername):
    try:
        output_folder = 'outputs'
        folder_path = os.path.join(output_folder, foldername)
        
        if not os.path.exists(folder_path):
            return jsonify({"error": "Folder not found"}), 404
        
        memory_file = BytesIO()
        with zipfile.ZipFile(memory_file, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, dirs, files in os.walk(folder_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, folder_path)
                    zipf.write(file_path, arcname)
        
        memory_file.seek(0)
        
        return send_file(
            memory_file,
            mimetype='application/zip',
            as_attachment=True,
            download_name=f'{foldername}.zip'
        )
    except Exception as e:
        logger.error(f"Error downloading folder: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/download', methods=['GET'])
@auth_required
def download_outputs():
    try:
        output_folder = 'outputs'
        
        if not os.path.exists(output_folder):
            return jsonify({"error": "Outputs folder not found"}), 404

        memory_file = io.BytesIO()
        with zipfile.ZipFile(memory_file, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, dirs, files in os.walk(output_folder):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, output_folder)
                    zipf.write(file_path, arcname)
        
        memory_file.seek(0)
        
        return send_file(
            memory_file,
            mimetype='application/zip',
            as_attachment=True,
            download_name='outputs.zip'
        )
    except Exception as e:
        logger.error(f"Error downloading outputs: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)