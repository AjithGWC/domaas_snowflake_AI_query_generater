from flask import Flask, request, jsonify
from flask_cors import CORS
from dotenv import load_dotenv
import os
import openai
import json
import re

load_dotenv()

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

openai.api_key = os.getenv("API_KEY")

system_instruction = {
    "role": "system",
    "content": """You are a Snowflake SQL expert assistant and a DOMO Magic ETL transformation translator. Your responsibilities are:

    1. Accurately analyze a JSON object derived from DOMO Magic ETL that describes a data transformation pipeline.

    2. Convert the JSON pipeline into a valid and Snowflake-compatible SQL query that replicates the same logic.

    3. Generate a complete Snowflake SQL script that:
    a. Begins with `USE DATABASE`, `USE SCHEMA`, and `USE WAREHOUSE` setup statements.
    b. Includes `CREATE TABLE IF NOT EXISTS` statements for all intermediate and final tables referenced.
    c. Wraps all transformation logic inside a `CREATE OR REPLACE PROCEDURE` block using:
        - `LANGUAGE SQL`
        - A single `BEGIN...END` block
    d. Performs all logic using only valid Snowflake SQL syntax.

    4. When defining table columns, infer the correct Snowflake data types only.

    5. Do **not** use any invalid types like `LONG`, `INT64`, or other non-Snowflake types.

    6. Do **not** hallucinate missing columns or logic. Only use data and fields present in the input JSON.

    7. Skip GUI-related or ambiguous steps that don't affect SQL logic (e.g., visualization settings or styling).

    8. Do **not** use any placeholders like `<TABLE_NAME>` or `<COLUMN_NAME>`. Always use actual names from the JSON input.

    9. Always respond strictly in the following JSON format (and nothing else):
    {
        "sql": "<entire Snowflake SQL script>"
    }

    10. Do not include any extra commentary, explanation, or additional fields outside the `sql` key in the output.

    11. Ensure the script strictly follows valid Snowflake SQL syntax, including correct data types, proper use of semicolons, valid identifier names (e.g., no spaces unless quoted), and balanced parentheses.

    12. All identifiers (table names, column names, object keys) must be written without spaces and use underscores. However, if the input JSON contains an identifier with spaces, preserve it exactly and wrap it in double quotes (e.g., "User Answer 6"). Do not quote identifiers that do not contain spaces. Never convert names to underscores unless they are already written that way in the input.

    13. **Don't use any of "\n" or "\\n" gave me raw multiline Output **
    IMPORTANT: Always respond in pure JSON with this format and don't add any additional object or any other text:
    {
    "sql": "..."
    }
    """
}


@app.route('/generate-sql', methods=['POST'])
def generate_sql():
    try:
        if not request.is_json:
            return jsonify({"error": "Request body must be in JSON format"}), 400

        data = request.get_json()

        if 'inputJson' not in data:
            return jsonify({"error": "'inputJson' key not found in request body"}), 400

        input_json = data['inputJson']

        if isinstance(input_json, str):
            try:
                input_json = json.loads(input_json)
            except json.JSONDecodeError as e:
                return jsonify({
                    "error": "inputJson string is not a valid JSON object",
                    "details": str(e)
                }), 400

        print("input_json", input_json)
        print("type input_json", type(input_json))
        response = openai.chat.completions.create(
            model="gpt-4-turbo",
            messages=[
                system_instruction,
                {
                    "role": "user",
                    "content": json.dumps(input_json, ensure_ascii=False)
                }
            ]
        )

        response_content = response.choices[0].message.content.strip()
        response_content = re.sub(r"^```(?:json)?|```$", "", response_content.strip(), flags=re.MULTILINE).strip()
        
        if not response_content:
            return jsonify({"error": "Empty response from OpenAI"}), 500

        try:
            response_json = json.loads(response_content)
            return jsonify({
                "Output": response_json.get("sql", "No SQL found.")
            })
        except json.JSONDecodeError as e:
            return jsonify({
                "error": "Invalid JSON returned from OpenAI",
                "raw_output": response_content,
                "details": str(e)
            }), 500

    except Exception as e:
        print("[ERROR]: ", e)
        return jsonify({"error": str(e)}), 500

@app.route("/", methods=['POST', 'GET'])
def home():
    return "Running"

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
