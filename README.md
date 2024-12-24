###PySparkScriptGen Code Converter
This project provides a script that converts SAS code into equivalent PySpark code using the Google Gemini API. It allows users to upload a file containing multiple SAS scripts, processes them one by one, and saves the converted PySpark code in a new Python file.

Features
SAS to PySpark Conversion: Uses the Google Gemini API to convert multiple SAS code blocks into PySpark code.
Multiple Scripts Handling: Supports reading and converting multiple SAS code blocks in a single file.
Output to Python File: Converts the SAS scripts and saves them as PySpark Python files with clear separation between each script.
Environment Variable Management: Loads sensitive credentials (e.g., Google API Key) from a .env file for security.
Error Handling: Catches errors during the conversion process and provides feedback to the user.

Prerequisites
Python 3.x
Google Gemini API Key

Dependencies
This project requires the following Python libraries:

google-generativeai for integrating with the Google Gemini API.
dotenv for managing environment variables securely.
pathlib for handling file paths.
