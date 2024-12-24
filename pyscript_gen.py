import google.generativeai as genai
import os
import dotenv
from pathlib import Path

# Load environment variables
dotenv.load_dotenv()

# Configure the Gemini API with the API key
genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))


# Define the SAS to PySpark conversion function for handling multiple scripts
def sas_to_pyspark(sas_code_list):
    try:
        pyspark_code_list = []  # List to store converted PySpark code

        for sas_code in sas_code_list:
            # Define the prompt for Gemini API
            prompt = f"Convert the following SAS script to equivalent PySpark code:\n\n{sas_code}"

            # Get the response from the model
            model = genai.GenerativeModel(model_name="gemini-1.5-pro")
            response = model.generate_content(prompt)

            # Append the result to the list
            pyspark_code_list.append(response.text)

        return pyspark_code_list
    except Exception as e:
        return f"An error occurred: {str(e)}"


# Function to read SAS script from a file
def read_sas_file(file_path):
    with open(file_path, 'r') as file:
        sas_code = file.read()
    return sas_code


# Function to save PySpark code to an output file
def save_pyspark_code(output_path, pyspark_code_list):
    with open(output_path, 'w') as file:
        for idx, pyspark_code in enumerate(pyspark_code_list, 1):
            file.write(f"Generated PySpark Code for SAS Script {idx}:\n")
            file.write(pyspark_code)
            file.write("\n" + "=" * 50 + "\n")


# Main function to handle file upload, SAS to PySpark conversion, and saving the result
def main():
    # User input for SAS script file upload (in this case, we manually specify the path)
    sas_file_path = input("Enter the path to your SAS script file: ").strip()

    # Check if the file exists
    if not os.path.isfile(sas_file_path):
        print("Invalid file path. Please try again.")
        return

    try:
        # Read the SAS script from the uploaded file
        sas_code = read_sas_file(sas_file_path)

        # Split the SAS code into individual scripts (in case the file contains multiple SAS blocks)
        sas_code_list = sas_code.split("\nrun;\n")  # SAS blocks are typically ended by 'run;'

        # Convert the SAS code to PySpark code using Gemini API
        pyspark_code_list = sas_to_pyspark(sas_code_list)

        # Save the generated PySpark code to an output file
        output_file_path = Path(sas_file_path).stem + "_converted_pyspark.py"
        save_pyspark_code(output_file_path, pyspark_code_list)

        # Inform the user of the output location
        print(f"Conversion successful! PySpark code saved to {output_file_path}")

    except Exception as e:
        print(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    main()
