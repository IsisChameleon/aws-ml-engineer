import yaml
import json
import sys

def convert_yaml_to_json(yaml_file_path, json_file_path):
    try:
        # Read the YAML file
        with open(yaml_file_path, 'r') as yaml_file:
            yaml_content = yaml.safe_load(yaml_file)

        # Convert YAML content to JSON
        with open(json_file_path, 'w') as json_file:
            json.dump(yaml_content, json_file, indent=2)

        print(f"Successfully converted {yaml_file_path} to {json_file_path}")

    except Exception as e:
        print(f"Error converting file: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python convert_yaml_to_json.py <input_yaml_file> <output_json_file>")
    else:
        input_yaml_file = sys.argv[1]
        output_json_file = sys.argv[2]
        convert_yaml_to_json(input_yaml_file, output_json_file)