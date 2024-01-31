import yaml
import json
import argparse

def main():
    # Initialize parser
    parser = argparse.ArgumentParser(description='Convert YAML to JSON')
    
    # Adding required arguments
    parser.add_argument('input_file', type=str, help='Path to the input YAML file')
    parser.add_argument('output_file', type=str, help='Path to the output JSON file')
    
    # Read arguments from the command line
    args = parser.parse_args()
    
    # Read YAML file
    with open(args.input_file, 'r') as file:
        yaml_content = yaml.safe_load(file)
    
    # Convert to JSON
    # Use indent and sort_keys for pretty printing
    json_content = json.dumps(yaml_content, indent=2, sort_keys=True)
    
    # Write the JSON content to the output file
    with open(args.output_file, 'w') as json_file:
        json_file.write(json_content)
    
    print(f"YAML file {args.input_file} has been converted to JSON file {args.output_file}")

if __name__ == "__main__":
    main()