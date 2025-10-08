"""
Microsoft Fabric Deployment Script
Handles artifact transformation and deployment from Dev to Production
"""

import os
import yaml
import json
import requests
from pathlib import Path
from jsonpath_ng import parse
import base64


class FabricDeployer:
    def __init__(self, environment):
        self.environment = environment
        self.params = self.load_parameters()
        self.access_token = self.get_access_token()
        
    def load_parameters(self):
        """Load configuration from parameters.yml"""
        with open('parameters.yml', 'r') as f:
            return yaml.safe_load(f)
    
    def get_access_token(self):
        """Get Azure AD access token for Fabric API"""
        tenant_id = os.environ.get('AZURE_TENANT_ID')
        client_id = os.environ.get('AZURE_CLIENT_ID')
        client_secret = os.environ.get('AZURE_CLIENT_SECRET')
        
        if not all([tenant_id, client_id, client_secret]):
            raise ValueError("Missing required Azure credentials in environment variables")
        
        url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
        data = {
            'grant_type': 'client_credentials',
            'client_id': client_id,
            'client_secret': client_secret,
            'scope': 'https://analysis.windows.net/powerbi/api/.default'
        }
        
        response = requests.post(url, data=data)
        response.raise_for_status()
        return response.json()['access_token']
    
    def process_find_replace(self):
        """Process simple string replacements in files"""
        print("Processing find_replace rules...")
        
        for rule in self.params.get('find_replace', []):
            find_val = rule['find_value']
            replace_val = rule['replace_value'][self.environment]
            
            print(f"\n  Rule: Replace '{find_val}' with '{replace_val}'")
            
            for file_pattern in rule['file_path']:
                for file_path in Path('.').glob(file_pattern):
                    if file_path.is_file():
                        print(f"    Processing: {file_path}")
                        
                        try:
                            content = file_path.read_text(encoding='utf-8')
                            
                            if find_val in content:
                                content = content.replace(find_val, replace_val)
                                file_path.write_text(content, encoding='utf-8')
                                print(f"      ✓ Replaced successfully")
                            else:
                                print(f"      ⊘ No matches found")
                        except Exception as e:
                            print(f"      ✗ Error: {str(e)}")
    
    def process_key_value_replace(self):
        """Process JSON path-based replacements"""
        print("\nProcessing key_value_replace rules...")
        
        for rule in self.params.get('key_value_replace', []):
            jsonpath_expr = parse(rule['find_key'])
            replace_val = rule['replace_value'][self.environment]
            
            print(f"\n  Rule: JSONPath '{rule['find_key']}'")
            print(f"        Replace with: '{replace_val}'")
            
            for file_pattern in rule['file_path']:
                for file_path in Path('.').glob(file_pattern):
                    if file_path.is_file() and file_path.suffix == '.json':
                        print(f"    Processing: {file_path}")
                        
                        try:
                            with open(file_path, 'r', encoding='utf-8') as f:
                                data = json.load(f)
                            
                            matches = jsonpath_expr.find(data)
                            if matches:
                                for match in matches:
                                    match.full_path.update(data, replace_val)
                                
                                with open(file_path, 'w', encoding='utf-8') as f:
                                    json.dump(data, f, indent=2)
                                print(f"      ✓ Updated {len(matches)} JSON path(s)")
                            else:
                                print(f"      ⊘ No matches found")
                        except Exception as e:
                            print(f"      ✗ Error: {str(e)}")
    
    def deploy_to_fabric(self):
        """Deploy artifacts to Fabric workspace"""
        print("\n" + "="*60)
        print("Deploying to Fabric workspace...")
        print("="*60)
        
        workspace_id = self.params['environments'][self.environment]['workspace_id']
        workspace_name = self.params['environments'][self.environment]['workspace_name']
        
        print(f"\nTarget Workspace: {workspace_name}")
        print(f"Workspace ID: {workspace_id}")
        
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }
        
        # Deploy notebooks
        self.deploy_notebooks(workspace_id, headers)
        
        # Deploy pipelines
        self.deploy_pipelines(workspace_id, headers)
        
        print("\n" + "="*60)
        print("✓ Deployment completed successfully!")
        print("="*60)
    
    def deploy_notebooks(self, workspace_id, headers):
        """Deploy notebook artifacts"""
        print("\nDeploying notebooks...")
        
        notebook_count = 0
        for notebook_path in Path('.').glob('**/*.Notebook'):
            if notebook_path.is_dir():
                notebook_name = notebook_path.name.replace('.Notebook', '')
                content_file = notebook_path / 'notebook-content.py'
                
                if content_file.exists():
                    print(f"\n  Notebook: {notebook_name}")
                    notebook_count += 1
                    
                    try:
                        with open(content_file, 'r', encoding='utf-8') as f:
                            content = f.read()
                        
                        # Create or update notebook via API
                        url = f"{os.environ['FABRIC_API_URL']}/workspaces/{workspace_id}/notebooks"
                        payload = {
                            'displayName': notebook_name,
                            'definition': {
                                'format': 'ipynb',
                                'parts': [
                                    {
                                        'path': 'notebook-content.py',
                                        'payload': base64.b64encode(content.encode()).decode(),
                                        'payloadType': 'InlineBase64'
                                    }
                                ]
                            }
                        }
                        
                        response = requests.post(url, headers=headers, json=payload)
                        if response.status_code in [200, 201]:
                            print(f"    ✓ Deployed successfully")
                        else:
                            print(f"    ✗ Failed: {response.status_code}")
                            print(f"    Response: {response.text}")
                    except Exception as e:
                        print(f"    ✗ Error: {str(e)}")
        
        if notebook_count == 0:
            print("  ⊘ No notebooks found")
    
    def deploy_pipelines(self, workspace_id, headers):
        """Deploy pipeline artifacts"""
        print("\nDeploying pipelines...")
        
        pipeline_count = 0
        for pipeline_path in Path('.').glob('**/*.DataPipeline'):
            if pipeline_path.is_dir():
                pipeline_name = pipeline_path.name.replace('.DataPipeline', '')
                content_file = pipeline_path / 'pipeline-content.json'
                
                if content_file.exists():
                    print(f"\n  Pipeline: {pipeline_name}")
                    pipeline_count += 1
                    
                    try:
                        with open(content_file, 'r', encoding='utf-8') as f:
                            content = json.load(f)
                        
                        # Create or update pipeline via API
                        url = f"{os.environ['FABRIC_API_URL']}/workspaces/{workspace_id}/dataPipelines"
                        payload = {
                            'displayName': pipeline_name,
                            'definition': content
                        }
                        
                        response = requests.post(url, headers=headers, json=payload)
                        if response.status_code in [200, 201]:
                            print(f"    ✓ Deployed successfully")
                        else:
                            print(f"    ✗ Failed: {response.status_code}")
                            print(f"    Response: {response.text}")
                    except Exception as e:
                        print(f"    ✗ Error: {str(e)}")
        
        if pipeline_count == 0:
            print("  ⊘ No pipelines found")
    
    def run(self):
        """Execute full deployment process"""
        print("\n" + "="*60)
        print(f"FABRIC DEPLOYMENT - {self.environment.upper()} ENVIRONMENT")
        print("="*60 + "\n")
        
        self.process_find_replace()
        self.process_key_value_replace()
        self.deploy_to_fabric()


if __name__ == "__main__":
    environment = os.environ.get('TARGET_ENVIRONMENT', 'prod')
    
    try:
        deployer = FabricDeployer(environment)
        deployer.run()
    except Exception as e:
        print(f"\n✗ Deployment failed: {str(e)}")
        exit(1)
