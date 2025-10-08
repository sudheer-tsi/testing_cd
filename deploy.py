"""
Microsoft Fabric Deployment Script
Handles artifact transformation and deployment from Dev to Production
Uses fabric_cicd library for deployment
"""

import os
import yaml
import json
import csv
from pathlib import Path
from jsonpath_ng import parse
from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items


class FabricDeployer:
    def __init__(self, environment):
        self.environment = environment
        self.params = self.load_parameters()
        self.repo_dir = "."
        self.item_types = ["Lakehouse", "Notebook", "Environment", "Warehouse", "DataPipeline", "SemanticModel", "Report"]
        
    def load_parameters(self):
        """Load configuration from parameters.yml"""
        params_file = Path('parameters.yml')
        if params_file.exists():
            with open(params_file, 'r') as f:
                return yaml.safe_load(f)
        return {}
    
    def get_workspace_id_by_env(self, env: str, csv_path: str) -> str:
        """Get workspace ID from CSV file based on environment"""
        env = env.strip().lower()
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("Environment", "").strip().lower() == env:
                    workspace_id = row.get("Workspace ID", "").strip()
                    if not workspace_id:
                        raise ValueError(f"Workspace ID missing for env={env} in {csv_path}")
                    return workspace_id
        raise ValueError(f"No row for env={env} in {csv_path}")
    
    def get_workspace_info(self):
        """Get workspace ID and name for current environment"""
        # Try to get from CSV first
        csv_file = Path("workspace_data.csv")
        if csv_file.exists():
            workspace_id = self.get_workspace_id_by_env(self.environment, csv_file)
            workspace_name = f"{self.environment.upper()} Workspace"
        # Fallback to parameters.yml
        elif self.params and 'environments' in self.params:
            env_config = self.params['environments'].get(self.environment, {})
            workspace_id = env_config.get('workspace_id')
            workspace_name = env_config.get('workspace_name', f"{self.environment.upper()} Workspace")
            if not workspace_id:
                raise ValueError(f"Workspace ID not found for environment: {self.environment}")
        else:
            raise ValueError("No workspace configuration found. Provide either workspace_data.csv or parameters.yml")
        
        return workspace_id, workspace_name
    
    def process_find_replace(self):
        """Process simple string replacements in files"""
        if not self.params.get('find_replace'):
            print("No find_replace rules found, skipping...")
            return
            
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
        if not self.params.get('key_value_replace'):
            print("No key_value_replace rules found, skipping...")
            return
            
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
    
    def check_reserved_names(self):
        """Check for reserved names in notebooks and warn user"""
        print("\nChecking for reserved names...")
        reserved_keywords = ['final', 'temp', 'system', 'log', 'metadata']
        found_issues = False
        
        for notebook_path in Path('.').glob('**/*.Notebook'):
            if notebook_path.is_dir():
                notebook_name = notebook_path.name.replace('.Notebook', '')
                
                if any(keyword in notebook_name.lower() for keyword in reserved_keywords):
                    print(f"  ⚠ WARNING: '{notebook_name}' may contain reserved keywords")
                    print(f"    Consider renaming to avoid deployment failures")
                    found_issues = True
        
        if not found_issues:
            print("  ✓ No reserved name issues found")
        
        return found_issues
    
    def deploy_to_fabric(self):
        """Deploy artifacts to Fabric workspace using fabric_cicd"""
        print("\n" + "="*60)
        print("Deploying to Fabric workspace...")
        print("="*60)
        
        workspace_id, workspace_name = self.get_workspace_info()
        
        print(f"\nTarget Workspace: {workspace_name}")
        print(f"Workspace ID: {workspace_id}")
        print(f"Environment: {self.environment.upper()}")
        print(f"Item Types: {', '.join(self.item_types)}")
        
        try:
            # Initialize FabricWorkspace
            print("\nInitializing Fabric workspace connection...")
            workspace = FabricWorkspace(
                workspace_id=workspace_id,
                environment=self.environment,
                repository_directory=self.repo_dir,
                item_type_in_scope=self.item_types
            )
            
            # Publish all items
            print("\nPublishing all items to workspace...")
            print("This will create new items or update existing ones automatically.")
            
            publish_all_items(workspace)
            
            print("\n✓ All items published successfully!")
            
            # Optional: Uncomment to remove orphaned items
            # print("\nCleaning up orphaned items...")
            # unpublish_all_orphan_items(workspace)
            
        except Exception as e:
            print(f"\n✗ Deployment failed: {str(e)}")
            raise
    
    def run(self):
        """Execute full deployment process"""
        print("\n" + "="*60)
        print(f"FABRIC DEPLOYMENT - {self.environment.upper()} ENVIRONMENT")
        print("="*60 + "\n")
        
        # Check for reserved names first
        has_reserved_names = self.check_reserved_names()
        if has_reserved_names:
            print("\n⚠ Found items with potentially reserved names.")
            print("Continuing with deployment, but monitor for failures...\n")
        
        # Process transformations
        self.process_find_replace()
        self.process_key_value_replace()
        
        # Deploy using fabric_cicd
        self.deploy_to_fabric()


def main():
    """Main entry point"""
    # Normalize environment name
    raw_env = os.getenv("ENVIRONMENT", "dev").lower()
    if raw_env.endswith("dev"):
        environment = "dev"
    elif raw_env.endswith("prod"):
        environment = "prod"
    else:
        environment = raw_env
    
    # Allow TARGET_ENVIRONMENT as fallback
    environment = os.environ.get('TARGET_ENVIRONMENT', environment)
    
    try:
        deployer = FabricDeployer(environment)
        deployer.run()
        
        print("\n" + "="*60)
        print("✓ DEPLOYMENT COMPLETED SUCCESSFULLY!")
        print("="*60)
        
    except Exception as e:
        print(f"\n✗ Deployment failed: {str(e)}")
        import traceback
        traceback.print_exc()
        exit(1)


if __name__ == "__main__":
    main()
