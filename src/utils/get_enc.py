import os
import sys
import logging
from pathlib import Path
from dotenv import dotenv_values

class EncEnv:
    def __init__(self):
        self.env_vars = {}
        self.env_paths = []
        
        # IMPORTANT: Order matters here - we prioritize the executable directory
        
        # First, try in the current working directory (highest priority)
        self.env_paths.append(os.path.join(os.getcwd(), '.env'))
        
        # If running as PyInstaller executable
        if getattr(sys, 'frozen', False):
            # Try in the executable directory (second highest priority)
            exe_dir = os.path.dirname(sys.executable)
            self.env_paths.append(os.path.join(exe_dir, '.env'))
        
        # Try in the script's root directory (lower priority)
        script_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
        self.env_paths.append(os.path.join(script_root, '.env'))
        
        # Try in the PyInstaller temp directory if available (lowest priority)
        if hasattr(sys, '_MEIPASS'):
            self.env_paths.append(os.path.join(sys._MEIPASS, '.env'))
        
        # Log all paths we're considering
        # print("\n[EncEnv] Looking for .env file in multiple locations:")
        for p in self.env_paths:
            exists = os.path.exists(p)
            # print(f"  - {p} (exists: {exists})")
            # logging.info(f"[EncEnv] .env path: {p} (exists: {exists})")
            
            # If we find a valid .env file, print its contents for debugging
            if exists:
                try:
                    with open(p, 'r') as f:
                        content = f.read()
                        # print(f"\n[EncEnv] Found .env file at {p}")
                        # print("[EncEnv] First few lines:")
                        # lines = content.split('\n')[:5]  # Print first 5 lines
                        # for line in lines:
                        #     if not any(secret in line.lower() for secret in ['password', 'secret', 'key']):
                        #         print(f" a")
                        # print("...")
                except Exception as e:
                    print(f"[EncEnv] Error reading .env file: {e}")

                    
                # Break after finding the first valid .env file
                break
    
    def get(self, key, default=None):
        """Get a specific environment variable by key with optional default value"""
        # Clear any previously loaded env vars to force reload
        self.env_vars = {}
        
        # Load from the first existing .env file
        for env_path in self.env_paths:
            if os.path.exists(env_path):
                try:
                    self.env_vars = dotenv_values(env_path)
                    break
                except Exception as e:
                    print(f"Error loading environment file: {e}")
        
        # Get the value with optional default
        value = self.env_vars.get(key, default)
        
        # Mask sensitive values in logs
        masked_value = "*****" if key.lower() in ["api_key", "password", "secret"] else value
        # print(f"Env variable {key} = {masked_value}")
        
        return value
        
    def fetch(self):
        """Load all environment variables from .env file"""
        # Load from the first existing .env file
        for env_path in self.env_paths:
            if os.path.exists(env_path):
                try:
                    self.env_vars = dotenv_values(env_path)
                    return self.env_vars
                except Exception as e:
                    print(f"Error loading environment file: {e}")
        
        return {}