import requests
import json
import pandas as pd
from pathlib import Path
from datetime import datetime
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ComtradeMetadataExtractor:
    """Extract reference data and metadata from UN Comtrade API"""
    
    # Correct file-based reference data endpoints
    BASE_URL = "https://comtradeapi.un.org/files/v1/app/reference"
    
    def __init__(self, api_key: str, output_dir: str = "data/metadata/comtrade"):
        self.api_key = api_key
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.headers = {
            'Ocp-Apim-Subscription-Key': api_key
        }
        
    def get_reporters(self):
        """Get all reporter countries/regions with their codes."""
        url = f"{self.BASE_URL}/Reporters.json"
        logger.info("Fetching reporter countries reference data...")
        return self._fetch_and_save(url, 'reporters')
            
    def get_partners(self):
        """Get all partner countries/regions."""
        url = f"{self.BASE_URL}/partnerAreas.json"
        logger.info("Fetching partner countries reference data...")
        return self._fetch_and_save(url, 'partners')
    
    def get_flows(self):
        """Get trade flow types and regimes."""
        url = f"{self.BASE_URL}/tradeRegimes.json"
        logger.info("Fetching flow codes reference data...")
        return self._fetch_and_save(url, 'flows')
    
    def get_hs_codes(self):
        """Get all HS (Harmonized System) commodity codes."""
        url = f"{self.BASE_URL}/HS.json"
        logger.info("Fetching HS commodity codes...")
        return self._fetch_and_save(url, 'hs_codes')
    
    def get_modes_of_transport(self):
        """Get transport mode codes."""
        url = f"{self.BASE_URL}/mot.json"
        logger.info("Fetching modes of transport...")
        return self._fetch_and_save(url, 'transport_modes')
    
    def get_customs_codes(self):
        """Get customs procedure codes."""
        url = f"{self.BASE_URL}/customs.json"
        logger.info("Fetching customs codes...")
        return self._fetch_and_save(url, 'customs_codes')
    
    def get_qty_units(self):
        """Get quantity unit codes (kg, tonnes, etc.)."""
        url = f"{self.BASE_URL}/qt.json"
        logger.info("Fetching quantity unit codes...")
        return self._fetch_and_save(url, 'qty_units')
    
    def _fetch_and_save(self, url: str, name: str):
        """
        Generic fetch and save method.
        
        Args:
            url: API endpoint URL
            name: Base name for saved files
        """
        try:
            response = requests.get(url, headers=self.headers, timeout=60)
            response.raise_for_status()
            data = response.json()
            
            # Save raw JSON
            self._save_json(data, f'{name}_raw.json')
            
            # Convert to DataFrame
            if isinstance(data, list):
                df = pd.DataFrame(data)
            elif 'results' in data:
                df = pd.DataFrame(data['results'])
            else:
                df = pd.DataFrame([data])
                
            self._save_csv(df, f'{name}.csv')
            logger.info(f"✓ Found {len(df)} records")
            logger.info(f"  Columns: {df.columns.tolist()}")
            return df
                
        except Exception as e:
            logger.error(f"Failed to fetch {name}: {e}")
            raise
    
    def extract_all_metadata(self):
        """Run all metadata extractions in sequence."""
        logger.info("=" * 60)
        logger.info("Starting complete metadata extraction")
        logger.info("=" * 60)
        
        metadata_summary = {
            'extraction_date': datetime.now().isoformat(),
            'api_key_used': self.api_key[:8] + "...",
            'extractions': []
        }
        
        # All extractions are required
        extractions = [
            ('Reporters', self.get_reporters),
            ('Partners', self.get_partners),
            ('Flows', self.get_flows),
            ('HS Commodity Codes', self.get_hs_codes),
            ('Transport Modes', self.get_modes_of_transport),
            ('Customs Codes', self.get_customs_codes),
            ('Quantity Units', self.get_qty_units)
        ]
        
        for name, func in extractions:
            try:
                logger.info(f"\n--- Extracting {name} ---")
                result = func()
                
                # Add record count to summary
                record_count = len(result) if result is not None else 0
                
                metadata_summary['extractions'].append({
                    'name': name,
                    'status': 'success',
                    'record_count': record_count,
                    'timestamp': datetime.now().isoformat()
                })
                
            except Exception as e:
                logger.error(f"Failed to extract {name}: {e}")
                metadata_summary['extractions'].append({
                    'name': name,
                    'status': 'failed',
                    'error': str(e),
                    'timestamp': datetime.now().isoformat()
                })
                # Stop on first failure
                logger.error("Extraction failed. Stopping.")
                break
        
        # Save summary
        self._save_json(metadata_summary, 'extraction_summary.json')
        
        logger.info("=" * 60)
        logger.info("Metadata extraction complete")
        logger.info(f"Results saved to: {self.output_dir}")
        logger.info("=" * 60)
        
        return metadata_summary
    
    def _save_json(self, data, filename: str):
        """Save data as JSON"""
        filepath = self.output_dir / filename
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        logger.info(f"Saved: {filepath}")
        
    def _save_csv(self, df: pd.DataFrame, filename: str):
        """Save DataFrame as CSV"""
        filepath = self.output_dir / filename
        df.to_csv(filepath, index=False)
        logger.info(f"Saved: {filepath}")


if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    
    # Load API key from environment
    load_dotenv()
    api_key = os.getenv('COMTRADE_API_KEY')
    
    if not api_key:
        print("Error: COMTRADE_API_KEY not found in environment")
        print("Create a .env file with: COMTRADE_API_KEY=your_key_here")
        exit(1)
    
    # Run metadata extraction
    extractor = ComtradeMetadataExtractor(api_key=api_key)
    summary = extractor.extract_all_metadata()
    
    print("\n" + "=" * 60)
    print("EXTRACTION SUMMARY")
    print("=" * 60)
    
    success_count = sum(1 for item in summary['extractions'] if item['status'] == 'success')
    total_count = len(summary['extractions'])
    
    for item in summary['extractions']:
        status_symbol = '✓' if item['status'] == 'success' else '✗'
        count = f"({item.get('record_count', 0)} records)" if item['status'] == 'success' else ""
        print(f"{status_symbol} {item['name']}: {item['status']} {count}")
    
    print("\n" + "=" * 60)
    print(f"Extractions: {success_count}/{total_count} successful")
    
    if success_count == total_count:
        print("✓ All reference data extracted successfully!")
        print("\nYou can now proceed with trade data extraction.")
    else:
        print("✗ Some extractions failed.")