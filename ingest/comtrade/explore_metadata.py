import pandas as pd
from pathlib import Path
import json

class MetadataExplorer:
    """Helper to explore extracted metadata"""
    
    def __init__(self, metadata_dir: str = "data/metadata/comtrade"):
        self.metadata_dir = Path(metadata_dir)
        
    def show_reporters(self, filter_regions: list = None):
        """
        Show reporter countries.
        
        Args:
            filter_regions: List of ISO3 codes to filter (e.g., EU27 codes)
        """
        df = pd.read_csv(self.metadata_dir / 'reporters.csv')
        
        if filter_regions:
            df = df[df['reporterCodeIsoAlpha3'].isin(filter_regions)]
        
        print(f"\nReporter Countries/Regions ({len(df)} total):")
        print("=" * 80)
        display_cols = ['reporterCode', 'reporterCodeIsoAlpha3', 'reporterDesc']
        print(df[display_cols].to_string(index=False))
        return df
    
    def show_eu27_reporters(self):
        """Show EU27 member states."""
        eu27_codes = [
            'AUT', 'BEL', 'BGR', 'HRV', 'CYP', 'CZE', 'DNK', 
            'EST', 'FIN', 'FRA', 'DEU', 'GRC', 'HUN', 'IRL', 
            'ITA', 'LVA', 'LTU', 'LUX', 'MLT', 'NLD', 'POL', 
            'PRT', 'ROU', 'SVK', 'SVN', 'ESP', 'SWE'
        ]
        print("\nEU27 Member States:")
        return self.show_reporters(filter_regions=eu27_codes)
    
    def show_usa_reporter(self):
        """Show USA reporter code."""
        print("\nUSA Reporter:")
        return self.show_reporters(filter_regions=['USA'])
    
    def show_partners(self, search_term: str = None):
        """
        Show partner countries.
        
        Args:
            search_term: Optional search string for partner name
        """
        df = pd.read_csv(self.metadata_dir / 'partners.csv')
        
        if search_term:
            df = df[df['PartnerDesc'].str.contains(search_term, case=False, na=False)]
        
        print(f"\nPartner Countries/Regions ({len(df)} total):")
        print("=" * 80)
        display_cols = ['PartnerCode', 'PartnerCodeIsoAlpha3', 'PartnerDesc']
        print(df[display_cols].to_string(index=False))
        return df
    
    def show_flows(self):
        """Show available trade flows."""
        df = pd.read_csv(self.metadata_dir / 'flows.csv')
        
        print("\nTrade Flow Codes:")
        print("=" * 80)
        print(df.to_string(index=False))
        print("\nKey flows for analysis:")
        print("  • M  = Import")
        print("  • X  = Export")
        print("  • RM = Re-Import")
        print("  • RX = Re-Export")
        return df
    
    def show_hs_codes(self, chapter: str = None, search_term: str = None, level: int = None):
        """
        Show HS commodity codes.
        
        Args:
            chapter: Filter by HS chapter (e.g., '10' for cereals)
            search_term: Search in commodity description
            level: Filter by aggregation level (2, 4, 6 digit codes)
        """
        df = pd.read_csv(self.metadata_dir / 'hs_codes.csv')
        
        if chapter:
            df = df[df['id'].astype(str).str.startswith(chapter)]
        
        if search_term:
            df = df[df['text'].str.contains(search_term, case=False, na=False)]
        
        if level:
            df = df[df['aggrLevel'] == level]
        
        print(f"\nHS Commodity Codes ({len(df)} total):")
        print("=" * 80)
        display_cols = ['id', 'text', 'aggrLevel', 'parent']
        print(df[display_cols].head(50).to_string(index=False))
        if len(df) > 50:
            print(f"\n... and {len(df) - 50} more records")
        return df
    
    def show_agricultural_hs_codes(self):
        """Show agricultural HS codes (chapters 01-24)."""
        print("\nAgricultural Commodity Codes (HS Chapters 01-24):")
        print("=" * 80)
        df = pd.read_csv(self.metadata_dir / 'hs_codes.csv')
        
        # Filter for agricultural chapters (01-24) at chapter level (aggrLevel=2)
        ag_chapters = df[
            (df['id'].astype(str).str.match(r'^(0[1-9]|1[0-9]|2[0-4])$')) & 
            (df['aggrLevel'] == 2)
        ].sort_values('id')
        
        print("\nChapter Overview:")
        print(ag_chapters[['id', 'text']].to_string(index=False))
        
        # Count total agricultural codes
        all_ag = df[df['id'].astype(str).str.match(r'^(0[1-9]|1[0-9]|2[0-4])')]
        print(f"\nTotal agricultural codes: {len(all_ag)}")
        
        return all_ag
    
    def show_key_commodities(self):
        """Show key commodities for the project."""
        print("\nKey Commodities for Project:")
        print("=" * 80)
        
        commodities = {
            'wheat': '10',
            'maize': '10',
            'corn': '10',
            'barley': '10',
            'rice': '10',
            'soybean': '12',
            'rapeseed': '12',
            'sunflower': '12',
            'sugar': '17',
            'potato': '07'
        }
        
        df = pd.read_csv(self.metadata_dir / 'hs_codes.csv')
        
        for name, chapter in commodities.items():
            print(f"\n{name.upper()} (Chapter {chapter}):")
            matches = df[
                (df['id'].astype(str).str.startswith(chapter)) &
                (df['text'].str.contains(name, case=False, na=False)) &
                (df['aggrLevel'] >= 4)
            ].head(5)
            
            if not matches.empty:
                print(matches[['id', 'text', 'aggrLevel']].to_string(index=False))
            else:
                print(f"  No direct matches - search chapter {chapter}")
    
    def show_summary(self):
        """Show extraction summary."""
        with open(self.metadata_dir / 'extraction_summary.json', 'r') as f:
            summary = json.load(f)
        
        print("\n" + "=" * 80)
        print("METADATA EXTRACTION SUMMARY")
        print("=" * 80)
        print(f"Extraction Date: {summary['extraction_date']}")
        print(f"API Key: {summary.get('api_key_used', 'N/A')}")
        print("\nExtractions:")
        
        for item in summary['extractions']:
            status = item['status']
            symbol = '✓' if status == 'success' else '✗' if status == 'failed' else '⊘'
            count = f"({item.get('record_count', 0)} records)" if status == 'success' else ""
            print(f"  {symbol} {item['name']}: {status} {count}")
        
        print("=" * 80)
        
        # Quick stats
        success_count = sum(1 for item in summary['extractions'] if item['status'] == 'success')
        total_count = len(summary['extractions'])
        print(f"\nOverall: {success_count}/{total_count} extractable")
        print(f"Successfully extracted: {success_count} reference datasets")
    
    def get_available_files(self):
        """List all available metadata files."""
        print("\nAvailable Metadata Files:")
        print("=" * 80)
        
        expected_files = {
            'reporters.csv': 'Reporter countries/regions',
            'partners.csv': 'Partner countries/regions', 
            'flows.csv': 'Trade flow types',
            'hs_codes.csv': 'HS commodity codes'
        }
        
        for filename, description in expected_files.items():
            filepath = self.metadata_dir / filename
            if filepath.exists():
                file_size = filepath.stat().st_size / 1024  # KB
                print(f"  ✓ {filename:<20} - {description:<35} ({file_size:.1f} KB)")
            else:
                print(f"  ✗ {filename:<20} - {description:<35} (missing)")


if __name__ == "__main__":
    explorer = MetadataExplorer()
    
    # Show summary
    explorer.show_summary()
    
    # Show available files
    explorer.get_available_files()
    
    # Show flows
    print("\n")
    explorer.show_flows()
    
    # Show EU27 and USA
    explorer.show_eu27_reporters()
    explorer.show_usa_reporter()
    
    # Show agricultural commodities
    explorer.show_agricultural_hs_codes()
    
    # Show key project commodities
    explorer.show_key_commodities()