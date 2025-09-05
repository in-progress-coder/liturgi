import json, os, sys
sys.path.insert(0, os.getcwd())
from src.liturgi.liturgy_tool import get_properties_for_date

print(json.dumps(get_properties_for_date('2025-09-14'), ensure_ascii=False, indent=2))
