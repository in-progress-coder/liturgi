import sys
import liturgy_tool as lt

date_str = sys.argv[1] if len(sys.argv) > 1 else '2025-09-07'
p = lt.get_properties_for_date(date_str)
print('core:', p['core'])
print('custom keys:', list(p['custom'].keys()))
out = lt.update_word_file('template.docx', date_str, p)
print('wrote:', out)
