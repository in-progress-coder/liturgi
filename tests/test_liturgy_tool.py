import os
import zipfile
import xml.etree.ElementTree as ET

import src.liturgi.liturgy_tool as lt


def test_update_word_properties():
    props = lt.get_properties_for_date('2024-04-07')
    out_path = lt.update_word_file('template.docx', '2024-04-07', props)
    assert os.path.exists(out_path)
    with zipfile.ZipFile(out_path) as z:
        core = ET.fromstring(z.read('docProps/core.xml'))
        ns_core = {'dc': 'http://purl.org/dc/elements/1.1/'}
        assert core.find('dc:title', ns_core).text == 'Tema Contoh'
        assert core.find('dc:subject', ns_core).text == 'Minggu Palma'
        custom = ET.fromstring(z.read('docProps/custom.xml'))
        ns_c = {
            'cp': 'http://schemas.openxmlformats.org/officeDocument/2006/custom-properties',
            'vt': 'http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes'
        }
        def get_prop(name):
            for p in custom.findall('cp:property', ns_c):
                if p.get('name') == name:
                    v = p.find('vt:lpwstr', ns_c)
                    return v.text if v is not None else None
            return None
        assert get_prop('@BACAAN 1') == 'Yesaya 1:1'
    os.remove(out_path)
