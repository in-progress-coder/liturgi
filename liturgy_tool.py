"""Tool to update Word document properties based on Excel schedule."""
import os
import shutil
import sys
import zipfile
import xml.etree.ElementTree as ET
from typing import Dict, Tuple, List
from datetime import date

# Mapping from Excel column headers to (property_type, property_name)
# property_type: 'core' for core document properties, 'custom' for custom properties
COLUMN_TO_PROPERTY: Dict[str, Tuple[str, str]] = {
    "Tanggal": ("custom", "@TANGGAL"),
    "Nama Minggu": ("core", "subject"),
    "Warna Liturgi": ("custom", "@WARNA LITURGI"),
    "Tema": ("core", "title"),
    "Bacaan 1": ("custom", "@BACAAN 1"),
    "Antar Bacaan": ("custom", "@ANTAR BACAAN"),
    "Bacaan 2": ("custom", "@BACAAN 2"),
    "Bacaan Injil": ("custom", "@BACAAN INJIL"),
    "Pelayan Firman": ("custom", "@PELAYAN FIRMAN"),
    "Nyanyian Prosesi": ("custom", "@NYANYIAN_PROSESI"),
    "Nyanyian Pengakuan Dosa": ("custom", "@NYANYIAN_PENGAKUAN_DOSA"),
    "Nyanyian Berita Anugerah": ("custom", "@NYANYIAN_BERITA_ANUGERAH"),
    "Nyanyian Persembahan": ("custom", "@NYANYIAN_PERSEMBAHAN"),
    "Nyanyian Peneguhan": ("custom", "@NYANYIAN PENGUTUSAN"),
}

EXCEL_FILE = os.path.join(os.path.dirname(__file__), "Jadwal Liturgi.xlsx")
SHEET_NAME = "LITURGI INDUK"


def _read_shared_strings(z: zipfile.ZipFile) -> List[str]:
    """Return list of shared strings if present, else empty list."""
    try:
        data = z.read("xl/sharedStrings.xml")
    except KeyError:
        return []
    ns = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    root = ET.fromstring(data)
    strings: List[str] = []
    for si in root.findall("m:si", ns):
        # Concatenate all text nodes to handle rich text
        text_parts: List[str] = []
        for t in si.findall('.//m:t', ns):
            text_parts.append(t.text or "")
        strings.append("".join(text_parts))
    return strings


def _cell_text(cell: ET.Element, ns: Dict[str, str], shared_strings: List[str]) -> str:
    """Return the user-visible text for a cell.

    Handles inline strings, shared strings, formula strings, and raw values.
    """
    t_attr = cell.get("t")
    # Inline string
    if t_attr == "inlineStr":
        is_elem = cell.find("m:is", ns)
        if is_elem is not None:
            t = is_elem.find("m:t", ns)
            if t is not None:
                return t.text or ""
    v = cell.find("m:v", ns)
    if v is None:
        return ""
    # Shared string lookup
    if t_attr == "s":
        try:
            idx = int(v.text) if v.text is not None else -1
        except ValueError:
            idx = -1
        if 0 <= idx < len(shared_strings):
            return shared_strings[idx]
        return ""
    # Formula string (already calculated value in <v>) or plain number/text
    return v.text or ""


def _workbook_uses_1904(z: zipfile.ZipFile) -> bool:
    """Detect if the workbook uses the 1904 date system."""
    try:
        data = z.read("xl/workbook.xml")
    except KeyError:
        return False
    ns = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    root = ET.fromstring(data)
    wbpr = root.find("m:workbookPr", ns)
    return wbpr is not None and wbpr.get("date1904") in {"1", "true", "True"}


def _excel_serial_from_date_str(date_str: str, use_1904: bool) -> int:
    """Convert YYYY-MM-DD to Excel serial number (integer days)."""
    d = date.fromisoformat(date_str)
    if use_1904:
        # In 1904 system, serial 0 = 1904-01-01
        base = date(1904, 1, 1)
        return (d - base).days
    # 1900 system with Excel's leap-year bug (treats 1900 as leap year)
    base = date(1899, 12, 31)
    serial = (d - base).days
    if d >= date(1900, 3, 1):
        serial += 1
    return serial


def read_schedule_row(date_str: str, excel_path: str = EXCEL_FILE) -> Dict[str, str]:
    """Return mapping of column header to cell value for the given date.

    The Excel file stores dates as numeric serials; this converts the input
    YYYY-MM-DD to the matching Excel serial and compares numerically.
    """
    with zipfile.ZipFile(excel_path) as z:
        sheet_xml = z.read("xl/worksheets/sheet1.xml")
        shared = _read_shared_strings(z)
        use_1904 = _workbook_uses_1904(z)
    ns = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    root = ET.fromstring(sheet_xml)
    rows = root.find("m:sheetData", ns).findall("m:row", ns)
    # Build headers using sharedStrings to resolve indices
    headers = [_cell_text(c, ns, shared) for c in rows[0].findall("m:c", ns)]
    # Target Excel serial
    target_serial = _excel_serial_from_date_str(date_str, use_1904)
    for row in rows[1:]:
        cells = row.findall("m:c", ns)
        values = [_cell_text(c, ns, shared) for c in cells]
        if not values:
            continue
        row_dict = dict(zip(headers, values))
        raw = row_dict.get("Tanggal")
        if raw is None or raw == "":
            continue
        try:
            cell_serial = int(float(raw))
        except ValueError:
            # Not numeric; skip
            continue
        if cell_serial == target_serial:
            return row_dict
    raise ValueError(f"Date {date_str} not found in schedule")


def get_properties_for_date(date_str: str, excel_path: str = EXCEL_FILE) -> Dict[str, Dict[str, str]]:
    row = read_schedule_row(date_str, excel_path)
    props = {"core": {}, "custom": {}}
    for column, (ptype, pname) in COLUMN_TO_PROPERTY.items():
        value = row.get(column)
        if value is None:
            continue
        props[ptype][pname] = value
    return props


def _update_core_properties(data: bytes, core_props: Dict[str, str]) -> bytes:
    ns = {
        "cp": "http://schemas.openxmlformats.org/package/2006/metadata/core-properties",
        "dc": "http://purl.org/dc/elements/1.1/",
        "dcterms": "http://purl.org/dc/terms/",
        "dcmitype": "http://purl.org/dc/dcmitype/",
        "xsi": "http://www.w3.org/2001/XMLSchema-instance",
    }
    root = ET.fromstring(data)
    for key, value in core_props.items():
        tag = f"{{{ns['dc']}}}{'title' if key == 'title' else 'subject'}" if key in {'title', 'subject'} else None
        if tag is None:
            continue
        elem = root.find(f"dc:{key}", ns)
        if elem is None:
            elem = ET.SubElement(root, tag)
        elem.text = value
    return ET.tostring(root, encoding="UTF-8", xml_declaration=True)


def _update_custom_properties(data: bytes, custom_props: Dict[str, str]) -> bytes:
    ns = {
        "cp": "http://schemas.openxmlformats.org/officeDocument/2006/custom-properties",
        "vt": "http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes",
    }
    root = ET.fromstring(data)
    fmtid = "{D5CDD505-2E9C-101B-9397-08002B2CF9AE}"
    # determine next pid
    pid = 1
    for prop in root.findall("cp:property", ns):
        pid = max(pid, int(prop.get("pid", "0")))
    pid += 1
    for name, value in custom_props.items():
        prop = None
        for p in root.findall("cp:property", ns):
            if p.get("name") == name:
                prop = p
                break
        if prop is None:
            prop = ET.SubElement(root, f"{{{ns['cp']}}}property", fmtid=fmtid, pid=str(pid), name=name)
            pid += 1
        # remove existing children
        for child in list(prop):
            prop.remove(child)
        val_elem = ET.SubElement(prop, f"{{{ns['vt']}}}lpwstr")
        val_elem.text = value
    return ET.tostring(root, encoding="UTF-8", xml_declaration=True)


def update_word_file(template_path: str, date_str: str, props: Dict[str, Dict[str, str]]) -> str:
    """Copy template to new path and update properties. Return new file path."""
    out_dir = os.path.dirname(template_path)
    out_name = f"Liturgi {date_str}.docx"
    out_path = os.path.join(out_dir, out_name)
    shutil.copyfile(template_path, out_path)
    with zipfile.ZipFile(out_path, "a") as z:
        core_xml = z.read("docProps/core.xml")
        core_xml = _update_core_properties(core_xml, props.get("core", {}))
        z.writestr("docProps/core.xml", core_xml)
        custom_xml = z.read("docProps/custom.xml")
        custom_xml = _update_custom_properties(custom_xml, props.get("custom", {}))
        z.writestr("docProps/custom.xml", custom_xml)
    return out_path


def main(argv=None):
    argv = argv or sys.argv[1:]
    if len(argv) != 2:
        print("Usage: python liturgy_tool.py YYYY-MM-DD path/to/template.docx")
        return 1
    date_str, template_path = argv
    props = get_properties_for_date(date_str)
    out_path = update_word_file(template_path, date_str, props)
    print(f"Updated document saved to {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
